# DHRefreshAAS

Azure Functions app (.NET 8, isolated worker) that triggers **Azure Analysis Services (AAS)** tabular refreshes, tracks operations in **Azure Table Storage**, and exposes HTTP endpoints for **status** and **diagnostics** (token and connection tests).

## HTTP functions

| Function name | Methods | Purpose |
|---------------|---------|---------|
| `DHRefreshAAS_TestToken` | GET, POST | Test token acquisition for the configured auth mode |
| `DHRefreshAAS_TestConnection` | GET, POST | Test connectivity to the configured AAS server |
| `DHRefreshAAS_HttpStart` | POST | Start a long-running refresh (returns 202 + operation id for polling) |
| `DHRefreshAAS_Status` | GET | Poll status (optional `operationId` query for a single operation) |
| `DHRefreshAAS_PortalModels` | GET | List self-service models after Entra/App Service auth |
| `DHRefreshAAS_PortalTables` | GET | List self-service tables for a selected model |
| `DHRefreshAAS_PortalPartitions` | GET | List live AAS partitions for an allowed table |
| `DHRefreshAAS_PortalSubmitRefresh` | POST | Submit an authenticated portal refresh request |
| `DHRefreshAAS_PortalStatus` | GET | View authenticated portal history and operation status |

Triggers use `AuthorizationLevel.Function`; callers must supply the function key or appropriate identity at the gateway.

Portal endpoints use `AuthorizationLevel.Anonymous` and are intended to sit behind App Service Authentication / Microsoft Entra ID. The app reads `X-MS-CLIENT-PRINCIPAL` after token validation and applies role/group authorization in code.

## Resume Here

When resuming work on this project, read these in order:

1. [`docs/MasterModelHandoff.md`](docs/MasterModelHandoff.md)
2. [`docs/ProjectSessionResume.md`](docs/ProjectSessionResume.md)
3. [`docs/AzureCliAndDatabaseOperations.md`](docs/AzureCliAndDatabaseOperations.md)
4. [`docs/SaveChangesFailureEvidence.md`](docs/SaveChangesFailureEvidence.md)
5. [`docs/EnvironmentRoutingAudit.md`](docs/EnvironmentRoutingAudit.md)
6. [`docs/app-settings-production.json`](docs/app-settings-production.json)

If another model should understand the project from one document first, start with `docs/MasterModelHandoff.md`. Use the remaining docs for deeper detail on operations, evidence, routing, and baseline settings.

## Shared Operator Env

For cross-model handoff, shared operational connection metadata now lives in:

- `.env.example`: tracked schema/template
- `.env`: local working copy for the current machine
- `load-env.ps1`: PowerShell helper that loads `.env` into the current process

Usage:

```powershell
. .\load-env.ps1
```

After loading, other models or scripts can use variables such as `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `FUNCTION_APP_NAME`, `LOGIC_APP_REFRESHCUBE`, and `SQL_DYNAMICINVOKE_PROD_URL` directly from the environment.

## Configuration

Application settings are read via `ConfigurationService` (see `Services/ConfigurationService.cs`). Typical keys include `AAS_SERVER_URL`, `AAS_DATABASE`, `AAS_AUTH_MODE`, credentials for Service Principal or User/Password, and retry/timeout settings such as `MAX_RETRY_ATTEMPTS`, `OPERATION_TIMEOUT_MINUTES`. **`operationTimeoutMinutes`** in the refresh POST body sets both the overall cancellation budget and the minimum **SaveChanges** wait (together with **`connectionTimeoutMinutes`**, which drives MSOLAP **Connect Timeout**; **Command Timeout** is derived from the larger of operation and `SAVE_CHANGES_TIMEOUT_MINUTES`, capped at two hours). Use Key Vault references or secure app settings in Azure; keep secrets out of source control.

For the current bottleneck profile, start from this safer operational baseline:

- `SAVE_CHANGES_BATCH_SIZE = 3`
- `SAVE_CHANGES_MAX_PARALLELISM = 2`
- Logic App `Foreach` concurrency = `5`
- `MAX_CONCURRENT_REFRESHES = 5`
- `ENABLE_ELASTIC_POOL_AUTO_SCALING = true`
- production baseline in `docs/app-settings-production.json` enables `ENABLE_AAS_AUTO_SCALING = true` with `AAS_SCALE_UP_SKU = S4`

For `MaxRowsPerRun`, use the current preflight pattern from `docs/LogicApp_RefreshCube_Workflow.json` and `docs/LogicApp_RefreshCube_UAT_Workflow.json`: compute row deltas in a separate `changedRows` CTE first, then join them into `candidate`. Do not put aggregate expressions such as `MAX(etlLog.ChangedRowCount)` directly inside the `candidate` guardrail `CASE` logic; that query shape previously broke the Logic App SQL connector.

For the SQL policy-driven pilot, `docs/migration_add_CubeRefreshPolicy.sql` adds table-level policy metadata (`PolicyGroup`, `RefreshWave`, `RefreshPriority`, `TableOwnerRecipients`) plus `etl.cuberefreshnotificationpolicy` for warning/failure recipient routing. `RefreshCube_UAT` is the first workflow using this contract so recipient changes can be made in SQL without editing Logic App email expressions.

For the self-service portal, add these application settings:

- `SELF_SERVICE_SQL_CONNECTION_STRING`: SQL connection string used to read `etl.datawarehouseandcubemapping`
- `SELF_SERVICE_SQL_DATABASE_NAME`: display/documentation hint for the metadata database, default `datalakeprod`
- `PORTAL_METADATA_ROLES`
- `PORTAL_REFRESH_ROLES`
- `PORTAL_ADMIN_ROLES`
- `PORTAL_METADATA_GROUP_IDS`
- `PORTAL_REFRESH_GROUP_IDS`
- `PORTAL_ADMIN_GROUP_IDS`

The portal metadata flow is:

1. SQL mapping decides which models/tables are allowed.
2. AAS/TOM returns the live current table/partition list.
3. The backend intersects both so the portal only shows refreshable existing objects.

Portal submissions are audited into operation storage with requester user id, display name, email, and request source.

## Operational Policy Locations

- Table mapping and guardrails live in `etl.datawarehouseandcubemapping`
- Warning/failure recipient policy lives in `etl.cuberefreshnotificationpolicy`
- Policy schema and seed logic live in `docs/migration_add_CubeRefreshPolicy.sql`
- Shortlist `MaxRowsPerRun = 500000` seed logic lives in `docs/migration_set_MaxRowsPerRun_500000_shortlist.sql`
- Azure CLI, Logic App, and SQL `dynamicInvoke` operating steps live in `docs/AzureCliAndDatabaseOperations.md`
- Workflow definitions live in:
  - `docs/LogicApp_RefreshCube_Workflow.json`
  - `docs/LogicApp_RefreshCube_UAT_Workflow.json`
  - `docs/LogicApp_RefreshCubeNew_Workflow.json`

Current state:

- `RefreshCube` and `RefreshCube_UAT` resolve warning/failure recipients from SQL policy first
- `RefreshCube_New` still uses the older contract and has not yet been migrated to SQL policy-driven recipient routing
- `portal/` contains a lightweight static Entra/MSAL frontend for self-service refresh

## Dependency injection

`Program.cs` registers: `ConfigurationService`, `ConnectionService`, `AasRefreshService` (singleton), `OperationStorageService`, `ProgressTrackingService`, `ErrorHandlingService`, `RequestProcessingService`, `ResponseService`, and `DHRefreshAASController`.

Copy `local.settings.json.example` to `local.settings.json` for local runs; the real file is gitignored.

## Build and test

```powershell
dotnet build
dotnet test
```

GitHub Actions runs the same solution on push/PR to `main` or `master` (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)). On push to `master`, code is also deployed to the Azure Function App via OIDC (see [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml)). CI does not require repository secrets. For GitHub Actions variables and optional deploy secrets, see [`docs/github-actions-secrets.md`](docs/github-actions-secrets.md).

To run the Functions host locally (verify HTTP routes and runtime resolution), install [Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local) and run `func start` from the project folder. This environment may not have `func` on PATH; install or use `npx azure-functions-core-tools` if needed.

## Refresh flow simulation (no AAS)

Console project **`RefreshFlowSimulation`** models partition **extract** and **load** delays and compares orchestration strategies (fully sequential, parallel per partition, wave extract→load, limited concurrency). It does **not** call Analysis Services; use it to reason about wall-clock bounds and timeouts before changing production code.

```powershell
dotnet run --project RefreshFlowSimulation/RefreshFlowSimulation.csproj -- 6 1000
# args: partitionCount, simulated SaveChanges milliseconds
```

The function app today issues `RequestRefresh` per object then one `SaveChanges`; real engine parallelism happens inside the service, not in this repo’s loop.

## Tests

Project **`DHRefreshAAS.Tests`** (xUnit) covers services and `DHRefreshAASController`.

| Test file | Focus |
|-----------|--------|
| `AasRefreshServiceTests` | Constructor validation; refresh when `CreateServerConnectionAsync` fails |
| `ConfigurationServiceTests` | Configuration getters and defaults |
| `ConnectionServiceTests` | Connection string and auth modes; some live token tests are skipped |
| `DHRefreshAASControllerTests` | Controller orchestration with mocked services |
| `DHRefreshAASIntegrationTests` | Controller flows with mocked dependencies |
| `ErrorHandlingServiceTests` | Error responses and `HandleExceptionAsync` |
| `ProgressTrackingServiceTests` | `ProgressTrackingService` and `OperationStatus` |
| `RequestProcessingServiceTests` | Parse/validate requests and enhanced post data |

Skipped tests: see `ConnectionServiceTests.cs` for live HTTP token tests. **`TestHttpHelpers`** provides Moq-backed `HttpRequestData` / `HttpResponseData` for the Functions worker.
