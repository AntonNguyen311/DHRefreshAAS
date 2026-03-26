# DHRefreshAAS

Azure Functions app (.NET 8, isolated worker) that triggers **Azure Analysis Services (AAS)** tabular refreshes, tracks operations in **Azure Table Storage**, and exposes HTTP endpoints for **status** and **diagnostics** (token and connection tests).

## HTTP functions

| Function name | Methods | Purpose |
|---------------|---------|---------|
| `DHRefreshAAS_TestToken` | GET, POST | Test token acquisition for the configured auth mode |
| `DHRefreshAAS_TestConnection` | GET, POST | Test connectivity to the configured AAS server |
| `DHRefreshAAS_HttpStart` | POST | Start a long-running refresh (returns 202 + operation id for polling) |
| `DHRefreshAAS_Status` | GET | Poll status (optional `operationId` query for a single operation) |

Triggers use `AuthorizationLevel.Function`; callers must supply the function key or appropriate identity at the gateway.

## Configuration

Application settings are read via `ConfigurationService` (see `Services/ConfigurationService.cs`). Typical keys include `AAS_SERVER_URL`, `AAS_DATABASE`, `AAS_AUTH_MODE`, credentials for Service Principal or User/Password, and retry/timeout settings such as `MAX_RETRY_ATTEMPTS`, `OPERATION_TIMEOUT_MINUTES`. **`operationTimeoutMinutes`** in the refresh POST body sets both the overall cancellation budget and the minimum **SaveChanges** wait (together with **`connectionTimeoutMinutes`**, which drives MSOLAP **Connect Timeout**; **Command Timeout** is derived from the larger of operation and `SAVE_CHANGES_TIMEOUT_MINUTES`, capped at two hours). Use Key Vault references or secure app settings in Azure; keep secrets out of source control.

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
