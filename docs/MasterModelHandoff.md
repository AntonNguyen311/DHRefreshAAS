# Master Model Handoff

This is the single-file handoff for `DHRefreshAAS`.

If another model needs to continue work on this project, this is the first file to read. It is written to be self-contained enough that the model can understand the project, the live environment, the current safe operating baseline, the main risks, and the intended direction without reconstructing the full prior session history.

If the task is specifically about Azure Data Factory orchestration, read `docs/ADF_ArchitectureAndRunbook.md` immediately after this file.

## 1. What This Project Is

`DHRefreshAAS` is a .NET 8 Azure Functions app that orchestrates Azure Analysis Services refreshes.

At a high level:

- Logic Apps decide what should be refreshed
- SQL metadata decides which tables are runnable, ignored, partition-required, row-limited, and who should be notified
- the Function App submits AAS refresh requests and performs `SaveChanges`
- Azure Table Storage stores operation status and diagnostics

Core runtime entry points:

- `DHRefreshAAS_HttpStart`
- `DHRefreshAAS_Status`
- `DHRefreshAAS_TestToken`
- `DHRefreshAAS_TestConnection`

Main orchestrators:

- `RefreshCube`
- `RefreshCube_UAT`
- `RefreshCube_New`

Main SQL metadata tables:

- `etl.datawarehouseandcubemapping`
- `etl.cuberefreshnotificationpolicy`

## 2. What Another Model Should Do First

### Local operational setup

Load the local environment file first:

```powershell
. .\load-env.ps1
```

That loads shared operational values such as:

- Azure subscription and resource group
- Function App name and base URL
- Logic App names
- AAS server metadata
- SQL `dynamicInvoke` URLs
- current runtime baseline values

Important security note:

- `.env` is local-only and ignored by git
- on this machine it may contain real working credentials or function keys
- do not copy its contents into tracked docs or commit it

Then verify Azure CLI login:

```powershell
az login
az account show -o json
```

Expected active subscription:

- `8730775e-045c-47d1-a080-e3b9882cec01`

## 3. Live Resource Map

These are the real resources currently used by this project:

- subscription: `8730775e-045c-47d1-a080-e3b9882cec01`
- resource group: `vn-rg-sa-sdp-solution-p`
- Function App: `vn-fa-sa-sdp-p-aas`
- AAS server name: `vnaassasdpp01`
- AAS server URL: `asazure://southeastasia.asazure.windows.net/vnaassasdpp01`
- SQL server: `vn-sql-sa-sdp-solution-p-01`
- SQL elastic pool: `vn-sql-sa-sdp-pool`

Logic App names:

- `RefreshCube`
- `RefreshCube_UAT`
- `RefreshCube_New`

## 4. The Real Refresh Flow

This is the conceptual flow now running in prod/UAT:

1. A Logic App receives an HTTP trigger
2. It loads the list of cubes to consider
3. It runs preflight SQL against the correct SQL connection
4. Preflight SQL returns:
   - runnable refresh payload
   - ignored/blocked tables
   - warning recipients
   - guidance text
5. If there is a runnable payload, the Logic App calls `DHRefreshAAS_HttpStart`
6. The Function App submits `RequestRefresh` for each object and then performs `SaveChanges`
7. The Logic App polls `DHRefreshAAS_Status`
8. On failure, it fetches failure guidance and sends email
9. On skip/no-payload, it appends a skipped result instead of calling the Function App

Important implementation fact:

- real engine-side parallelism happens inside AAS
- this repo does not create the true engine parallelism itself
- the repo code mainly controls request shape, batch size, and how many batches are attempted together

## 5. Workflow To SQL Mapping

This mapping matters a lot when debugging:

- `RefreshCube` -> SQL connection resource `sql`
- `RefreshCube_UAT` -> SQL connection resource `sql-1`
- `RefreshCube_New` -> workflow key `sql-1`, but that key is bound to SQL connection resource `sql-2`

Interpretation:

- `sql` is the prod SQL connection
- `sql-1` is the UAT SQL connection
- `sql-2` is the new environment SQL connection

Do not assume a workflow name implies an isolated data source. Some AAS models still cross environments.

## 6. AAS Model Routing Facts

The verified live model-to-data-source routing is:

- `DAModel` -> `datalakeprod`
- `ID_CubeModel` -> `datalakeprod`
- `MM_CubeModel` -> `datalakeprod` and `datalakeprod_uat`
- `NEW_CubeModel` -> `new` and `datalakeprod`
- `PROD_DataAnalyticsModel` -> `datalakeprod_uat`
- `VN_CubeModel` -> `datalakeprod`

Operational meaning:

- `datalakeprod` is the main pressure hotspot
- `MM_CubeModel` and `NEW_CubeModel` can inherit pressure from shared data sources
- UAT or new runs are not guaranteed to be isolated if the AAS model still points to `datalakeprod`
- `PROD_DataAnalyticsModel -> datalakeprod_uat` should be treated as intentional only after explicit confirmation

## 7. Root Cause That Was Actually Observed

The recurring failures were not explained mainly by "AAS too small" alone.

The strongest evidence points to:

- SQL pressure centered on `datalakeprod`
- especially transaction log write pressure during refresh windows
- then AAS `SaveChanges` failing or hanging while waiting on the backend work

Observed evidence:

- `avg_log_write_percent` on `datalakeprod` peaked at `99.81%`
- `datalakeprod_uat` and `new` were quiet in the same failure window
- failing cubes like `VN_CubeModel` and `MM_CubeModel` mapped to `datalakeprod`

Failure shapes seen in practice:

- early batch abort
- long `Saving Changes` hang
- mixed outcomes across batches

Conclusion:

- the main stability fix is not "increase concurrency"
- the main stability fix is "reduce overlap, keep payloads smaller, block risky tables early, and autoscale strategically"

## 8. Current Safe Baseline

These are the current safe defaults and should be preserved unless there is a deliberate reason to change them:

- `SAVE_CHANGES_BATCH_SIZE = 3`
- `SAVE_CHANGES_MAX_PARALLELISM = 2`
- Logic App `Foreach` concurrency = `5`
- `MAX_CONCURRENT_REFRESHES = 5`
- `SAVE_CHANGES_TIMEOUT_MINUTES = 15`
- `OPERATION_TIMEOUT_MINUTES = 60`
- `CONNECTION_TIMEOUT_MINUTES = 10`
- `ENABLE_AAS_AUTO_SCALING = true`
- `AAS_SCALE_UP_SKU = S4`
- `AAS_ORIGINAL_SKU = S2`
- `ENABLE_ELASTIC_POOL_AUTO_SCALING = true`
- `ELASTIC_POOL_SCALE_UP_DTU = 1600`
- `ELASTIC_POOL_ORIGINAL_DTU = 800`

Important interpretation:

- outside refresh windows, AAS can appear as `S2` and the pool can appear as `800 DTU`
- that does not automatically mean autoscaling is broken
- it usually means the system is idle and not currently scaled up

## 9. Guardrail Model

The guardrail precedence in SQL is:

1. `IsDisabled`
2. `IsIgnored`
3. `RequirePartition`
4. `MaxRowsPerRun`
5. runnable

Current facts:

- `MaxRowsPerRun = 500000` is seeded only for the agreed shortlist
- rows with explicit partition mappings are biased toward `RequirePartition = 1`
- `VNFIN vw_fSalesNAV_Petfood` is quarantined with `IsIgnored = 1`

The main table for this logic is:

- `etl.datawarehouseandcubemapping`

Important columns used operationally:

- `IsDisabled`
- `IsIgnored`
- `IgnoreReason`
- `FixGuide`
- `OwnerEmails`
- `GuardrailType`
- `RequirePartition`
- `MaxRowsPerRun`
- `MaxLookbackMonths`
- `PolicyGroup`
- `RefreshWave`
- `RefreshPriority`
- `TableOwnerRecipients`
- `IsPolicyEnabled`

## 10. Recipient And Ordering Model

The project moved away from hardcoded Logic App email routing and toward SQL policy-driven routing.

Recipient routing table:

- `etl.cuberefreshnotificationpolicy`

Current precedence for `RefreshCube` and `RefreshCube_UAT`:

1. SQL notification policy match from `etl.cuberefreshnotificationpolicy`
2. `TableOwnerRecipients`
3. legacy `OwnerEmails`
4. emergency fallback list from workflow SQL output

Current state:

- `RefreshCube` uses SQL policy-driven warning/failure recipients
- `RefreshCube_UAT` uses SQL policy-driven warning/failure recipients
- `RefreshCube_New` has not yet been fully migrated to this model

Ordering is also SQL-driven now:

- cube ordering is based on `RefreshWave` and `RefreshPriority`

Where to change behavior:

- change warning/failure recipients: `etl.cuberefreshnotificationpolicy`
- change table guardrails/order/owner fallback: `etl.datawarehouseandcubemapping`

## 11. The Safe Preflight SQL Pattern

This is one of the most important rules in the entire project.

The safe pattern is:

- aggregate row changes first in a `changedRows` CTE
- join that result into `candidate`
- evaluate `MaxRowsPerRun` from the pre-aggregated value

Do not reintroduce this unsafe shape:

- inline aggregate checks like `MAX(etlLog.ChangedRowCount)` inside `candidate` guardrail `CASE` logic

Why:

- that query shape previously broke the Logic App SQL connector
- it caused `Get_AAS_Model_Tables_JSON_Format` failures

Safe rollback rule:

- if preflight starts failing again, revert to the currently checked-in `changedRows -> candidate` shape in the workflow JSON files

## 12. What Has Already Been Implemented Live

Already deployed and validated:

- safer runtime baseline
- Logic App `Foreach` concurrency reduced to `1`
- AAS autoscaling baseline to `S4` on scale-up
- elastic pool autoscaling enabled
- improved `SaveChanges` diagnostics in the Function App
- `changedRows -> candidate` preflight pattern in `RefreshCube` and `RefreshCube_UAT`
- shortlist `MaxRowsPerRun = 500000`
- SQL policy schema added on UAT and prod
- SQL policy-driven warning/failure recipients in `RefreshCube` and `RefreshCube_UAT`
- SQL policy-driven cube ordering in `RefreshCube` and `RefreshCube_UAT`

Not fully migrated yet:

- `RefreshCube_New` recipient and ordering contract

## 13. Important Validation Runs

Use these run IDs as anchors when you need to confirm what "good" looked like:

- UAT guardrail reintroduction:
  - `08584268132096677854381144049CU58`
  - `08584268119312186463939749743CU57`
- prod guardrail reintroduction:
  - `08584268116075162255161193967CU37`
- UAT SQL policy-driven pilot:
  - `08584268057043402729736565292CU92`
  - `08584268044266033724858419896CU94`
- prod SQL policy-driven promotion:
  - `08584268018430812336637589863CU07`

## 14. How To Operate The System

### Standard operator flow

1. Load `.env`
2. Verify `az login`
3. Confirm subscription
4. Inspect Function App settings
5. Inspect Logic App concurrency
6. Inspect AAS / elastic pool state
7. Make repo changes
8. Validate workflow JSON
9. Deploy UAT first if changing workflow SQL or routing logic
10. Run a controlled validation
11. Monitor run and action status
12. Only then promote to prod

### The preferred way to query SQL

Use Azure CLI login plus ARM token plus Logic App SQL connection `dynamicInvoke`.

Do not rely on interactive `sqlcmd -G` for the main operating path.

### The preferred way to trigger production behavior

Use the Logic App, not a broad direct Function App call, unless you are isolating a very specific test case.

### How deployment works

- Function App code is deployed by GitHub Actions on push to `master`
- Logic App definitions are operated separately and were updated manually through Azure CLI during this project
- if you change workflow JSON, do not assume a code push will deploy the Logic App definition for you

## 15. Common Pitfalls

### Wrong Function App name

The real Function App is:

- `vn-fa-sa-sdp-p-aas`

### ARM token expiration during long polling

Fix:

- refresh the ARM token inside the polling loop

### Wrong SQL connection target

Fix:

- always verify whether the workflow is using `sql`, `sql-1`, or `sql-2`

### Multi-batch SQL migration does not apply correctly

Fix:

- split the SQL script by `GO`
- execute each batch separately
- verify the schema explicitly after execution

### Empty SQL output shape breaks Logic App expressions

Fix:

- keep output schema stable
- return a row with expected fields
- prefer empty arrays or null payloads over missing keys

### Cross-environment routing surprises

Fix:

- verify actual AAS model data sources, not just Logic App names

## 16. How To Debug A Failure Quickly

Use this order:

1. Check whether the failing cube maps to `datalakeprod`
2. Check whether the workflow is prod/UAT/new and which SQL connection it really uses
3. Confirm live app settings still match the safe baseline
4. Confirm Logic App concurrency is still `1`
5. Identify whether failure happened in:
   - preflight SQL
   - `DHRefreshAAS_HttpStart`
   - long `Saving Changes`
6. If preflight changed recently, verify it still follows `changedRows -> candidate`
7. If email recipients look wrong, inspect `etl.cuberefreshnotificationpolicy` first

## 17. Where To Edit Things

### Repo files

- main overview: `README.md`
- single-file handoff: `docs/MasterModelHandoff.md`
- operator restart guide: `docs/ProjectSessionResume.md`
- Azure CLI / PowerShell operations: `docs/AzureCliAndDatabaseOperations.md`
- failure evidence: `docs/SaveChangesFailureEvidence.md`
- routing facts: `docs/EnvironmentRoutingAudit.md`
- prod Function App baseline reference: `docs/app-settings-production.json`
- prod workflow definition: `docs/LogicApp_RefreshCube_Workflow.json`
- UAT workflow definition: `docs/LogicApp_RefreshCube_UAT_Workflow.json`
- new workflow definition: `docs/LogicApp_RefreshCubeNew_Workflow.json`
- policy schema migration: `docs/migration_add_CubeRefreshPolicy.sql`
- shortlist guardrail seed: `docs/migration_set_MaxRowsPerRun_500000_shortlist.sql`

### Local operator files

- local shared env: `.env`
- tracked env template: `.env.example`
- env loader: `load-env.ps1`

### SQL tables

- table mapping and guardrails: `etl.datawarehouseandcubemapping`
- notification routing: `etl.cuberefreshnotificationpolicy`

## 18. Current Direction

If continuing the project, the strategic direction should remain:

1. Keep refreshes safe and predictable rather than maximizing raw concurrency
2. Push control into SQL metadata instead of embedding business rules in Logic Apps
3. Keep warning/failure recipients data-driven
4. Reduce or remove cross-environment data source coupling where possible
5. Preserve the current safe preflight SQL shape
6. Validate in UAT before prod when changing workflow logic

Most likely next meaningful improvements:

- migrate `RefreshCube_New` to the same SQL policy-driven recipient and ordering contract
- reduce or eliminate cross-environment model routing where possible
- confirm whether `PROD_DataAnalyticsModel -> datalakeprod_uat` is intentional
- continue moving operational knowledge into simple reusable scripts for future agents/operators

## 19. What Not To Do

- do not raise Logic App concurrency casually
- do not restore large `SaveChanges` batch size / parallelism without evidence
- do not reintroduce inline aggregate guardrail expressions in preflight SQL
- do not assume UAT/new is isolated from prod pressure
- do not change email routing in Logic Apps first when SQL policy can handle it
- do not depend on local memory of resource names when `.env` already contains them

## 20. Minimal Restart Checklist

If a future model has almost no context, do this:

1. Read this file completely
2. Run:

```powershell
. .\load-env.ps1
az login
az account show -o json
```

3. Inspect current Function App settings and Logic App definitions before changing anything
4. If the issue is refresh failure, assume `datalakeprod` pressure is relevant until disproven
5. If the issue is recipient/routing behavior, inspect SQL metadata before editing workflow expressions
6. If changing workflow SQL, preserve `changedRows -> candidate`

## 21. Supporting Documents

This file is intended to be enough on its own, but these supporting documents exist for deeper detail:

- `docs/ADF_ArchitectureAndRunbook.md`
- `docs/ProjectSessionResume.md`
- `docs/AzureCliAndDatabaseOperations.md`
- `docs/SaveChangesFailureEvidence.md`
- `docs/EnvironmentRoutingAudit.md`
- `docs/app-settings-production.json`
