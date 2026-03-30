# SaveChanges Failure Evidence

This note captures the failure patterns observed while diagnosing recurring `SaveChanges` issues in `DHRefreshAAS`.

Use this file for:

- root cause evidence
- failure pattern history
- important validation run IDs
- rollback guidance for preflight SQL changes

For the overall project/session restart view, start with `docs/ProjectSessionResume.md`.

## Recent Operation IDs

These operations were inspected through `DHRefreshAAS_Status` and the `OperationStatus` Azure Table:

- `2d9eb4fc-8d2e-4c78-833b-e8893a973762`
- `41cd8289-8b49-4773-ba14-199cd29e77fc`
- `877cb27d-ee7d-469c-baf7-bbe7c6c5d4da`
- `1e4d33df-371e-437e-a6d3-0da28a3b233f`

## Failure Patterns

### 1. Early batch abort

Observed shape:

- A whole batch fails together with `SaveChanges failed for batch 1`.
- All objects requested in that batch are marked failed at once.
- The status API previously hid the inner exception and only surfaced the generic batch failure message.

Examples:

- `41cd8289-8b49-4773-ba14-199cd29e77fc`
- `877cb27d-ee7d-469c-baf7-bbe7c6c5d4da`
- `1e4d33df-371e-437e-a6d3-0da28a3b233f`

### 2. Long SaveChanges hang

Observed shape:

- Operation enters `Saving Changes`.
- It can remain there for far longer than the intended monitoring window.
- Cleanup later marks it as zombie during startup or scale-down.

Examples from storage:

- `2d9eb4fc-8d2e-4c78-833b-e8893a973762` was still running in `Saving Changes` after more than 70 minutes.
- `0548c581-a0c6-4093-b6ca-54158a951135`
- `02a7ad5e-142b-4851-b798-13b71622bb94`
- `0458f1e5-7728-4fa8-99de-face51d87801`
- `013c1bb6-c969-45a2-aa4f-5f1866eabb0c`
- `06b24119-02af-4194-a4a3-06a7dd89416b`

### 3. Mixed outcomes across batches

Some older rows show one early batch failing while later tables still complete successfully. That means:

- the failing unit is often the transaction committed by a single `SaveChanges`, not necessarily the entire request;
- a single bad source query, partition, or transient engine problem can abort one batch while later work still succeeds.

Example:

- `068b183d-bb52-4c26-a13c-09e237c971e7`

## Confirmed Hotspot

The historically failing payloads were cross-checked against both AAS metadata and SQL mapping tables:

- `MM_CubeModel` failing batch tables map to `datalakeprod`
- `VN_CubeModel` failing batch tables map to `datalakeprod`
- the same tables do not map to `datalakeprod_uat` for those payloads

SQL resource history during the failure window (`2026-03-29 07:25` to `08:00` UTC) showed:

- `datalakeprod`
  - `avg_log_write_percent` peaked at `99.81%`
  - `avg_data_io_percent` peaked at `38.83%`
  - `avg_cpu_percent` peaked at `22.25%`
- `datalakeprod_uat`
  - negligible activity
- `new`
  - negligible activity in the same failure window

This strengthens the conclusion that the main SQL hotspot is `datalakeprod`, not the entire pool uniformly and not the UAT/new databases.

## Environment Routing Notes

Live AAS metadata currently shows:

- `VN_CubeModel` -> `datalakeprod`
- `MM_CubeModel` -> `datalakeprod` and `datalakeprod_uat`
- `NEW_CubeModel` -> `new` and `datalakeprod`
- `PROD_DataAnalyticsModel` -> `datalakeprod_uat`

That means cross-environment data source routing is part of the production risk surface, especially for `MM_CubeModel` and `NEW_CubeModel`.

## Configuration Cross-check

### Logic App concurrency

The current live baseline keeps cube refreshes serialized inside the workflow:

- `docs/LogicApp_RefreshCube_Workflow.json`
- `runtimeConfiguration.concurrency.repetitions = 1`

### Function host timeout

The function host allows the process to live longer than the app-level refresh budget:

- `host.json`
- `functionTimeout = 02:00:00`

### SaveChanges execution settings

The code computes effective `SaveChanges` timeout as:

- `max(SAVE_CHANGES_TIMEOUT_MINUTES, OPERATION_TIMEOUT_MINUTES)`

So if runtime settings are:

- `OPERATION_TIMEOUT_MINUTES = 60`
- `SAVE_CHANGES_TIMEOUT_MINUTES = 15`
- `SAVE_CHANGES_BATCH_SIZE = 15`
- `SAVE_CHANGES_MAX_PARALLELISM = 8`

Then the effective behavior is:

- `SaveChanges` batch size = `15`
- `SaveChanges` max parallelism = `8`
- effective `SaveChanges` timeout = `60`, not `15`

The checked-in reference file `docs/app-settings-production.json` has been updated to the safer baseline (`SAVE_CHANGES_BATCH_SIZE = 3`, `SAVE_CHANGES_MAX_PARALLELISM = 2`, SQL auto-scaling enabled), but always verify the live Azure app settings before tuning.

## Post-hotfix observations

- `RefreshCube_UAT` was revalidated after reintroducing `MaxRowsPerRun` through a pre-aggregated `changedRows` CTE:
  - mapped-change run `08584268132096677854381144049CU58` succeeded;
  - no-payload run `08584268119312186463939749743CU57` skipped safely without calling `DHRefreshAAS_HttpStart`.
- `RefreshCube` was revalidated on prod with run `08584268116075162255161193967CU37`, and `Get_AAS_Model_Tables_JSON_Format`, `DHRefreshAAS_HttpStart`, and `Append_Database_Result` all succeeded.
- The safe query pattern is now:
  - aggregate source deltas once in `changedRows`;
  - join `changedRows` into `candidate`;
  - evaluate `MaxRowsPerRun` from the pre-aggregated column instead of using aggregate expressions inline inside `CASE`.
- The shortlist guardrail metadata now carries:
  - `MaxRowsPerRun = 500000`;
  - `GuardrailType = MaxRowsPerRun`;
  - default owner/fix guidance;
  - `RequirePartition = 1` for rows that already have explicit partition mappings.
- A SQL policy-driven pilot was added for `RefreshCube_UAT`:
  - `docs/migration_add_CubeRefreshPolicy.sql` adds policy columns plus `etl.cuberefreshnotificationpolicy`;
  - warning/failure recipients are now resolved from SQL policy rows first, with table-owner and emergency fallback only if policy rows do not match;
  - cube ordering in `Get_List_CubeName` is now driven by `RefreshWave` and `RefreshPriority`.
- The SQL policy-driven model was later promoted to `RefreshCube` on prod:
  - controlled run `08584268018430812336637589863CU07` succeeded after promotion;
  - warning/failure recipient resolution on prod now follows SQL policy first, then owner fallback, then emergency fallback.

## Safe rollback rule

If a future preflight edit causes `Get_AAS_Model_Tables_JSON_Format` to fail again:

- immediately revert the query shape to the current `changedRows -> candidate` pattern used in:
  - `docs/LogicApp_RefreshCube_Workflow.json`
  - `docs/LogicApp_RefreshCube_UAT_Workflow.json`
  - `docs/LogicApp_RefreshCubeNew_Workflow.json`
- do not reintroduce inline aggregate checks such as `MAX(etlLog.ChangedRowCount)` inside `candidate` `CASE` expressions.

## Why the old diagnostics were insufficient

Before this change set:

- `ExecuteBatchSaveChangesAsync()` only returned `true` or `false`;
- the underlying exception was swallowed;
- status storage kept no batch table list or failure category;
- zombie operations showed almost no clue about which batch was hanging.

The new diagnostics are intended to preserve:

- current or last batch index;
- batch table list;
- detailed error text;
- failure category and likely source.
