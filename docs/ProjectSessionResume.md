# Project Session Resume

This document is the operator-facing restart guide for `DHRefreshAAS`.

If another model needs one single file to understand the whole project and current direction first, read `docs/MasterModelHandoff.md` before this file.

For the exact Azure CLI, PowerShell, Logic App, and SQL execution steps used in practice, then read `docs/AzureCliAndDatabaseOperations.md`.

## Project Summary

`DHRefreshAAS` is a .NET 8 Azure Functions app that orchestrates Azure Analysis Services refreshes, persists operation status in Azure Table Storage, and is driven by Logic Apps plus SQL mapping/policy metadata.

Core runtime pieces:

- Azure Functions:
  - `DHRefreshAAS_HttpStart`
  - `DHRefreshAAS_Status`
  - `DHRefreshAAS_TestToken`
  - `DHRefreshAAS_TestConnection`
- Logic Apps:
  - `RefreshCube`
  - `RefreshCube_UAT`
  - `RefreshCube_New`
- SQL metadata:
  - `etl.datawarehouseandcubemapping`
  - `etl.cuberefreshnotificationpolicy`

## Live Resource Map

- Function App: `vn-fa-sa-sdp-p-aas`
- AAS server:
  - name: `vnaassasdpp01`
  - URL: `asazure://southeastasia.asazure.windows.net/vnaassasdpp01`
- SQL elastic pool:
  - server: `vn-sql-sa-sdp-solution-p-01`
  - pool: `vn-sql-sa-sdp-pool`
- Resource group: `vn-rg-sa-sdp-solution-p`
- Subscription: `8730775e-045c-47d1-a080-e3b9882cec01`

## Logic App To SQL Connection Mapping

- `RefreshCube` -> SQL connection resource `sql`
- `RefreshCube_UAT` -> SQL connection resource `sql-1`
- `RefreshCube_New` -> workflow key `sql-1`, bound to SQL connection resource `sql-2`

Use `docs/EnvironmentRoutingAudit.md` when the question is specifically about model/data source/environment routing.

## Refresh Flow

```mermaid
flowchart TD
    trigger[TriggerBody] --> cubeList[Get_List_CubeName]
    cubeList --> preflight[Get_AAS_Model_Tables_JSON_Format]
    preflight --> ignored[IgnoredTablesAndRecipients]
    preflight --> runnable[RefreshPayload]
    runnable -->|"payload exists"| httpStart[DHRefreshAAS_HttpStart]
    runnable -->|"payload empty"| skip[Append_Skipped_Result]
    httpStart --> status[DHRefreshAAS_Status]
    status --> result[Append_Database_Result]
    status --> failureGuide[Get_Failed_Table_Guidance]
    ignored --> warningMail[Send_PreRefresh_Warning_Email]
    failureGuide --> failureMail[Send_an_email_(V2)]
```

Important behavior:

- The Function app issues `RequestRefresh` per object and then one `SaveChanges`.
- Real engine-side parallelism happens inside AAS, not in this repo loop.
- The safe preflight SQL pattern for `RefreshCube` and `RefreshCube_UAT` is `changedRows -> candidate`.

## Queue Behavior

`DHRefreshAAS_HttpStart` now acts as the global execution gate for refresh work targeting `vnaassasdpp01`.

- New requests are persisted first with `status = queued`.
- Only one request per AAS queue scope is promoted to `running` at a time.
- If another request arrives while one refresh is active, it stays queued and later starts automatically in FIFO order.
- `DHRefreshAAS_Status` now distinguishes `queued` from real execution failure and includes queue metadata such as scope, queue position, and lease timestamps.

Operational implications:

- Logic App `Foreach` concurrency `1` still only serializes databases inside a single workflow run.
- The Function-level queue is the cross-run guard that prevents overlapping refresh execution when multiple `RefreshCube` requests land close together.
- If the host shuts down or a running operation becomes stale, startup/shutdown cleanup now releases the queue lease before the next queued request is allowed to start.

## Current Safe Baseline

These are the defaults to preserve unless there is a deliberate tuning change:

- `SAVE_CHANGES_BATCH_SIZE = 3`
- `SAVE_CHANGES_MAX_PARALLELISM = 2`
- Logic App `Foreach` concurrency = `1`
- `SAVE_CHANGES_TIMEOUT_MINUTES = 15`
- `OPERATION_TIMEOUT_MINUTES = 60`
- `CONNECTION_TIMEOUT_MINUTES = 10`
- `ENABLE_AAS_AUTO_SCALING = true`
- `AAS_SCALE_UP_SKU = S4`
- `AAS_ORIGINAL_SKU = S2`
- `ENABLE_ELASTIC_POOL_AUTO_SCALING = true`
- `ELASTIC_POOL_SCALE_UP_DTU = 1600`
- `ELASTIC_POOL_ORIGINAL_DTU = 800`

Reference files:

- `docs/app-settings-production.json`
- `readme.md`
- `Services/ConfigurationService.cs`

Operational note:

- AAS and elastic pool can be observed at their original idle levels (`S2`, `800 DTU`) outside refresh windows. That does not mean autoscaling is misconfigured; it means they are not currently scaled up.

## Current Guardrail Model

Guardrail precedence is:

1. `IsDisabled`
2. `IsIgnored`
3. `RequirePartition`
4. `MaxRowsPerRun`
5. runnable

Current live/checked-in guardrail facts:

- `MaxRowsPerRun` is currently seeded at `500000` for the agreed shortlist in `docs/migration_set_MaxRowsPerRun_500000_shortlist.sql`
- Rows with explicit partition mappings are biased toward `RequirePartition = 1`
- `VNFIN vw_fSalesNAV_Petfood` remains quarantined with `IsIgnored = 1`

## SQL Policy-Driven Recipient And Ordering Model

Policy schema was introduced in `docs/migration_add_CubeRefreshPolicy.sql`.

Table-level policy fields on `etl.datawarehouseandcubemapping`:

- `PolicyGroup`
- `RefreshWave`
- `RefreshPriority`
- `TableOwnerRecipients`
- `IsPolicyEnabled`
- `ModifiedBy`
- `ModifiedAtUtc`

Recipient routing table:

- `etl.cuberefreshnotificationpolicy`

Recipient precedence for `RefreshCube` and `RefreshCube_UAT`:

1. SQL notification policy match from `etl.cuberefreshnotificationpolicy`
2. `TableOwnerRecipients`
3. legacy `OwnerEmails`
4. emergency fallback list in workflow SQL output

Current scope:

- `RefreshCube` uses SQL policy-driven warning/failure recipients
- `RefreshCube_UAT` uses SQL policy-driven warning/failure recipients
- `RefreshCube_New` has not yet been migrated to the SQL policy-driven recipient contract

Where to edit things:

- Change warning/failure recipients: `etl.cuberefreshnotificationpolicy`
- Change table guardrails/order/owner fallback: `etl.datawarehouseandcubemapping`
- Change shortlist `MaxRowsPerRun` seed logic in repo: `docs/migration_set_MaxRowsPerRun_500000_shortlist.sql`

## Environment Routing Facts

Live routing summarized from `docs/EnvironmentRoutingAudit.md`:

- `VN_CubeModel` -> `datalakeprod`
- `MM_CubeModel` -> `datalakeprod` and `datalakeprod_uat`
- `NEW_CubeModel` -> `new` and `datalakeprod`
- `PROD_DataAnalyticsModel` -> `datalakeprod_uat`

Operational implications:

- `datalakeprod` is the main SQL hotspot for the historically failing payloads
- `MM_CubeModel` and `NEW_CubeModel` can inherit pressure from shared cross-environment routing
- `PROD_DataAnalyticsModel -> datalakeprod_uat` should be treated as intentional only after explicit confirmation

## Known Hotspots And Failure History

From `docs/SaveChangesFailureEvidence.md`:

- Main hotspot: `datalakeprod`
- Confirmed metric spike:
  - `avg_log_write_percent` reached `99.81%`
- Historical failure shapes:
  - early batch abort
  - long `Saving Changes` hang
  - mixed outcomes across batches

Why this matters:

- the main failure surface is SQL pressure around `datalakeprod`, not just AAS capacity
- reducing overlap, controlling payload size, and pre-blocking risky tables matters more than raising concurrency

## Recent Important Validation Runs

Use these IDs as anchors when cross-checking recent changes:

- UAT guardrail reintroduction:
  - `08584268132096677854381144049CU58` -> mapped-change run succeeded
  - `08584268119312186463939749743CU57` -> no-payload run skipped safely
- Prod guardrail reintroduction:
  - `08584268116075162255161193967CU37` -> `RefreshCube` validated on prod
- UAT SQL policy-driven pilot:
  - `08584268057043402729736565292CU92` -> succeeded with policy-driven ordering/recipient resolution
  - `08584268044266033724858419896CU94` -> skip branch still succeeded safely
- Prod SQL policy-driven promotion:
  - `08584268018430812336637589863CU07` -> controlled prod run succeeded after promotion

## What Is Already Deployed Live

Already deployed and validated live:

- safer runtime baseline
- `changedRows -> candidate` preflight pattern for `RefreshCube` and `RefreshCube_UAT`
- `MaxRowsPerRun = 500000` shortlist
- SQL policy schema on UAT and prod:
  - `etl.cuberefreshnotificationpolicy`
  - policy columns on `etl.datawarehouseandcubemapping`
- SQL policy-driven recipient and ordering model on:
  - `RefreshCube`
  - `RefreshCube_UAT`

Not yet fully migrated:

- `RefreshCube_New` recipient/order contract

## Rollback Rules

If preflight SQL starts failing again:

- immediately revert to the current `changedRows -> candidate` pattern
- do not reintroduce inline aggregate checks such as `MAX(etlLog.ChangedRowCount)` inside `candidate` guardrail `CASE` logic
- use:
  - `docs/LogicApp_RefreshCube_Workflow.json`
  - `docs/LogicApp_RefreshCube_UAT_Workflow.json`
  - `docs/LogicApp_RefreshCubeNew_Workflow.json`
  as the current known-good references

If email routing behaves unexpectedly:

- inspect `etl.cuberefreshnotificationpolicy` first
- then inspect `TableOwnerRecipients`
- then inspect `OwnerEmails`
- only after that inspect Logic App literals or fallback handling

## First Checks When Refresh Fails Again

1. Read `docs/SaveChangesFailureEvidence.md`
2. Check whether the failing cube maps to `datalakeprod`
3. Confirm current live app settings against `docs/app-settings-production.json`
4. Confirm Logic App `Foreach` concurrency is still `1`
5. Inspect whether the run failed in:
   - preflight SQL
   - `DHRefreshAAS_HttpStart`
   - long `Saving Changes`
6. If preflight changed recently, verify the SQL still follows the `changedRows -> candidate` pattern
7. If a warning/failure email went to the wrong people, inspect `etl.cuberefreshnotificationpolicy`

## SQL 209 Incident Note

After `docs/migration_add_ETL_ObservabilityAndFailurePolicy.sql` added `ETL.EtlLog.DataSourceId`, the live proc `[ETL].[ExtractEtlGenerateDataForExtractRuntime]` became vulnerable to `Ambiguous column name 'DataSourceId'` because it used an unqualified predicate while also reading `ETL.EtlLog`.

The tracked hotfix is:

- `docs/hotfix_fix_ExtractEtlGenerateDataForExtractRuntime_DataSourceId.sql`

The durable rule is:

- when an ETL proc reads both `ETL.ExtractLoadControlTable` and `ETL.EtlLog`, qualify shared columns such as `DataSourceId`, `ExtractLoadControlTableId`, `TableName`, and `PipelineRunId`

Related validation finding:

- extract manual validation later exposed a separate ADF logging issue: `common pipeline - 04 - ETL loggingJsonStr` checks optional fields by searching the whole serialized payload string, so extract callers that include `DataSourceId` only inside nested `Parametters` can still fail
- the live fix was to make `104 - Extract pipeline for One DataSource - DeltaExtract` emit top-level `DataSourceId` and `ExtractLoadControlTableId` in its logging payload before calling `common pipeline - 04`

## March 30 Follow-Up Hardening

Implemented live after the SQL 209 fix:

- `103 - Extract pipeline for One DataSource - BottomLevel` now emits top-level `DataSourceId`, `ExtractLoadControlTableId`, and `ChildPipelineRunId` for `FullExtract` and `DefaultExtract` logging payloads
- `104 - Extract pipeline for One DataSource - DeltaExtract` now emits the same top-level typed fields for delta logging payloads
- `103` and `104` now include explicit fallback wait activities on logging failure so extract business execution does not fail only because the logging child pipeline failed
- the tracked SQL migration for controlled recovery is `docs/migration_add_MasterLineageOverrideAndSsisJobCorrelation.sql`

What the migration adds:

- nullable `LineageKey`, `MasterPipelineRunId`, and `LastLinkedDateUtc` columns on `dbo.SSISJobInfo`
- `ETL.p_LinkMasterLineageToSSISJobInfo` to correlate SSIS job rows to a master lineage
- `ETL.MasterLineageOverride` plus `ETL.p_GrantMasterLineageOverride` for audited one-time master reopen approval
- an updated `ETL.MasterEtlGetLineageKey` that honors only an active scoped override and consumes it on use

Automatic recovery follow-up:

- the tracked SQL migration for transient-only auto rerun is `docs/migration_add_MasterAutoRecoveryPolicy.sql`
- it adds `ETL.MasterAutoRecoveryPolicy` and `ETL.p_GetMasterAutoRecoveryDecision`
- `ETL.MasterEtlGetLineageKey` now auto-grants `AUTO_ALLOW_NEXT_MASTER_RUN` only when the failed lineage has transient ETL failures only, the related `SSISJobInfo` row is linked, the job is in the safe `FAILED` state, and the auto retry budget is still available
- `Extract and load pipeline in queue` now writes back the child master `pipelineRunId`, resolved `LineageKey`, and `LastLinkedDateUtc` into `dbo.SSISJobInfo` after the master run completes, so later recovery decisions have a durable job-to-lineage correlation

Latest controlled validation:

- manual rerun of `101 - Extract pipeline for One DataSource - TopLevel` for `LineageKey = 29483` succeeded with run id `781af03b-bee3-4c85-87df-951d86031802`
- the rerun used `MasterPipelineRunId = 7c30c7eb-7a9a-4f5f-8daa-6dcad7a6e410`
- pending extract count for `LineageKey = 29483` and `DataSourceId = 1` dropped from `10` delta tables to `0`
- transaction-only validation confirmed `ETL.p_GetMasterAutoRecoveryDecision` returns `AutoRerunEligible` for a transient-only failed lineage linked to a failed `SSISJobInfo` row
- transaction-only validation confirmed `ETL.MasterEtlGetLineageKey` auto-created and consumed `AUTO_ALLOW_NEXT_MASTER_RUN`, then opened the next lineage successfully

## Restart Checklist For A Future Session

1. Read `readme.md`
2. Read this file: `docs/ProjectSessionResume.md`
3. Read `docs/AzureCliAndDatabaseOperations.md`
4. Read `docs/SaveChangesFailureEvidence.md`
5. Read `docs/EnvironmentRoutingAudit.md`
6. Check `docs/app-settings-production.json`
7. If editing recipients or guardrails, identify whether the target workflow is:
   - policy-driven (`RefreshCube`, `RefreshCube_UAT`)
   - legacy recipient model (`RefreshCube_New`)
8. Before changing preflight SQL, preserve the current query shape and output contract

## Where To Edit Common Things

- Add/change warning/failure recipients:
  - `etl.cuberefreshnotificationpolicy`
- Add/change table owner fallback:
  - `etl.datawarehouseandcubemapping.TableOwnerRecipients`
- Change table guardrails:
  - `etl.datawarehouseandcubemapping`
- Change shortlist threshold seed in repo:
  - `docs/migration_set_MaxRowsPerRun_500000_shortlist.sql`
- Change policy schema in repo:
  - `docs/migration_add_CubeRefreshPolicy.sql`
- Change live workflow behavior:
  - `docs/LogicApp_RefreshCube_Workflow.json`
  - `docs/LogicApp_RefreshCube_UAT_Workflow.json`
  - `docs/LogicApp_RefreshCubeNew_Workflow.json`
- Run Azure CLI / PowerShell operations safely:
  - `docs/AzureCliAndDatabaseOperations.md`
