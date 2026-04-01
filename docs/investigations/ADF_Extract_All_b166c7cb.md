# ADF investigation: `001 - master pipeline daily Extract - All`

**Pipeline run id:** `b166c7cb-0513-4756-a4df-768ff8238823`  
**Factory:** `vn-adf-sa-sdp-solution-p-42`  
**LineageKey (context):** 29519  

## Summary

The extract stage failed with **no per-table failures** recorded in `ETL.EtlLog` / `vw_EtlStageSummary`, while ADF explicitly failed the stage via a **`Fail`** activity.

## Failed activities (Azure CLI)

| Activity | Type | Error (abbrev.) |
|----------|------|-----------------|
| `100 - Extract pipeline - Start` | ExecutePipeline | Nested failure under `ForEachDataSource` → `Execute 101 - Extract pipeline for One DataSource` → … → **`RouteJobsBasedOnExtractBehavior`** (inner chain failed). |
| `Handle Extract Outcome` | IfCondition | Inner activity failed. |
| `Fail Extract Stage` | Fail | **`Extract stage failed after processing all eligible tables. Failed table count: 0`** |

## Interpretation

- The pipeline **intentionally** calls **`Fail Extract Stage`** when the orchestration concludes the extract stage failed but the **failed table count is 0** — matching SQL `MasterEtlLog` / `p_GetMasterAutoRecoveryDecision` behavior (orchestration failure without table-level evidence).
- Deepest named failing activity in the message chain: **`RouteJobsBasedOnExtractBehavior`** (under `ListTablesFromOneGroup` → … → `100 - Extract pipeline - Start`). Further drill-down: inspect **child pipeline runs** for `101 - Extract pipeline for One DataSource` and activities under **`RouteJobsBasedOnExtractBehavior`** in ADF Monitoring.

## Related fixes (repo)

- SQL: `docs/migration_master_recovery_orchestration_failure.sql` — auto-recovery when `MasterEtlLog` has `EXTRACT FAILED` and table summary shows zero failures.
- Ops: `docs/ops_scripts/grant_master_lineage_override.sql` — manual one-time override if needed before migration is deployed.
