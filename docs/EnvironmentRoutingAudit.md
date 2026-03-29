# Environment Routing Audit

This note captures the current environment routing that was verified with live AAS metadata and SQL mapping tables.

## Logic Apps

- `RefreshCube` uses SQL connection resource `sql`
- `RefreshCube_UAT` uses SQL connection resource `sql-1`
- `RefreshCube_new` uses SQL connection key `sql-1`, which is bound to connection resource `sql-2`

These workflow files are:

- `docs/LogicApp_RefreshCube_Workflow.json`
- `docs/LogicApp_RefreshCube_UAT_Workflow.json`
- `docs/LogicApp_RefreshCubeNew_Workflow.json`

## SQL Mapping Tables

### `datalakeprod`

Contains cube mappings for:

- `DAModel`
- `ID_CubeModel`
- `MM_CubeModel`
- `NEW_CubeModel`
- `VN_CubeModel`

### `datalakeprod_uat`

Contains cube mappings for:

- `MM_CubeModel`
- `PROD_DataAnalyticsModel`

### `new`

Contains cube mappings for:

- `NEW_CubeModel`

## Live AAS Model Data Sources

### `DAModel`

- `datalakeprod`

### `ID_CubeModel`

- `datalakeprod`

### `MM_CubeModel`

- `datalakeprod`
- `datalakeprod_uat`

### `NEW_CubeModel`

- `new`
- `datalakeprod`

### `PROD_DataAnalyticsModel`

- `datalakeprod_uat`

### `VN_CubeModel`

- `datalakeprod`

## Operational Implications

- `RefreshCube` failures involving `VN_CubeModel` are consistent with `datalakeprod` pressure.
- `RefreshCube_UAT` can still inherit production pressure through `MM_CubeModel`, because that model mixes `datalakeprod` and `datalakeprod_uat`.
- `RefreshCube_new` can still inherit production pressure through `NEW_CubeModel`, because that model mixes `new` and `datalakeprod`.
- `PROD_DataAnalyticsModel` currently points to `datalakeprod_uat`; this should be confirmed as intentional.

## Follow-up Checks

- Confirm the live SQL connection target for each Logic App resource connection (`sql`, `sql-1`, `sql-2`).
- Confirm whether `PROD_DataAnalyticsModel -> datalakeprod_uat` is by design.
- Remove cross-environment data sources from shared models where possible.
