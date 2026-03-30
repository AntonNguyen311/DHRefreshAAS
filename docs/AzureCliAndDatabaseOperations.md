# Azure CLI And Database Operations

This document captures the exact working pattern used to operate `DHRefreshAAS` from a Windows PowerShell session so another model or engineer can follow the same process.

## Scope

Use this file for:

- Azure CLI login and context checks
- Function App, Logic App, AAS, and SQL elastic pool inspection
- Logic App deploy / rerun / monitor flow
- SQL execution through Logic App SQL connections via ARM `dynamicInvoke`
- common pitfalls and proven workarounds

For the broader architecture and operational context, start with `docs/ProjectSessionResume.md`.

## Load Shared Env First

Shared operational variables are stored in:

- `.env.example` for the tracked template
- `.env` for the local working copy
- `load-env.ps1` for loading variables into PowerShell

Load them at the start of a session:

```powershell
. .\load-env.ps1
```

Then you can reference variables like:

```powershell
$env:AZURE_SUBSCRIPTION_ID
$env:AZURE_RESOURCE_GROUP
$env:FUNCTION_APP_NAME
$env:SQL_DYNAMICINVOKE_PROD_URL
```

## Environment Assumptions

- OS: Windows
- shell: PowerShell
- subscription: `8730775e-045c-47d1-a080-e3b9882cec01`
- resource group: `vn-rg-sa-sdp-solution-p`
- Function App: `vn-fa-sa-sdp-p-aas`
- AAS server: `vnaassasdpp01`
- SQL server: `vn-sql-sa-sdp-solution-p-01`
- elastic pool: `vn-sql-sa-sdp-pool`

## Login And Context

### 1. Sign in

```powershell
az login
```

### 2. Verify the active subscription

```powershell
az account show -o json
```

Expected subscription id:

- `8730775e-045c-47d1-a080-e3b9882cec01`

If needed, set it explicitly:

```powershell
az account set --subscription "8730775e-045c-47d1-a080-e3b9882cec01"
```

### 3. Confirm the main resources exist

```powershell
az functionapp list -g "vn-rg-sa-sdp-solution-p" -o json
az logic workflow list -g "vn-rg-sa-sdp-solution-p" -o json
```

## High-Value Inspection Commands

### Function App settings

```powershell
az functionapp config appsettings list -g "vn-rg-sa-sdp-solution-p" -n "vn-fa-sa-sdp-p-aas" -o json
```

Useful query pattern:

```powershell
az functionapp config appsettings list -g "vn-rg-sa-sdp-solution-p" -n "vn-fa-sa-sdp-p-aas" --query "[?name=='ENABLE_AAS_AUTO_SCALING' || name=='ENABLE_ELASTIC_POOL_AUTO_SCALING' || name=='AAS_SCALE_UP_SKU' || name=='AAS_ORIGINAL_SKU' || name=='SAVE_CHANGES_BATCH_SIZE' || name=='SAVE_CHANGES_MAX_PARALLELISM' || name=='SAVE_CHANGES_TIMEOUT_MINUTES' || name=='OPERATION_TIMEOUT_MINUTES' || name=='CONNECTION_TIMEOUT_MINUTES']" -o json
```

### Logic App concurrency

```powershell
az logic workflow show -g "vn-rg-sa-sdp-solution-p" -n "RefreshCube" --query "definition.actions.For_each.runtimeConfiguration.concurrency" -o json
```

### AAS server state / SKU

```powershell
az resource show --resource-group "vn-rg-sa-sdp-solution-p" --resource-type "Microsoft.AnalysisServices/servers" --name "vnaassasdpp01" --query "{sku:sku.name,state:properties.state,querypoolConnectionMode:properties.querypoolConnectionMode}" -o json
```

### Elastic pool state

```powershell
az sql elastic-pool show -g "vn-rg-sa-sdp-solution-p" -s "vn-sql-sa-sdp-solution-p-01" -n "vn-sql-sa-sdp-pool" --query "{edition:edition,dtu:requestedDtu,maxDtu:databaseDtuMax,minDtu:databaseDtuMin,state:state}" -o json
```

## Logic App Connection Mapping

Use the correct SQL connection for the target environment:

- `sql` -> prod / `datalakeprod`
- `sql-1` -> UAT / `datalakeprod_uat`
- `sql-2` -> new / `new`

Workflow mapping:

- `RefreshCube` uses `sql`
- `RefreshCube_UAT` uses `sql-1`
- `RefreshCube_New` is wired through workflow key `sql-1`, but that key is bound to connection resource `sql-2`

## Deploy Workflow JSON

### Deploy a Logic App definition

```powershell
az logic workflow update -g "vn-rg-sa-sdp-solution-p" -n "RefreshCube" --definition "@docs/LogicApp_RefreshCube_Workflow.json" --query "name" -o tsv
```

Use the matching file and workflow name:

- `docs/LogicApp_RefreshCube_Workflow.json` -> `RefreshCube`
- `docs/LogicApp_RefreshCube_UAT_Workflow.json` -> `RefreshCube_UAT`
- `docs/LogicApp_RefreshCubeNew_Workflow.json` -> `RefreshCube_New`

### Validate JSON first

```powershell
$null = Get-Content "docs/LogicApp_RefreshCube_Workflow.json" -Raw | ConvertFrom-Json
Write-Output "VALID"
```

## Trigger A Logic App Run

### 1. Get an ARM token

```powershell
$armToken = az account get-access-token --resource https://management.azure.com/ --query accessToken -o tsv
```

### 2. Get the callback URL

```powershell
$baseUri = "https://management.azure.com/subscriptions/8730775e-045c-47d1-a080-e3b9882cec01/resourceGroups/vn-rg-sa-sdp-solution-p/providers/Microsoft.Logic/workflows/RefreshCube"
$callback = Invoke-RestMethod -Method Post -Uri ($baseUri + "/triggers/When_a_HTTP_request_is_received/listCallbackUrl?api-version=2019-05-01") -Headers @{ Authorization = "Bearer $armToken" }
$callback.value
```

### 3. Trigger the workflow

Example with lineage key:

```powershell
# Replace 12345 with a current lineage key known to produce changed rows.
$payload = @{ MasterEtlLineageKey = 12345 } | ConvertTo-Json -Compress
Invoke-RestMethod -Method Post -Uri $callback.value -ContentType "application/json" -Body $payload
```

Example with empty body:

```powershell
Invoke-RestMethod -Method Post -Uri $callback.value -ContentType "application/json" -Body "{}"
```

## Monitor A Logic App Run

### Find the new run

```powershell
$runs = Invoke-RestMethod -Method Get -Uri ($baseUri + "/runs?api-version=2019-05-01&%24top=5") -Headers @{ Authorization = "Bearer $armToken" }
$runId = $runs.value[0].name
$runId
```

### Poll the run status

Refresh the ARM token inside long loops:

```powershell
for ($poll = 1; $poll -le 45; $poll++) {
    $armToken = az account get-access-token --resource https://management.azure.com/ --query accessToken -o tsv
    $runDetail = Invoke-RestMethod -Method Get -Uri ($baseUri + "/runs/$runId?api-version=2019-05-01") -Headers @{ Authorization = "Bearer $armToken" }
    Write-Output ("Poll=" + $poll + "|RunStatus=" + $runDetail.properties.status)
    if ($runDetail.properties.status -ne "Running" -and $runDetail.properties.status -ne "Waiting" -and $runDetail.properties.status -ne "InProgress") { break }
    Start-Sleep -Seconds 20
}
```

### Fetch action-level status

```powershell
$armToken = az account get-access-token --resource https://management.azure.com/ --query accessToken -o tsv
$actions = Invoke-RestMethod -Method Get -Uri ($baseUri + "/runs/$runId/actions?api-version=2019-05-01") -Headers @{ Authorization = "Bearer $armToken" }
$actions.value | Select-Object name, @{n='status';e={$_.properties.status}}
```

Useful action names:

- `Get_List_CubeName`
- `Get_AAS_Model_Tables_JSON_Format`
- `Ignored_Tables_Condition`
- `Condition`
- `DHRefreshAAS_HttpStart`
- `Until_Operation_Complete`
- `Append_Database_Result`
- `Append_Skipped_Result`
- `Get_Failed_Table_Guidance`
- `Send_PreRefresh_Warning_Email`
- `Send_an_email_(V2)`
- `Scale_Up`
- `Scale_Down`

## Call The Function App Directly

Most production operations were driven through Logic Apps, but direct Function App calls are still useful for isolated testing.

### Get a function key

```powershell
az functionapp function keys list -g "vn-rg-sa-sdp-solution-p" -n "vn-fa-sa-sdp-p-aas" --function-name "DHRefreshAAS_HttpStart" -o json
```

### Start a refresh directly

```powershell
$functionKey = "<function-key>"
$startUri = "https://vn-fa-sa-sdp-p-aas.azurewebsites.net/api/DHRefreshAAS_HttpStart?code=$functionKey"
$body = @{
    objects = @(
        @{
            table = "vw_fExample"
            partition = "p202603"
            refreshType = "automatic"
        }
    )
    operationTimeoutMinutes = 60
    connectionTimeoutMinutes = 10
} | ConvertTo-Json -Depth 10

$startResp = Invoke-RestMethod -Method Post -Uri $startUri -ContentType "application/json" -Body $body
$startResp
```

### Poll status directly

```powershell
$statusUri = "https://vn-fa-sa-sdp-p-aas.azurewebsites.net/api/DHRefreshAAS_Status?code=$functionKey&operationId=$($startResp.operationId)"
Invoke-RestMethod -Method Get -Uri $statusUri
```

## Work With Database Via `dynamicInvoke`

### Why this path is used

Interactive `sqlcmd -G` was not reliable in this workflow because the auth flow can prompt or fail in unattended runs. The proven method is:

- use Azure CLI login
- get an ARM token
- call the Logic App SQL connection resource through `dynamicInvoke`

### Connection-specific endpoints

Prod:

```powershell
https://management.azure.com/subscriptions/8730775e-045c-47d1-a080-e3b9882cec01/resourceGroups/vn-rg-sa-sdp-solution-p/providers/Microsoft.Web/connections/sql/dynamicInvoke?api-version=2016-06-01
```

UAT:

```powershell
https://management.azure.com/subscriptions/8730775e-045c-47d1-a080-e3b9882cec01/resourceGroups/vn-rg-sa-sdp-solution-p/providers/Microsoft.Web/connections/sql-1/dynamicInvoke?api-version=2016-06-01
```

New:

```powershell
https://management.azure.com/subscriptions/8730775e-045c-47d1-a080-e3b9882cec01/resourceGroups/vn-rg-sa-sdp-solution-p/providers/Microsoft.Web/connections/sql-2/dynamicInvoke?api-version=2016-06-01
```

### Run a SQL query

```powershell
$armToken = az account get-access-token --resource https://management.azure.com/ --query accessToken -o tsv
$uri = "https://management.azure.com/subscriptions/8730775e-045c-47d1-a080-e3b9882cec01/resourceGroups/vn-rg-sa-sdp-solution-p/providers/Microsoft.Web/connections/sql/dynamicInvoke?api-version=2016-06-01"
$query = @"
SELECT TOP 10 CubeName, CubeTableName
FROM etl.datawarehouseandcubemapping
ORDER BY CubeName, CubeTableName
"@
$bodyObject = @{
    request = @{
        method = "post"
        path = "/v2/datasets/default,default/query/sql"
        body = @{
            query = $query
        }
    }
}
$resp = Invoke-RestMethod -Method Post -Uri $uri -Headers @{ Authorization = "Bearer $armToken"; "Content-Type" = "application/json" } -Body ($bodyObject | ConvertTo-Json -Depth 20)
$resp.response.body.ResultSets.Table1 | ConvertTo-Json -Depth 8
```

### Apply a SQL migration file

Important: the SQL connector does not safely accept a whole multi-batch script with `GO` in one call. The proven method is:

1. read the file
2. split on `GO`
3. execute each batch one by one

```powershell
$armToken = az account get-access-token --resource https://management.azure.com/ --query accessToken -o tsv
$scriptPath = "C:\path\to\migration.sql"
$content = [System.IO.File]::ReadAllText($scriptPath)
$batches = [System.Text.RegularExpressions.Regex]::Split($content, '(?im)^\s*GO\s*$') | Where-Object { $_.Trim().Length -gt 0 }
$uri = "https://management.azure.com/subscriptions/8730775e-045c-47d1-a080-e3b9882cec01/resourceGroups/vn-rg-sa-sdp-solution-p/providers/Microsoft.Web/connections/sql-1/dynamicInvoke?api-version=2016-06-01"

$i = 0
foreach ($batch in $batches) {
    $i++
    $bodyObject = @{
        request = @{
            method = "post"
            path = "/v2/datasets/default,default/query/sql"
            body = @{
                query = $batch
            }
        }
    }
    Invoke-RestMethod -Method Post -Uri $uri -Headers @{ Authorization = "Bearer $armToken"; "Content-Type" = "application/json" } -Body ($bodyObject | ConvertTo-Json -Depth 20) | Out-Null
    Write-Output ("Batch " + $i + " applied")
}
```

### Verify schema after a migration

Always run a direct check after a DDL migration:

```powershell
SELECT COUNT(*) AS PolicyRows FROM etl.cuberefreshnotificationpolicy
```

or:

```powershell
SELECT TOP 5 CubeName, CubeTableName, PolicyGroup
FROM etl.datawarehouseandcubemapping
WHERE PolicyGroup IS NOT NULL
```

### Audit shared-column ambiguity after schema changes

If a migration adds columns to shared ETL objects like `ETL.EtlLog`, audit older procedures that join or `OUTER APPLY` those objects before running ADF again.

Example check:

```powershell
SELECT
    OBJECT_SCHEMA_NAME(p.object_id) AS SchemaName,
    p.name AS ProcName
FROM sys.procedures p
JOIN sys.sql_modules sm
    ON sm.object_id = p.object_id
WHERE sm.definition LIKE '%ExtractLoadControlTable%'
  AND sm.definition LIKE '%EtlLog%'
  AND sm.definition LIKE '%DataSourceId%'
ORDER BY p.name
```

For the SQL 209 incident caused after `ETL.EtlLog.DataSourceId` was added, use:

- `docs/hotfix_fix_ExtractEtlGenerateDataForExtractRuntime_DataSourceId.sql`

## Proven Workflow For A Change

### Function App / runtime baseline work

1. Check Azure account and subscription
2. Check Function App settings
3. Check Logic App concurrency
4. Check AAS state / SKU
5. Check elastic pool state
6. Change repo files
7. Deploy Logic App or push code if needed
8. Trigger a controlled run
9. Monitor run + actions
10. Verify SQL side effects or outputs

### SQL guardrail / workflow query change

1. Edit repo workflow JSON first
2. Validate JSON with `ConvertFrom-Json`
3. Deploy `RefreshCube_UAT` first
4. Trigger one run with real payload
5. Trigger one run with no payload / skip behavior
6. If needed, run failure recipient SQL directly
7. Only then mirror to `RefreshCube` prod

## Known Good Validation Patterns

These were useful anchors in prior sessions:

- use a real lineage key that is known to produce changed rows for the target environment
- use a second lineage key or payload that is expected to produce no runnable tables
- for prod validation, prefer one controlled lineage key rather than a broad trigger

Do not hard-code old lineage keys into the operating playbook. Pick live keys based on the current ETL state.

## Common Pitfalls And Fixes

### 1. Wrong Function App name

The real Function App is:

- `vn-fa-sa-sdp-p-aas`

Not:

- `func-dhrefreshaas-prod`

### 2. ARM token expiry during long polling

Symptom:

- `ExpiredAuthenticationToken`

Fix:

- refresh the token inside the polling loop, not just once at the beginning

### 3. SQL connector rejects complex inline aggregates

Symptom:

- `Get_AAS_Model_Tables_JSON_Format` fails
- SQL syntax errors around aggregate expressions

Fix:

- use the `changedRows -> candidate` query pattern
- do not put inline aggregate checks like `MAX(etlLog.ChangedRowCount)` inside `candidate` guardrail `CASE` logic

### 4. Multi-batch SQL script silently does not apply

Symptom:

- no explicit error
- schema objects still missing afterwards

Fix:

- split the script by `GO`
- execute each batch separately
- verify the schema explicitly afterwards

### 5. Wrong SQL connection target

Symptom:

- query succeeds but hits the wrong environment

Fix:

- check the correct connection resource:
  - `sql`
  - `sql-1`
  - `sql-2`
- cross-check against `docs/EnvironmentRoutingAudit.md`

### 6. Empty output shape breaks Logic App actions

Symptom:

- Logic App expressions fail on `Table1[0]`

Fix:

- keep SQL outputs stable
- always return a row with expected fields
- use empty arrays / null payloads instead of missing keys

### 7. Interactive SQL auth is brittle

Symptom:

- `sqlcmd -G` prompts or fails in unattended use

Fix:

- prefer Azure CLI login + ARM token + `dynamicInvoke`

### 8. `az` is installed but not on PATH

Symptom:

- `az` is not recognized

Fix:

- add the Azure CLI install path to the current PowerShell session before retrying

### 9. `az logic workflow update --definition` rejects the file shape

Symptom:

- error saying the value does not contain a `definition` key

Fix:

- pass the full exported workflow JSON file directly with `@file.json`
- do not strip the file down to only a nested fragment unless you re-check the exact CLI contract first

## Recommended Read Order For Another Model

1. `readme.md`
2. `docs/ProjectSessionResume.md`
3. `docs/AzureCliAndDatabaseOperations.md`
4. `docs/SaveChangesFailureEvidence.md`
5. `docs/EnvironmentRoutingAudit.md`
6. `docs/app-settings-production.json`

## When To Use Which Document

- `readme.md`: architecture and entry points
- `docs/ProjectSessionResume.md`: current operational state and restart checklist
- `docs/AzureCliAndDatabaseOperations.md`: exact hands-on commands and workflow
- `docs/SaveChangesFailureEvidence.md`: root cause evidence, validations, rollback guidance
- `docs/EnvironmentRoutingAudit.md`: routing facts and cross-environment implications
