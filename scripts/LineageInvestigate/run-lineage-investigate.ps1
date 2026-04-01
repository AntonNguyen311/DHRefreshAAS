# Loads .env then runs dotnet. Uses SELF_SERVICE_SQL_CONNECTION_STRING as-is (User ID may be literal "$(sqlServerUsername)").

$ErrorActionPreference = "Stop"
$here = $PSScriptRoot
$repoRoot = Resolve-Path (Join-Path $here "..\..")
. (Join-Path $repoRoot "load-env.ps1")

Set-Location $here
dotnet run -c Release
