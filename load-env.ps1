param(
    [string]$Path = ".env"
)

$resolvedPath = Join-Path $PSScriptRoot $Path

if (-not (Test-Path $resolvedPath)) {
    throw "Environment file not found: $resolvedPath"
}

Get-Content $resolvedPath | ForEach-Object {
    $line = $_.Trim()

    if (-not $line -or $line.StartsWith("#")) {
        return
    }

    $separatorIndex = $line.IndexOf("=")
    if ($separatorIndex -lt 1) {
        return
    }

    $name = $line.Substring(0, $separatorIndex).Trim()
    $value = $line.Substring($separatorIndex + 1)

    [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
}

Write-Output ("Loaded environment variables from " + $resolvedPath)
