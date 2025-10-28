<#
Postgres diagnostics for Windows hosts (PowerShell).
Run this ON THE POSTGRES HOST where Postgres is installed.
It attempts to locate recent Postgres logs and show auth-related failures.
#>
Param()

Write-Host "Postgres diagnostics - $(Get-Date -Format o)"

Function Show-IfExists([string]$path, [string]$label) {
    if (Test-Path $path) {
        Write-Host ("--- {0}: {1} ---" -f $label, $path)
        Get-Content $path -Tail 200 | Write-Host
    }
}

# Attempt common install locations
$common = @(
    "C:\Program Files\PostgreSQL",
    "C:\var\lib\pgsql",
    "$env:ProgramData\postgresql"
)

Write-Host "Searching common log directories..."
foreach ($base in $common) {
    if (Test-Path $base) {
        Get-ChildItem -Path $base -Recurse -Filter "*.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 5 | ForEach-Object {
            Write-Host "--- Log file: $($_.FullName) ---"
            Get-Content $_.FullName -Tail 200 | Select-String -Pattern "password authentication failed|authentication failed|no pg_hba.conf entry" -CaseSensitive:$false | ForEach-Object { Write-Host $_ }
        }
    }
}

Write-Host "If Postgres runs as a Windows service, check the data directory's pg_log folder or the service configuration."
Write-Host "If Postgres runs in Docker on Windows, run: docker logs <postgres_container> --tail 200"
