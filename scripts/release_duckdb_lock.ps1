# Libera el lock de nertz.duckdb deteniendo procesos Python del bot.
param(
    [switch]$ForceAllPython
)

$ErrorActionPreference = "SilentlyContinue"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$DbPath = Join-Path $ProjectRoot "data\nertz.duckdb"

if (-not (Test-Path $DbPath)) {
    Write-Host "No se encontro: $DbPath"
    exit 1
}

Write-Host "Liberando lock de: $DbPath"
$stopped = @()

function Stop-IfBotProcess {
    param([int]$ProcessId, [string]$CommandLine, [string]$Reason)
    if ($ProcessId -eq $PID) { return }
    Write-Host "Deteniendo PID $ProcessId ($Reason)"
    if ($CommandLine) { Write-Host "  $CommandLine" }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
    $script:stopped += $ProcessId
}

$patterns = @(
    "Nertzh\.py",
    "nertz_engine",
    "uvicorn",
    [regex]::Escape($ProjectRoot)
)

Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe' OR Name='py.exe'" |
    ForEach-Object {
        $cmd = [string]$_.CommandLine
        if (-not $cmd) { return }
        foreach ($pat in $patterns) {
            if ($cmd -match $pat) {
                Stop-IfBotProcess -ProcessId $_.ProcessId -CommandLine $cmd -Reason "bot/python del proyecto"
                break
            }
        }
    }

if ($ForceAllPython) {
    Get-Process python*, py*, pythonw* -ErrorAction SilentlyContinue |
        ForEach-Object {
            Stop-IfBotProcess -ProcessId $_.Id -CommandLine "" -Reason "ForceAllPython"
        }
}

if ($stopped.Count -eq 0) {
    Write-Host "No se encontraron procesos del bot."
    Write-Host "Si PyCharm/DBeaver tiene la DB abierta, cierra esa conexion (solo read_only)."
    Write-Host "Reintento agresivo: .\scripts\release_duckdb_lock.ps1 -ForceAllPython"
    exit 1
}

Write-Host "Detenidos $($stopped.Count) proceso(s): $($stopped -join ', ')"
Write-Host "Espera 2s antes de reiniciar Nertzh.py..."
Start-Sleep -Seconds 2
Write-Host "Listo."