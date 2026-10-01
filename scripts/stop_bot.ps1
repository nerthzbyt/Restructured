# Detiene instancias del bot y libera el lock de DuckDB.
& (Join-Path $PSScriptRoot "release_duckdb_lock.ps1") @args