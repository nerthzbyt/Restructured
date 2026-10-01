# ============================================================
# NertzMetalEngine C++ Toolchain Setup
# MSYS2 + MinGW-w64 + CMake + vcpkg + ZeroMQ
# Ejecutar como Administrador en PowerShell
# ============================================================

Write-Host "========================================"
Write-Host " NertzMetalEngine C++ Toolchain Setup"
Write-Host "========================================"
Write-Host ""

# 1. Instalar MSYS2 via winget
Write-Host "[1/6] Instalando MSYS2..."
winget install --id MSYS2.MSYS2 --silent --accept-source-agreements --accept-package-agreements
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [!] MSYS2 ya instalado o requiere reinicio, continuando..."
}

# 2. Path de MSYS2
$msys2_path = "C:\msys64"
if (-not (Test-Path $msys2_path)) {
    Write-Host "[ERROR] MSYS2 no encontrado en $msys2_path. Instalar manualmente desde https://www.msys2.org/"
    exit 1
}

Write-Host "[2/6] Actualizando paquetes base MSYS2..."
& "$msys2_path\usr\bin\bash.exe" -lc "pacman -Syu --noconfirm" 2>&1

Write-Host "[3/6] Instalando MinGW-w64 GCC + CMake + Ninja + Make..."
& "$msys2_path\usr\bin\bash.exe" -lc "pacman -S --noconfirm mingw-w64-x86_64-gcc mingw-w64-x86_64-cmake mingw-w64-x86_64-ninja mingw-w64-x86_64-make mingw-w64-x86_64-pkg-config git" 2>&1

Write-Host "[4/6] Instalando librerias C++ necesarias..."
& "$msys2_path\usr\bin\bash.exe" -lc "pacman -S --noconfirm mingw-w64-x86_64-zeromq mingw-w64-x86_64-cppzmq mingw-w64-x86_64-nlohmann-json mingw-w64-x86_64-openssl mingw-w64-x86_64-curl mingw-w64-x86_64-spdlog mingw-w64-x86_64-fmt mingw-w64-x86_64-libwebsockets" 2>&1

Write-Host "[5/6] Agregando MinGW-w64 al PATH del sistema..."
$mingw_bin = "$msys2_path\mingw64\bin"
$current_path = [System.Environment]::GetEnvironmentVariable("PATH", "User")
if ($current_path -notlike "*$mingw_bin*") {
    [System.Environment]::SetEnvironmentVariable("PATH", "$mingw_bin;$current_path", "User")
    Write-Host "  -> PATH actualizado. Abre una nueva terminal para que tome efecto."
} else {
    Write-Host "  -> MinGW-w64 ya estaba en PATH."
}

Write-Host "[6/6] Verificando instalacion..."
& "$mingw_bin\gcc.exe" --version 2>&1 | Select-Object -First 1
& "$mingw_bin\cmake.exe" --version 2>&1 | Select-Object -First 1

Write-Host ""
Write-Host "========================================"
Write-Host " Setup completado!"
Write-Host " Proximos pasos:"
Write-Host "   1. Abrir NUEVA terminal PowerShell"
Write-Host "   2. cd nertz_cpp"  
Write-Host "   3. .\build.bat"
Write-Host "========================================"
