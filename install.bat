@echo off
setlocal enabledelayedexpansion

set "PLUGIN_PATH=%localappdata%\GOG.com\Galaxy\plugins\installed\origin_7f53219b-4e2b-4591-9f4f-dfc5f4ba9eb0"

rmdir /S /Q "%PLUGIN_PATH%" 2>nul
mkdir "%PLUGIN_PATH%"

set "SCRIPT_DIR=%~dp0"
set "zip_file=%SCRIPT_DIR%origin_v0.44.4.zip"

if exist "%zip_file%" (
    echo Extracting %zip_file%
    powershell -Command "Expand-Archive -Path '%zip_file%' -DestinationPath '%PLUGIN_PATH%' -Force"
) else (
    echo Error: %zip_file% not found
    pause
)