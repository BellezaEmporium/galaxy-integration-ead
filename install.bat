@echo off
setlocal enabledelayedexpansion
cd /d %~dp0

set "PLUGIN_PATH=%localappdata%\GOG.com\Galaxy\plugins\installed\origin_7f53219b-4e2b-4591-9f4f-dfc5f4ba9eb0"

rmdir /S /Q -rf "%PLUGIN_PATH%"
mkdir "%PLUGIN_PATH%"

set zip_file=%~dp0windows.zip
echo Extracting %zip_file%
powershell Expand-Archive '%zip_file%' -DestinationPath '%PLUGIN_PATH%'