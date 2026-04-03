@echo off
setlocal

set SCRIPT_DIR=%~dp0
set PROJECT_DIR=%SCRIPT_DIR%DataProtectionTool.OneApp

set AZURITE_BLOB_PORT=10000
set AZURITE_QUEUE_PORT=10001
set AZURITE_TABLE_PORT=10002
set AZURITE_DATA_DIR=%SCRIPT_DIR%.azurite

set DEVSTORE_ACCOUNT=devstoreaccount1
set DEVSTORE_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==

if not exist "%AZURITE_DATA_DIR%" mkdir "%AZURITE_DATA_DIR%"

echo Starting Azurite...
start "Azurite" /B azurite ^
    --blobPort %AZURITE_BLOB_PORT% ^
    --queuePort %AZURITE_QUEUE_PORT% ^
    --tablePort %AZURITE_TABLE_PORT% ^
    --location "%AZURITE_DATA_DIR%" ^
    --silent

timeout /t 2 /nobreak >nul

set AzureTableStorage__ConnectionString=DefaultEndpointsProtocol=http;AccountName=%DEVSTORE_ACCOUNT%;AccountKey=%DEVSTORE_KEY%;TableEndpoint=http://127.0.0.1:%AZURITE_TABLE_PORT%/%DEVSTORE_ACCOUNT%;
set AzureBlobStorage__StorageAccount=%DEVSTORE_ACCOUNT%
set AzureBlobStorage__AccessKey=%DEVSTORE_KEY%
set AzureBlobStorage__Container=data
set AzureBlobStorage__PreviewContainer=preview

echo Building frontend...
cd /d "%PROJECT_DIR%\frontend"
call npm install
call npm run build

echo Starting DataProtectionTool.OneApp...
cd /d "%PROJECT_DIR%"
dotnet run

echo Stopping Azurite...
taskkill /F /IM azurite.exe 2>nul
taskkill /F /IM node.exe /FI "WINDOWTITLE eq Azurite" 2>nul

endlocal
