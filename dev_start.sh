#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/DataProtectionTool.OneApp"

AZURITE_BLOB_PORT=10000
AZURITE_QUEUE_PORT=10001
AZURITE_TABLE_PORT=10002
AZURITE_DATA_DIR="$SCRIPT_DIR/.azurite"

DEVSTORE_ACCOUNT="devstoreaccount1"
DEVSTORE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

cleanup() {
    echo ""
    echo "Shutting down Azurite (pid $AZURITE_PID)..."
    kill "$AZURITE_PID" 2>/dev/null || true
    wait "$AZURITE_PID" 2>/dev/null || true
    echo "Done."
}

mkdir -p "$AZURITE_DATA_DIR"

echo "Starting Azurite..."
azurite \
    --blobPort "$AZURITE_BLOB_PORT" \
    --queuePort "$AZURITE_QUEUE_PORT" \
    --tablePort "$AZURITE_TABLE_PORT" \
    --location "$AZURITE_DATA_DIR" \
    --silent &
AZURITE_PID=$!

trap cleanup EXIT INT TERM

sleep 2
if ! kill -0 "$AZURITE_PID" 2>/dev/null; then
    echo "ERROR: Azurite failed to start." >&2
    exit 1
fi
echo "Azurite running (pid $AZURITE_PID) — blob :$AZURITE_BLOB_PORT, queue :$AZURITE_QUEUE_PORT, table :$AZURITE_TABLE_PORT"

export AzureTableStorage__ConnectionString="DefaultEndpointsProtocol=http;AccountName=$DEVSTORE_ACCOUNT;AccountKey=$DEVSTORE_KEY;TableEndpoint=http://127.0.0.1:$AZURITE_TABLE_PORT/$DEVSTORE_ACCOUNT;"
export AzureBlobStorage__StorageAccount="$DEVSTORE_ACCOUNT"
export AzureBlobStorage__AccessKey="$DEVSTORE_KEY"
export AzureBlobStorage__Container="data"
export AzureBlobStorage__PreviewContainer="preview"

echo "Building frontend..."
cd "$PROJECT_DIR/frontend"
npm install
npm run build

echo "Starting DataProtectionTool.OneApp..."
cd "$PROJECT_DIR"
dotnet run
