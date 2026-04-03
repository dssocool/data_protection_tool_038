using System.Text.Json;
using Azure.Storage.Blobs;
using DataProtectionTool.OneApp.Helpers;
using DataProtectionTool.OneApp.Models;
using DataProtectionTool.OneApp.Services;

namespace DataProtectionTool.OneApp.Endpoints;

public static class TableEndpoints
{
    public static void MapTableEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/agents/{path}/sample-table", async (string path, HttpRequest request,
            SessionManager sessionManager, ClientTableService clientTableService,
            SqlOperationService sqlOps, BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig) =>
        {
            return await SampleTableCoreAsync(path, request, sessionManager, clientTableService, sqlOps, blobClient, blobStorageConfig, useCache: true, labelPrefix: "Sample table");
        });

        app.MapPost("/api/agents/{path}/reload-sample-table", async (string path, HttpRequest request,
            SessionManager sessionManager, ClientTableService clientTableService,
            SqlOperationService sqlOps, BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig) =>
        {
            return await SampleTableCoreAsync(path, request, sessionManager, clientTableService, sqlOps, blobClient, blobStorageConfig, useCache: false, labelPrefix: "Reload sample table");
        });
    }

    private static async Task<IResult> SampleTableCoreAsync(
        string path, HttpRequest request,
        SessionManager sessionManager, ClientTableService clientTableService,
        SqlOperationService sqlOps, BlobServiceClient blobClient, BlobStorageConfig blobStorageConfig,
        bool useCache, string labelPrefix)
    {
        if (!EndpointHelpers.TryGetSession(sessionManager, path, out var sessionInfo, out _))
        {
            var notFoundEvtSummary = $"{labelPrefix} failed: session not found";
            return EndpointHelpers.EventResult(false, "Agent not found.", "sample_table", notFoundEvtSummary);
        }

        var partitionKey = ClientEntity.BuildPartitionKey(sessionInfo.Oid, sessionInfo.Tid);
        var body = await request.ReadBodyAsync();

        string connRowKey = "", schema = "", tName = "";
        try
        {
            using var bodyDoc = JsonDocument.Parse(body);
            connRowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
            schema = bodyDoc.RootElement.TryGetProperty("schema", out var sEl) ? sEl.GetString() ?? "" : "";
            tName = bodyDoc.RootElement.TryGetProperty("tableName", out var tEl) ? tEl.GetString() ?? "" : "";
        }
        catch
        {
            if (!useCache)
            {
                var invalidBodyEvtSummary = $"{labelPrefix} failed: invalid request body";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", invalidBodyEvtSummary);
                return EndpointHelpers.EventResult(false, "Invalid request body.", "sample_table", invalidBodyEvtSummary);
            }
        }

        var tableLabel = $"{schema}.{tName}";

        var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, connRowKey);
        DataItemEntity? dataItem = null;

        if (useCache && connEntity != null)
        {
            dataItem = await clientTableService.GetDataItemByTableAsync(
                partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tName);

            if (dataItem != null && !string.IsNullOrEmpty(dataItem.PreviewFileList))
            {
                var cachedFilenames = dataItem.PreviewFileList.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
                var allBlobsExist = true;
                try
                {
                    var containerClient = blobClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
                    foreach (var fn in cachedFilenames)
                    {
                        if (!await containerClient.GetBlobClient(fn).ExistsAsync())
                        {
                            allBlobsExist = false;
                            break;
                        }
                    }
                }
                catch
                {
                    allBlobsExist = false;
                }

                if (allBlobsExist)
                {
                    var evtSummary = $"Sample table (cached): {tableLabel}";
                    _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", evtSummary);
                    return Results.Ok(new
                    {
                        success = true,
                        filenames = cachedFilenames,
                        cached = true,
                        @event = EndpointHelpers.EventPayload("sample_table", evtSummary)
                    });
                }

                await clientTableService.UpdatePreviewFileListAsync(dataItem, "");
            }
        }
        else if (!useCache && connEntity != null)
        {
            dataItem = await clientTableService.GetDataItemByTableAsync(
                partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tName);
        }
        else if (!useCache && connEntity == null)
        {
            return Results.NotFound(new { error = "Connection not found." });
        }

        try
        {
            var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
            if (string.IsNullOrWhiteSpace(uniqueId) || !EndpointHelpers.IsDigitsOnly(uniqueId))
            {
                var missingIdEvtSummary = $"{labelPrefix} failed: {tableLabel}";
                var missingIdDetail = "User unique ID is missing.";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", missingIdEvtSummary, missingIdDetail);
                return EndpointHelpers.EventResult(false, missingIdDetail, "sample_table", missingIdEvtSummary, missingIdDetail);
            }

            var sqlStatement = $"SELECT * FROM [{schema}].[{tName}] TABLESAMPLE (200 ROWS)";
            var result = await sqlOps.SampleToParquetAsync(partitionKey, connRowKey, uniqueId, sqlStatement);

            if (result.Success && dataItem != null && result.Filenames.Count > 0)
            {
                _ = clientTableService.UpdatePreviewFileListAsync(dataItem, string.Join(",", result.Filenames));
            }

            var evtSummary2 = result.Success
                ? $"{labelPrefix}: {tableLabel}"
                : $"{labelPrefix} failed: {tableLabel}";
            _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", evtSummary2);

            return Results.Ok(new
            {
                success = result.Success,
                filenames = result.Filenames,
                @event = EndpointHelpers.EventPayload("sample_table", evtSummary2)
            });
        }
        catch (Exception ex)
        {
            var evtSummary = $"{labelPrefix} error: {ex.Message}";
            _ = clientTableService.AppendEventAsync(partitionKey, "sample_table", evtSummary);
            return EndpointHelpers.EventResult(false, $"{labelPrefix} error: {ex.Message}", "sample_table", evtSummary);
        }
    }
}
