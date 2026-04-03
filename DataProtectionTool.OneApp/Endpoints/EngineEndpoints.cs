using System.Text.Json;
using Azure.Storage.Blobs;
using DataProtectionTool.OneApp.Helpers;
using DataProtectionTool.OneApp.Models;
using DataProtectionTool.OneApp.Services;
using Microsoft.Extensions.Logging;

namespace DataProtectionTool.OneApp.Endpoints;

public static class EngineEndpoints
{
    public static void MapEngineEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/agents/{path}/dp-preview", async (string path, HttpContext httpContext,
            SessionManager sessionManager, ClientTableService clientTableService,
            SqlOperationService sqlOps, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi, EngineMetadataService metadataService,
            ILoggerFactory loggerFactory) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            var info = await EndpointHelpers.RequireSessionAsync(sessionManager, path, response);
            if (info is null) return;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            SseWriter.SetupHeaders(response);
            var statusSteps = new List<string>();

            async Task WriteStatus(string msg)
            {
                await SseWriter.WriteEventAsync(response, "status", msg);
                statusSteps.Add(msg);
            }

            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                var root = bodyDoc.RootElement;
                var rowKey = root.GetProperty("rowKey").GetString() ?? "";
                var schema = root.GetProperty("schema").GetString() ?? "";
                var tableName = root.GetProperty("tableName").GetString() ?? "";

                var previewFilenames = ParseStringArray(root, "previewBlobFilenames");
                var previewHeaders = ParseStringArray(root, "previewHeaders");
                var previewColumnTypes = ParseStringArray(root, "previewColumnTypes");

                if (previewFilenames.Count == 0)
                {
                    await SseWriter.WriteErrorAsync(response, "No preview files available. Please preview the table first.");
                    return;
                }

                if (!await EngineRelayService.ValidateEngineConfigAsync(dataEngineConfig, response, requireProfileSetId: true))
                    return;

                var engineBaseUrl = engineApi.BaseUrl;

                await WriteStatus("Copying preview files...");
                var previewContainerClient = blobClient.GetBlobContainerClient(blobConfig.PreviewContainer);
                var engineContainerClient = blobClient.GetBlobContainerClient(blobConfig.Container);

                await CopyBlobsAsync(previewContainerClient, engineContainerClient, previewFilenames);

                await WriteStatus("Fetching SQL column types...");
                var sqlColumnTypes = new List<string>();
                try
                {
                    var fetchTypesResult = await sqlOps.ExecuteSqlAsync(partitionKey, rowKey,
                        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName ORDER BY ORDINAL_POSITION",
                        new Dictionary<string, string> { ["@schema"] = schema, ["@tableName"] = tableName });

                    if (fetchTypesResult.Success)
                    {
                        var typeByName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        foreach (var col in fetchTypesResult.Rows)
                        {
                            var colName = col.TryGetValue("COLUMN_NAME", out var cn) ? cn?.ToString() ?? "" : "";
                            var colType = col.TryGetValue("DATA_TYPE", out var ct) ? ct?.ToString() ?? "" : "";
                            if (!string.IsNullOrEmpty(colName))
                                typeByName[colName] = colType;
                        }

                        foreach (var header in previewHeaders)
                        {
                            sqlColumnTypes.Add(typeByName.TryGetValue(header, out var t) ? t : "");
                        }
                    }
                }
                catch (Exception ex)
                {
                    loggerFactory.CreateLogger<EngineApiClient>().LogWarning(ex, "Failed to fetch SQL types");
                }

                var connEntityForFormat = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
                DataItemEntity? dataItemForFormat = null;
                string fileFormatId = "";

                if (connEntityForFormat != null)
                {
                    dataItemForFormat = await clientTableService.GetDataItemByTableAsync(
                        partitionKey, connEntityForFormat.ServerName, connEntityForFormat.DatabaseName, schema, tableName);
                    if (dataItemForFormat != null && !string.IsNullOrEmpty(dataItemForFormat.FileFormatId))
                    {
                        fileFormatId = dataItemForFormat.FileFormatId;
                    }
                }

                if (string.IsNullOrEmpty(fileFormatId))
                {
                    await WriteStatus("Creating file format...");
                    var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
                    var blobRef = containerClient.GetBlobClient(previewFilenames[0]);
                    using var downloadStream = new MemoryStream();
                    await blobRef.DownloadToAsync(downloadStream);
                    var fileBytes = downloadStream.ToArray();

                    var (formatSuccess, newFileFormatId, _) = await engineApi.CreateFileFormatAsync(fileBytes, previewFilenames[0]);
                    if (!formatSuccess)
                    {
                        await SseWriter.WriteErrorAsync(response, "File format creation failed.");
                        return;
                    }

                    fileFormatId = newFileFormatId;

                    if (!string.IsNullOrEmpty(fileFormatId) && dataItemForFormat != null)
                    {
                        await clientTableService.UpdateFileFormatIdAsync(dataItemForFormat, fileFormatId);
                    }
                }
                else
                {
                    await WriteStatus("Creating file format... (skipped, already exists)");
                }

                await WriteStatus("Creating file ruleset...");
                var dryRunUuid = Guid.NewGuid().ToString("N");
                var rulesetName = $"ruleset_{dryRunUuid}";

                var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(rulesetName, dataEngineConfig.ConnectorId);
                if (!rulesetSuccess)
                {
                    await SseWriter.WriteErrorAsync(response, "File ruleset creation failed.");
                    return;
                }

                var (metaBatchSuccess, fileMetadataIds) = await EngineRelayService.CreateFileMetadataBatchAsync(
                    engineApi, response, previewFilenames, fileRulesetId, fileFormatId, statusSteps);
                if (!metaBatchSuccess) return;

                await WriteStatus("Creating profile job...");
                var profileJobId = await CreateEngineJobAsync(engineApi, response, "profile-jobs", new
                {
                    jobName = $"profile_{dryRunUuid}",
                    profileSetId = int.Parse(dataEngineConfig.ProfileSetId),
                    rulesetId = int.Parse(fileRulesetId)
                }, "profileJobId", "Profile job creation");
                if (profileJobId == null) return;

                await WriteStatus("Creating masking job...");
                var maskingJobId = await CreateEngineJobAsync(engineApi, response, "masking-jobs", new
                {
                    jobName = $"masking_{dryRunUuid}",
                    rulesetId = int.Parse(fileRulesetId),
                    onTheFlyMasking = false
                }, "maskingJobId", "Masking job creation");
                if (maskingJobId == null) return;

                await WriteStatus("Running profile job...");
                var profileStatus = await ExecuteAndPollAsync(
                    engineApi, response, profileJobId, "profile job", statusSteps: statusSteps);
                if (profileStatus == null) return;

                if (previewHeaders.Count > 0 && previewColumnTypes.Count == previewHeaders.Count)
                {
                    await WriteStatus("Applying mapping rules to column rules...");

                    var sqlTypeByColumn = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < previewHeaders.Count; i++)
                        sqlTypeByColumn[previewHeaders[i]] = previewColumnTypes[i];

                    try
                    {
                        var fixedCount = await ApplyMappingRulesAsync(
                            engineApi, metadataService, fileFormatId, sqlTypeByColumn, WriteStatus);

                        if (fixedCount > 0)
                            await WriteStatus($"Fixed {fixedCount} column rule(s) with incompatible algorithm types.");
                        else
                            await WriteStatus("All column rules have compatible algorithm types.");
                    }
                    catch (Exception ex)
                    {
                        await WriteStatus($"Warning: Could not apply mapping rules: {ex.Message}");
                    }
                }

                await WriteStatus("Running masking job...");
                var maskingStatus = await ExecuteAndPollAsync(
                    engineApi, response, maskingJobId, "masking job", statusSteps: statusSteps);
                if (maskingStatus == null) return;

                await WriteStatus("Copying masked results...");
                var maskedFilenames = await CopyBlobsWithRenameAsync(
                    engineContainerClient, previewContainerClient, previewFilenames, $"dryrun_{dryRunUuid}");

                object? columnRulesPayload = null;
                try
                {
                    await WriteStatus("Fetching column rules...");
                    columnRulesPayload = await FetchEnrichedColumnRulesPayload(engineApi, metadataService, fileFormatId);
                }
                catch { }

                var dryRunEvtSummary = $"DP preview completed: fileFormatId={fileFormatId}, fileRulesetId={fileRulesetId}, " +
                    $"profileJobId={profileJobId} ({profileStatus}), maskingJobId={maskingJobId} ({maskingStatus})";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview", dryRunEvtSummary, stepsDetail);
                await SseWriter.WriteEventPayloadAsync(response, "dp_preview", dryRunEvtSummary);

                var completeJson = JsonSerializer.Serialize(new
                {
                    success = true,
                    fileFormatId,
                    fileRulesetId,
                    fileMetadataIds,
                    profileJobId,
                    profileStatus,
                    maskingJobId,
                    maskingStatus,
                    maskedFilenames,
                    sqlColumnTypes,
                    columnRules = columnRulesPayload
                });
                await SseWriter.WriteEventAsync(response, "complete", completeJson);
            }
            catch (Exception ex)
            {
                var evtSummary = $"DP preview error: {ex.Message}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview", evtSummary, stepsDetail);
                await SseWriter.WriteEventPayloadAsync(response, "dp_preview", evtSummary);
                await SseWriter.WriteErrorAsync(response, $"DP preview error: {ex.Message}");
            }
        });

        app.MapPost("/api/agents/{path}/dp-preview-multi", async (string path, HttpContext httpContext,
            SessionManager sessionManager, ClientTableService clientTableService,
            SqlOperationService sqlOps, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi, EngineMetadataService metadataService,
            ILoggerFactory loggerFactory) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            var info = await EndpointHelpers.RequireSessionAsync(sessionManager, path, response);
            if (info is null) return;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            SseWriter.SetupHeaders(response);
            var statusSteps = new List<string>();
            var sseLock = new SemaphoreSlim(1, 1);

            async Task WriteStatus(string msg)
            {
                await EngineRelayService.WriteStatusThreadSafeAsync(response, sseLock, statusSteps, msg);
            }

            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                var root = bodyDoc.RootElement;

                if (!root.TryGetProperty("tables", out var tablesEl) || tablesEl.ValueKind != JsonValueKind.Array)
                {
                    await SseWriter.WriteErrorAsync(response, "Request must include a 'tables' array.");
                    return;
                }

                var tables = new List<(string rowKey, string schema, string tableName, List<string> previewFilenames)>();
                foreach (var tEl in tablesEl.EnumerateArray())
                {
                    var rk = tEl.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
                    var sc = tEl.TryGetProperty("schema", out var scEl) ? scEl.GetString() ?? "" : "";
                    var tn = tEl.TryGetProperty("tableName", out var tnEl) ? tnEl.GetString() ?? "" : "";
                    var pf = new List<string>();
                    if (tEl.TryGetProperty("previewFilenames", out var pfEl) && pfEl.ValueKind == JsonValueKind.Array)
                        pf = pfEl.EnumerateArray().Select(e => e.GetString() ?? "").Where(f => f != "").ToList();
                    if (!string.IsNullOrEmpty(rk) && !string.IsNullOrEmpty(sc) && !string.IsNullOrEmpty(tn))
                        tables.Add((rk, sc, tn, pf));
                }

                if (tables.Count == 0)
                {
                    await SseWriter.WriteErrorAsync(response, "No valid tables provided.");
                    return;
                }

                if (!await EngineRelayService.ValidateEngineConfigAsync(dataEngineConfig, response, requireProfileSetId: true))
                    return;

                var previewContainerClient = blobClient.GetBlobContainerClient(blobConfig.PreviewContainer);
                var engineContainerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
                var dryRunUuid = Guid.NewGuid().ToString("N");

                var group2Tcs = new TaskCompletionSource<(string fileRulesetId, string profileJobId, string maskingJobId)>(
                    TaskCreationOptions.RunContinuationsAsynchronously);

                var group2Task = Task.Run(async () =>
                {
                    try
                    {
                        await WriteStatus("Creating file ruleset...");
                        var rulesetName = $"ruleset_{dryRunUuid}";
                        var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(rulesetName, dataEngineConfig.ConnectorId);
                        if (!rulesetSuccess)
                        {
                            group2Tcs.TrySetException(new InvalidOperationException("File ruleset creation failed."));
                            return;
                        }

                        await WriteStatus("Creating profile job...");
                        using var profileJobResp = await EngineRelayService.CallEngineAsync(engineApi, "POST", "profile-jobs", new
                        {
                            jobName = $"profile_{dryRunUuid}",
                            profileSetId = int.Parse(dataEngineConfig.ProfileSetId),
                            rulesetId = int.Parse(fileRulesetId)
                        });

                        if (!(profileJobResp.RootElement.TryGetProperty("success", out var pjSuccessEl) && pjSuccessEl.GetBoolean()))
                        {
                            var msg = profileJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Profile job creation failed.";
                            group2Tcs.TrySetException(new InvalidOperationException(msg ?? "Profile job creation failed."));
                            return;
                        }

                        var profileJobId = EngineRelayService.ExtractBodyField(profileJobResp, "profileJobId");
                        if (string.IsNullOrEmpty(profileJobId))
                        {
                            group2Tcs.TrySetException(new InvalidOperationException("Profile job creation returned no profileJobId."));
                            return;
                        }

                        await WriteStatus("Creating masking job...");
                        using var maskingJobResp = await EngineRelayService.CallEngineAsync(engineApi, "POST", "masking-jobs", new
                        {
                            jobName = $"masking_{dryRunUuid}",
                            rulesetId = int.Parse(fileRulesetId),
                            onTheFlyMasking = false
                        });

                        if (!(maskingJobResp.RootElement.TryGetProperty("success", out var mjSuccessEl) && mjSuccessEl.GetBoolean()))
                        {
                            var msg = maskingJobResp.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "Masking job creation failed.";
                            group2Tcs.TrySetException(new InvalidOperationException(msg ?? "Masking job creation failed."));
                            return;
                        }

                        var maskingJobId = EngineRelayService.ExtractBodyField(maskingJobResp, "maskingJobId");
                        if (string.IsNullOrEmpty(maskingJobId))
                        {
                            group2Tcs.TrySetException(new InvalidOperationException("Masking job creation returned no maskingJobId."));
                            return;
                        }

                        group2Tcs.TrySetResult((fileRulesetId, profileJobId, maskingJobId));
                    }
                    catch (Exception ex)
                    {
                        group2Tcs.TrySetException(ex);
                    }
                });

                var tableResults = new TablePrepResult[tables.Count];

                var perTableTasks = tables.Select((table, idx) => Task.Run(async () =>
                {
                    var (rowKey, schema, tableName, previewFilenames) = table;
                    var tableLabel = $"{schema}.{tableName}";

                    var filenames = new List<string>();

                    if (previewFilenames.Count > 0)
                    {
                        await WriteStatus($"[{tableLabel}] Verifying cached sample...");
                        var allExist = true;
                        foreach (var fn in previewFilenames)
                        {
                            if (!await previewContainerClient.GetBlobClient(fn).ExistsAsync())
                            {
                                allExist = false;
                                break;
                            }
                        }
                        if (allExist)
                            filenames = previewFilenames;
                    }

                    if (filenames.Count == 0)
                    {
                        await WriteStatus($"[{tableLabel}] Sampling table...");
                        var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
                        var rk = rowKey;
                        var sc = schema;
                        var tn = tableName;
                        var sampleResult = await sqlOps.SampleToParquetAsync(partitionKey, rk, uniqueId ?? "", $"SELECT * FROM [{sc}].[{tn}] TABLESAMPLE (200 ROWS)");
                        if (!sampleResult.Success)
                            throw new InvalidOperationException($"[{tableLabel}] Sample failed.");
                        filenames = sampleResult.Filenames;

                        if (filenames.Count == 0)
                            throw new InvalidOperationException($"[{tableLabel}] Sample produced no files.");
                    }

                    await WriteStatus($"[{tableLabel}] Copying preview files...");
                    await CopyBlobsAsync(previewContainerClient, engineContainerClient, filenames);

                    await WriteStatus($"[{tableLabel}] Fetching SQL column types...");
                    var previewHeaders = new List<string>();
                    var sqlColumnTypes = new List<string>();
                    try
                    {
                        var fetchTypesResult = await sqlOps.ExecuteSqlAsync(partitionKey, rowKey,
                            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName ORDER BY ORDINAL_POSITION",
                            new Dictionary<string, string> { ["@schema"] = schema, ["@tableName"] = tableName });

                        if (fetchTypesResult.Success)
                        {
                            foreach (var col in fetchTypesResult.Rows)
                            {
                                var colName = col.TryGetValue("COLUMN_NAME", out var cn) ? cn?.ToString() ?? "" : "";
                                var colType = col.TryGetValue("DATA_TYPE", out var ct) ? ct?.ToString() ?? "" : "";
                                if (!string.IsNullOrEmpty(colName))
                                {
                                    previewHeaders.Add(colName);
                                    sqlColumnTypes.Add(colType);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        loggerFactory.CreateLogger<EngineApiClient>().LogWarning(ex, "Failed to fetch SQL types for {TableLabel}", tableLabel);
                    }

                    var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
                    DataItemEntity? dataItem = null;
                    string fileFormatId = "";

                    if (connEntity != null)
                    {
                        dataItem = await clientTableService.GetDataItemByTableAsync(
                            partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tableName);
                        if (dataItem != null && !string.IsNullOrEmpty(dataItem.FileFormatId))
                            fileFormatId = dataItem.FileFormatId;
                    }

                    if (string.IsNullOrEmpty(fileFormatId))
                    {
                        await WriteStatus($"[{tableLabel}] Creating file format...");
                        var containerClient = blobClient.GetBlobContainerClient(blobConfig.Container);
                        var blobRef = containerClient.GetBlobClient(filenames[0]);
                        using var downloadStream = new MemoryStream();
                        await blobRef.DownloadToAsync(downloadStream);
                        var fileBytes = downloadStream.ToArray();

                        var (formatSuccess, newFileFormatId, _) = await engineApi.CreateFileFormatAsync(fileBytes, filenames[0]);
                        if (!formatSuccess)
                            throw new InvalidOperationException($"[{tableLabel}] File format creation failed.");

                        fileFormatId = newFileFormatId;
                        if (!string.IsNullOrEmpty(fileFormatId) && dataItem != null)
                            await clientTableService.UpdateFileFormatIdAsync(dataItem, fileFormatId);
                    }
                    else
                    {
                        await WriteStatus($"[{tableLabel}] File format already exists.");
                    }

                    var (rulesetId, _, _) = await group2Tcs.Task;

                    await WriteStatus($"[{tableLabel}] Creating file metadata...");
                    var allMetadataIds = new List<string>();
                    for (var fi = 0; fi < filenames.Count; fi++)
                    {
                        var (metaSuccess, fileMetadataId, _) = await engineApi.CreateFileMetadataAsync(filenames[fi], rulesetId, fileFormatId);
                        if (!metaSuccess)
                            throw new InvalidOperationException($"[{tableLabel}] File metadata creation failed for {filenames[fi]}.");
                        allMetadataIds.Add(fileMetadataId);
                    }

                    await WriteStatus($"[{tableLabel}] Table preparation complete.");

                    tableResults[idx] = new TablePrepResult
                    {
                        RowKey = rowKey,
                        Schema = schema,
                        TableName = tableName,
                        Filenames = filenames,
                        FileFormatId = fileFormatId,
                        PreviewHeaders = previewHeaders,
                        SqlColumnTypes = sqlColumnTypes,
                        FileMetadataIds = allMetadataIds,
                    };
                })).ToArray();

                try
                {
                    await Task.WhenAll(perTableTasks);
                }
                catch (Exception ex)
                {
                    loggerFactory.CreateLogger<EngineApiClient>().LogError(ex, "Per-table tasks failed");
                    var failures = perTableTasks
                        .Where(t => t.IsFaulted)
                        .Select(t => t.Exception?.InnerExceptions.FirstOrDefault()?.Message ?? "Unknown error")
                        .ToList();
                    var failMsg = failures.Count > 0
                        ? string.Join("; ", failures)
                        : ex.Message;
                    await SseWriter.WriteErrorAsync(response, $"Table preparation failed: {failMsg}");
                    return;
                }

                try
                {
                    await group2Task;
                }
                catch (Exception ex)
                {
                    loggerFactory.CreateLogger<EngineApiClient>().LogError(ex, "Group 2 task failed");
                }

                string g2RulesetId, g2ProfileJobId, g2MaskingJobId;
                try
                {
                    var group2Result = await group2Tcs.Task;
                    g2RulesetId = group2Result.fileRulesetId;
                    g2ProfileJobId = group2Result.profileJobId;
                    g2MaskingJobId = group2Result.maskingJobId;
                }
                catch (Exception ex)
                {
                    loggerFactory.CreateLogger<EngineApiClient>().LogError(ex, "Ruleset/job creation failed");
                    await SseWriter.WriteErrorAsync(response, $"Ruleset/job creation failed: {ex.Message}");
                    return;
                }

                await WriteStatus("Running profile job...");
                var profileStatus = await ExecuteAndPollAsync(
                    engineApi, response, g2ProfileJobId, "profile job", statusSteps: statusSteps);
                if (profileStatus == null) return;

                await metadataService.EnsureLoadedAsync();
                foreach (var tr in tableResults)
                {
                    if (tr.PreviewHeaders.Count == 0 || tr.SqlColumnTypes.Count != tr.PreviewHeaders.Count)
                        continue;

                    var tableLabel = $"{tr.Schema}.{tr.TableName}";
                    await WriteStatus($"[{tableLabel}] Applying mapping rules...");

                    var sqlTypeByColumn = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < tr.PreviewHeaders.Count; i++)
                        sqlTypeByColumn[tr.PreviewHeaders[i]] = tr.SqlColumnTypes[i];

                    try
                    {
                        var fixedCount = await ApplyMappingRulesAsync(
                            engineApi, metadataService, tr.FileFormatId, sqlTypeByColumn,
                            msg => WriteStatus($"[{tableLabel}] {msg}"));

                        if (fixedCount > 0)
                            await WriteStatus($"[{tableLabel}] Fixed {fixedCount} column rule(s).");
                    }
                    catch (Exception ex)
                    {
                        await WriteStatus($"[{tableLabel}] Warning: Could not apply mapping rules: {ex.Message}");
                    }
                }

                await WriteStatus("Running masking job...");
                var maskingStatus = await ExecuteAndPollAsync(
                    engineApi, response, g2MaskingJobId, "masking job", statusSteps: statusSteps);
                if (maskingStatus == null) return;

                await WriteStatus("Copying masked results...");
                var perTableComplete = new List<object>();
                foreach (var tr in tableResults)
                {
                    var maskedFilenames = await CopyBlobsWithRenameAsync(
                        engineContainerClient, previewContainerClient, tr.Filenames, $"dryrun_{dryRunUuid}");

                    object? columnRulesPayload = null;
                    try
                    {
                        await WriteStatus($"[{tr.Schema}.{tr.TableName}] Fetching column rules...");
                        columnRulesPayload = await FetchEnrichedColumnRulesPayload(engineApi, metadataService, tr.FileFormatId);
                    }
                    catch { }

                    perTableComplete.Add(new
                    {
                        rowKey = tr.RowKey,
                        schema = tr.Schema,
                        tableName = tr.TableName,
                        fileFormatId = tr.FileFormatId,
                        maskedFilenames,
                        sqlColumnTypes = tr.SqlColumnTypes,
                        columnRules = columnRulesPayload,
                    });
                }

                var evtSummary = $"DP preview (multi) completed: {tables.Count} table(s), " +
                    $"fileRulesetId={g2RulesetId}, profileJobId={g2ProfileJobId} ({profileStatus}), " +
                    $"maskingJobId={g2MaskingJobId} ({maskingStatus})";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview_multi", evtSummary, stepsDetail);
                await SseWriter.WriteEventPayloadAsync(response, "dp_preview_multi", evtSummary);

                var completeJson = JsonSerializer.Serialize(new
                {
                    success = true,
                    fileRulesetId = g2RulesetId,
                    profileJobId = g2ProfileJobId,
                    profileStatus,
                    maskingJobId = g2MaskingJobId,
                    maskingStatus,
                    tables = perTableComplete,
                });
                await SseWriter.WriteEventAsync(response, "complete", completeJson);
            }
            catch (Exception ex)
            {
                var errMsg = ex is AggregateException agg ? agg.InnerExceptions[0].Message : ex.Message;
                var evtSummary = $"DP preview (multi) error: {errMsg}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_preview_multi", evtSummary, stepsDetail);
                await SseWriter.WriteEventPayloadAsync(response, "dp_preview_multi", evtSummary);
                await SseWriter.WriteErrorAsync(response, $"DP preview (multi) error: {errMsg}");
            }
        });

        app.MapPost("/api/agents/{path}/dp-run", async (string path, HttpContext httpContext,
            SessionManager sessionManager, ClientTableService clientTableService,
            SqlOperationService sqlOps, DataEngineConfig dataEngineConfig,
            BlobServiceClient blobClient, BlobStorageConfig blobConfig,
            EngineApiClient engineApi) =>
        {
            var response = httpContext.Response;
            var request = httpContext.Request;

            var info = await EndpointHelpers.RequireSessionAsync(sessionManager, path, response);
            if (info is null) return;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            SseWriter.SetupHeaders(response);
            var statusSteps = new List<string>();

            async Task WriteStatus(string msg)
            {
                await SseWriter.WriteEventAsync(response, "status", msg);
                statusSteps.Add(msg);
            }

            var flowId = "";
            using (var preDoc = JsonDocument.Parse(body))
            {
                var flowRowKey = preDoc.RootElement.TryGetProperty("flowRowKey", out var frEl) ? frEl.GetString() ?? "" : "";
                flowId = flowRowKey.StartsWith("flow_") ? flowRowKey["flow_".Length..] : flowRowKey;
            }

            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                var root = bodyDoc.RootElement;
                var rowKey = root.GetProperty("rowKey").GetString() ?? "";
                var schema = root.GetProperty("schema").GetString() ?? "";
                var tableName = root.GetProperty("tableName").GetString() ?? "";
                var destConnectionRowKey = root.TryGetProperty("destConnectionRowKey", out var dcrEl) ? dcrEl.GetString() ?? "" : "";
                var destSchema = root.TryGetProperty("destSchema", out var dsEl) ? dsEl.GetString() ?? "" : "";

                if (string.IsNullOrEmpty(destConnectionRowKey) || string.IsNullOrEmpty(destSchema))
                {
                    await SseWriter.WriteErrorAsync(response, "Destination connection and schema are required.");
                    return;
                }

                if (!await EngineRelayService.ValidateEngineConfigAsync(dataEngineConfig, response))
                    return;

                var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
                if (connEntity == null)
                {
                    await SseWriter.WriteErrorAsync(response, "Connection not found.");
                    return;
                }

                var dataItem = await clientTableService.GetDataItemByTableAsync(
                    partitionKey, connEntity.ServerName, connEntity.DatabaseName, schema, tableName);

                var fileFormatId = dataItem != null ? dataItem.FileFormatId : "";
                if (string.IsNullOrEmpty(fileFormatId))
                {
                    await SseWriter.WriteErrorAsync(response, "File format not found. Please run Dry Run first.");
                    return;
                }

                await WriteStatus("Exporting full table...");
                var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
                if (string.IsNullOrWhiteSpace(uniqueId) || !EndpointHelpers.IsDigitsOnly(uniqueId))
                {
                    await SseWriter.WriteErrorAsync(response, "User unique ID is missing.");
                    return;
                }

                var exportResult = await sqlOps.SampleToParquetAsync(
                    partitionKey, rowKey, uniqueId,
                    $"SELECT * FROM [{schema}].[{tableName}]",
                    filePrefix: "fullrun", containerName: blobConfig.Container,
                    unlimitedTimeout: true);

                if (!exportResult.Success)
                {
                    await SseWriter.WriteErrorAsync(response, "Export failed.");
                    return;
                }

                var exportFilenames = exportResult.Filenames;

                if (exportFilenames.Count == 0)
                {
                    await SseWriter.WriteErrorAsync(response, "Export produced no files.");
                    return;
                }

                var exportEvtSummary = $"DP run: exported {exportFilenames.Count} file(s) for {schema}.{tableName}";
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_run", flowId, exportEvtSummary);
                await SseWriter.WriteEventPayloadAsync(response, "dp_run", exportEvtSummary);

                await WriteStatus("Creating file ruleset...");
                var fullRunUuid = Guid.NewGuid().ToString("N");

                var (rulesetSuccess, fileRulesetId, _) = await engineApi.CreateFileRulesetAsync(
                    $"fullrun_ruleset_{fullRunUuid}", dataEngineConfig.ConnectorId);
                if (!rulesetSuccess)
                {
                    await SseWriter.WriteErrorAsync(response, "File ruleset creation failed.");
                    return;
                }

                var (metaBatchSuccess, fileMetadataIds) = await EngineRelayService.CreateFileMetadataBatchAsync(
                    engineApi, response, exportFilenames, fileRulesetId, fileFormatId, statusSteps);
                if (!metaBatchSuccess) return;

                await WriteStatus("Creating masking job...");
                var maskingJobId = await CreateEngineJobAsync(engineApi, response, "masking-jobs", new
                {
                    jobName = $"fullrun_masking_{fullRunUuid}",
                    rulesetId = int.Parse(fileRulesetId),
                    onTheFlyMasking = false
                }, "maskingJobId", "Masking job creation");
                if (maskingJobId == null) return;

                await WriteStatus("Executing masking job...");
                var maskingStatus = await ExecuteAndPollAsync(
                    engineApi, response, maskingJobId, "masking job", maxIterations: 600, statusSteps: statusSteps);
                if (maskingStatus == null) return;

                await WriteStatus("Loading masked data to destination...");
                for (var fi = 0; fi < exportFilenames.Count; fi++)
                {
                    var exportFile = exportFilenames[fi];
                    await WriteStatus($"Loading masked file to destination... ({fi + 1} of {exportFilenames.Count})");

                    var loadResult = await sqlOps.LoadMaskedToTableAsync(
                        partitionKey, destConnectionRowKey, destSchema, tableName,
                        exportFile, createTable: fi == 0, truncate: fi == 0,
                        containerName: blobConfig.Container);

                    if (!loadResult.Success)
                    {
                        await SseWriter.WriteErrorAsync(response, $"Failed to load masked file {exportFile} to destination.");
                        return;
                    }
                }

                var fullRunEvtSummary = $"DP run completed: fileFormatId={fileFormatId}, fileRulesetId={fileRulesetId}, " +
                    $"maskingJobId={maskingJobId} ({maskingStatus}), files={exportFilenames.Count}, " +
                    $"destination=[{destSchema}].{tableName}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_run", flowId, fullRunEvtSummary, stepsDetail);
                await SseWriter.WriteEventPayloadAsync(response, "dp_run", fullRunEvtSummary);

                var completeJson = JsonSerializer.Serialize(new
                {
                    success = true,
                    fileFormatId,
                    fileRulesetId,
                    fileMetadataIds,
                    maskingJobId,
                    maskingStatus,
                    exportFilenames
                });
                await SseWriter.WriteEventAsync(response, "complete", completeJson);
            }
            catch (Exception ex)
            {
                var evtSummary = $"DP run error: {ex.Message}";
                var stepsDetail = JsonSerializer.Serialize(statusSteps);
                _ = clientTableService.AppendEventAsync(partitionKey, "dp_run", flowId, evtSummary, stepsDetail);
                await SseWriter.WriteEventPayloadAsync(response, "dp_run", evtSummary);
                await SseWriter.WriteErrorAsync(response, $"DP run error: {ex.Message}");
            }
        });

        app.MapGet("/api/agents/{path}/column-rules", async (string path, string? fileFormatId,
            SessionManager sessionManager, EngineApiClient engineApi, EngineMetadataService metadataService) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out _, out var notFound))
                return notFound;

            if (string.IsNullOrWhiteSpace(fileFormatId))
                return Results.Ok(new { success = false, message = "fileFormatId is required." });

            try
            {
                await metadataService.EnsureLoadedAsync();
                var rules = await engineApi.FetchColumnRulesAsync(fileFormatId);
                var enriched = engineApi.EnrichColumnRules(rules, metadataService.Algorithms, metadataService.Domains, metadataService.Frameworks);

                return Results.Ok(new
                {
                    success = true,
                    fixedCount = 0,
                    responseList = enriched.Rules.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
                    algorithms = enriched.Algorithms.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
                    domains = enriched.Domains.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
                    frameworks = enriched.Frameworks.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray()
                });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Column rules fetch error: {ex.Message}" });
            }
        });

        app.MapPut("/api/agents/{path}/column-rule/{fileFieldMetadataId}", async (string path, string fileFieldMetadataId, HttpRequest request,
            SessionManager sessionManager, EngineApiClient engineApi) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out _, out var notFound))
                return notFound;

            if (string.IsNullOrWhiteSpace(fileFieldMetadataId))
                return Results.Ok(new { success = false, message = "fileFieldMetadataId is required." });

            var body = await request.ReadBodyAsync();

            using var bodyDoc = JsonDocument.Parse(body);
            var algorithmName = bodyDoc.RootElement.TryGetProperty("algorithmName", out var algEl) ? algEl.GetString() ?? "" : "";
            var domainName = bodyDoc.RootElement.TryGetProperty("domainName", out var domEl) ? domEl.GetString() ?? "" : "";
            var hasIsMasked = bodyDoc.RootElement.TryGetProperty("isMasked", out var imEl) && imEl.ValueKind == JsonValueKind.False;

            object engineBody = hasIsMasked
                ? new { isMasked = false, isProfilerWritable = false }
                : new { algorithmName, domainName, isProfilerWritable = false } as object;

            try
            {
                var responseBody = await engineApi.PutColumnRuleAsync(fileFieldMetadataId, engineBody);
                return Results.Content(responseBody, "application/json");
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Column rule save error: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/engine-metadata", async (string path, SessionManager sessionManager, EngineMetadataService metadataService) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out _, out var notFound))
                return notFound;

            try
            {
                await metadataService.EnsureLoadedAsync();

                return Results.Ok(new
                {
                    success = true,
                    algorithms = metadataService.Algorithms?.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray() ?? Array.Empty<object?>(),
                    domains = metadataService.Domains?.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray() ?? Array.Empty<object?>(),
                    frameworks = metadataService.Frameworks?.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray() ?? Array.Empty<object?>()
                });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"Engine metadata fetch error: {ex.Message}" });
            }
        });

        app.MapGet("/api/allowed-algorithm-types", (string sqlType) =>
        {
            var allowed = EndpointHelpers.GetAllowedAlgorithmTypes(sqlType);
            return Results.Ok(new { success = true, allowedTypes = allowed });
        });
    }

    private static List<string> ParseStringArray(JsonElement root, string propertyName)
    {
        var list = new List<string>();
        if (root.TryGetProperty(propertyName, out var el) && el.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in el.EnumerateArray())
            {
                var val = item.GetString();
                if (!string.IsNullOrEmpty(val))
                    list.Add(val);
            }
        }
        return list;
    }

    internal static async Task<string?> CreateEngineJobAsync(
        EngineApiClient engineApi, HttpResponse response,
        string endpoint, object payload, string idField, string jobLabel)
    {
        using var resp = await EngineRelayService.CallEngineAsync(engineApi, "POST", endpoint, payload);

        if (!(resp.RootElement.TryGetProperty("success", out var successEl) && successEl.GetBoolean()))
        {
            var msg = resp.RootElement.TryGetProperty("message", out var m)
                ? m.GetString() : $"{jobLabel} failed.";
            await SseWriter.WriteErrorAsync(response, msg ?? $"{jobLabel} failed.");
            return null;
        }

        var id = EngineRelayService.ExtractBodyField(resp, idField);
        if (string.IsNullOrEmpty(id))
        {
            await SseWriter.WriteErrorAsync(response, $"{jobLabel} returned no {idField}.");
            return null;
        }

        return id;
    }

    internal static async Task<string?> ExecuteAndPollAsync(
        EngineApiClient engineApi, HttpResponse response,
        string jobId, string statusLabel, int maxIterations = 300,
        List<string>? statusSteps = null, SemaphoreSlim? sseLock = null)
    {
        var execId = await CreateEngineJobAsync(
            engineApi, response, "executions",
            new { jobId = int.Parse(jobId) }, "executionId",
            $"{statusLabel} execution");
        if (execId == null) return null;

        var status = await EngineRelayService.PollExecutionAsync(
            engineApi, execId, response, statusLabel, maxIterations, statusSteps, sseLock);

        if (status is not ("SUCCEEDED" or "WARNING"))
        {
            await SseWriter.WriteErrorAsync(response, $"{statusLabel} did not succeed. Final status: {status}");
            return null;
        }

        return status;
    }

    internal static async Task CopyBlobsAsync(
        BlobContainerClient source, BlobContainerClient dest, IEnumerable<string> filenames)
    {
        foreach (var filename in filenames)
        {
            var sourceBlob = source.GetBlobClient(filename);
            var destBlob = dest.GetBlobClient(filename);
            using var stream = new MemoryStream();
            await sourceBlob.DownloadToAsync(stream);
            stream.Position = 0;
            await destBlob.UploadAsync(stream, overwrite: true);
        }
    }

    internal static async Task<List<string>> CopyBlobsWithRenameAsync(
        BlobContainerClient source, BlobContainerClient dest,
        IEnumerable<string> filenames, string prefix)
    {
        var renamed = new List<string>();
        foreach (var filename in filenames)
        {
            var sourceBlob = source.GetBlobClient(filename);
            var newName = $"{prefix}_{filename}";
            var destBlob = dest.GetBlobClient(newName);
            using var stream = new MemoryStream();
            await sourceBlob.DownloadToAsync(stream);
            stream.Position = 0;
            await destBlob.UploadAsync(stream, overwrite: true);
            renamed.Add(newName);
        }
        return renamed;
    }

    internal static async Task<object?> FetchEnrichedColumnRulesPayload(
        EngineApiClient engineApi, EngineMetadataService metadataService, string fileFormatId)
    {
        await metadataService.EnsureLoadedAsync();
        var rules = await engineApi.FetchColumnRulesAsync(fileFormatId);
        var enriched = engineApi.EnrichColumnRules(rules, metadataService.Algorithms, metadataService.Domains, metadataService.Frameworks);
        return new
        {
            rules = enriched.Rules.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
            algorithms = enriched.Algorithms.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
            domains = enriched.Domains.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
            frameworks = enriched.Frameworks.Select(e => JsonSerializer.Deserialize<object>(e.GetRawText())).ToArray(),
        };
    }

    internal static async Task<int> ApplyMappingRulesAsync(
        EngineApiClient engineApi, EngineMetadataService metadataService,
        string fileFormatId, Dictionary<string, string> sqlTypeByColumn,
        Func<string, Task> writeStatus)
    {
        await metadataService.EnsureLoadedAsync();
        var columnRules = await engineApi.FetchColumnRulesAsync(fileFormatId);
        var enriched = engineApi.EnrichColumnRules(columnRules, metadataService.Algorithms, metadataService.Domains, metadataService.Frameworks);

        var algMaskTypes = new Dictionary<string, string>();
        foreach (var alg in enriched.Algorithms)
        {
            var aName = alg.TryGetProperty("algorithmName", out var anEl) ? anEl.GetString() ?? "" : "";
            var aMaskType = alg.TryGetProperty("maskType", out var mtEl) ? mtEl.GetString() ?? "" : "";
            if (!string.IsNullOrEmpty(aName))
                algMaskTypes[aName] = aMaskType;
        }

        var fixedCount = 0;
        foreach (var rule in enriched.Rules)
        {
            var fieldName = rule.TryGetProperty("fieldName", out var fnEl) ? fnEl.GetString() ?? "" : "";
            var algName = rule.TryGetProperty("algorithmName", out var anEl) ? anEl.GetString() ?? "" : "";
            var metadataId = rule.TryGetProperty("fileFieldMetadataId", out var idEl) ? idEl.ToString() : "";
            var isMasked = !rule.TryGetProperty("isMasked", out var imEl) || imEl.ValueKind != JsonValueKind.False;

            if (string.IsNullOrEmpty(fieldName) || string.IsNullOrEmpty(algName)
                || string.IsNullOrEmpty(metadataId) || !isMasked)
                continue;

            if (!sqlTypeByColumn.TryGetValue(fieldName, out var sqlType))
                continue;

            var allowedTypes = EndpointHelpers.GetAllowedAlgorithmTypes(sqlType);
            if (!algMaskTypes.TryGetValue(algName, out var maskType))
                continue;
            if (allowedTypes.Contains(maskType))
                continue;

            await writeStatus($"Fixing type mismatch: {fieldName} ({sqlType}) — algorithm type {maskType} not allowed...");
            using var fixResp = await EngineRelayService.CallEngineAsync(engineApi, "PUT", $"file-field-metadata/{metadataId}", new
            {
                isMasked = false,
                isProfilerWritable = false
            });
            fixedCount++;
        }

        return fixedCount;
    }

    private class TablePrepResult
    {
        public string RowKey { get; set; } = "";
        public string Schema { get; set; } = "";
        public string TableName { get; set; } = "";
        public List<string> Filenames { get; set; } = new();
        public string FileFormatId { get; set; } = "";
        public List<string> PreviewHeaders { get; set; } = new();
        public List<string> SqlColumnTypes { get; set; } = new();
        public List<string> FileMetadataIds { get; set; } = new();
    }
}
