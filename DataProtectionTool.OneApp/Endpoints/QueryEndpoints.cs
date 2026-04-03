using System.Text.Json;
using DataProtectionTool.OneApp.Helpers;
using DataProtectionTool.OneApp.Models;
using DataProtectionTool.OneApp.Services;

namespace DataProtectionTool.OneApp.Endpoints;

public static class QueryEndpoints
{
    public static void MapQueryEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/agents/{path}/validate-query", async (string path, HttpRequest request, SessionManager sessionManager, ClientTableService clientTableService, SqlOperationService sqlOps) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;
                var connectionRowKey = root.TryGetProperty("connectionRowKey", out var crk) ? crk.GetString() ?? "" : "";
                var queryText = root.TryGetProperty("queryText", out var qt) ? qt.GetString() ?? "" : "";

                var result = await sqlOps.ValidateQueryAsync(
                    partitionKey, connectionRowKey, queryText,
                    "SET NOEXEC ON", "SET NOEXEC OFF");

                var evtSummary = result.Success ? "Query validation: success" : $"Query validation: failed — {result.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_query", evtSummary, result.Message);

                return EndpointHelpers.EventResult(result.Success, result.Message, "validate_query", evtSummary, result.Message);
            }
            catch (Exception ex)
            {
                var evtSummary = $"Query validation error: {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_query", evtSummary);
                return EndpointHelpers.EventResult(false, $"Query validation error: {ex.Message}", "validate_query", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/save-query", async (string path, HttpRequest request, SessionManager sessionManager, ClientTableService clientTableService) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var connectionRowKey = root.TryGetProperty("connectionRowKey", out var crk) ? crk.GetString() ?? "" : "";
                var queryText = root.TryGetProperty("queryText", out var qt) ? qt.GetString() ?? "" : "";

                var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var entity = await clientTableService.SaveQueryAsync(partitionKey, connectionRowKey, queryText);

                var evtSummary = "Query saved";
                _ = clientTableService.AppendEventAsync(partitionKey, "save_query", evtSummary);

                return EndpointHelpers.EventResultWithRowKey(true, "Query saved.", entity.RowKey, "save_query", evtSummary);
            }
            catch (Exception ex)
            {
                var pk = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var evtSummary = $"Save query failed: {ex.Message}";
                _ = clientTableService.AppendEventAsync(pk, "save_query", evtSummary);
                return EndpointHelpers.EventResult(false, $"Failed to save query: {ex.Message}", "save_query", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/sample-query", async (string path, HttpRequest request, SessionManager sessionManager, ClientTableService clientTableService, SqlOperationService sqlOps) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out _))
            {
                var notFoundEvtSummary = "Sample query failed: session not found";
                return EndpointHelpers.EventResult(false, "Agent not found.", "sample_query", notFoundEvtSummary);
            }

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            try
            {
                var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
                if (string.IsNullOrWhiteSpace(uniqueId) || !EndpointHelpers.IsDigitsOnly(uniqueId))
                {
                    var missingIdEvtSummary = "Sample query failed";
                    var missingIdDetail = "User unique ID is missing.";
                    _ = clientTableService.AppendEventAsync(partitionKey, "sample_query", missingIdEvtSummary, missingIdDetail);
                    return EndpointHelpers.EventResult(false, missingIdDetail, "sample_query", missingIdEvtSummary, missingIdDetail);
                }

                string connectionRowKey = "";
                string queryText = "";
                try
                {
                    using var bodyDoc = JsonDocument.Parse(body);
                    connectionRowKey = bodyDoc.RootElement.TryGetProperty("connectionRowKey", out var crkEl) ? crkEl.GetString() ?? "" : "";
                    queryText = bodyDoc.RootElement.TryGetProperty("queryText", out var qtEl) ? qtEl.GetString() ?? "" : "";
                }
                catch { }

                var sqlStatement = $"SELECT TOP 200 * FROM ({queryText}) AS _q";
                var result = await sqlOps.SampleToParquetAsync(partitionKey, connectionRowKey, uniqueId, sqlStatement);

                var queryEvtSummary = result.Success ? "Sample query completed" : "Sample query failed";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_query", queryEvtSummary);

                return Results.Ok(new
                {
                    success = result.Success,
                    filenames = result.Filenames,
                    @event = EndpointHelpers.EventPayload("sample_query", queryEvtSummary)
                });
            }
            catch (Exception ex)
            {
                var evtSummary = $"Sample query error: {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "sample_query", evtSummary);
                return EndpointHelpers.EventResult(false, $"Sample query error: {ex.Message}", "sample_query", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/list-query-columns", async (string path, HttpRequest request, SessionManager sessionManager, SqlOperationService sqlOps) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            string connectionRowKey;
            string queryText;
            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                connectionRowKey = bodyDoc.RootElement.TryGetProperty("connectionRowKey", out var crkEl) ? crkEl.GetString() ?? "" : "";
                queryText = bodyDoc.RootElement.TryGetProperty("queryText", out var qtEl) ? qtEl.GetString() ?? "" : "";
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid request body." });
            }

            if (string.IsNullOrEmpty(connectionRowKey) || string.IsNullOrEmpty(queryText))
                return Results.BadRequest(new { error = "connectionRowKey and queryText are required." });

            try
            {
                var sqlResult = await sqlOps.ExecuteSqlAsync(partitionKey, connectionRowKey,
                    $"SELECT TOP 0 * FROM ({queryText}) AS _q");

                if (sqlResult.Rows.Count == 0)
                    return Results.Ok(new { success = true, columns = Array.Empty<object>() });

                var columns = sqlResult.Rows[0].Keys.Select(k => new { name = k, type = "unknown" }).ToList();
                return Results.Ok(new { success = true, columns });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"List query columns error: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/queries", async (string path, string connectionRowKey, SessionManager sessionManager, ClientTableService clientTableService) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var queries = await clientTableService.GetQueriesAsync(partitionKey, connectionRowKey);

            var result = queries.Select(q => new
            {
                rowKey = q.RowKey,
                connectionRowKey = q.ConnectionRowKey,
                queryText = q.QueryText,
                createdAt = q.CreatedAt.ToString("O")
            });

            return Results.Ok(result);
        });
    }
}
