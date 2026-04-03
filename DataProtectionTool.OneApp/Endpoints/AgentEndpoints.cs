using System.Text.Json;
using DataProtectionTool.OneApp.Helpers;
using DataProtectionTool.OneApp.Models;
using DataProtectionTool.OneApp.Services;

namespace DataProtectionTool.OneApp.Endpoints;

public static class AgentEndpoints
{
    public static void MapAgentEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/agents/{path}", (string path, SessionManager sessionManager) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            return Results.Ok(new
            {
                oid = info.Oid,
                tid = info.Tid,
                agentId = $"oneapp-{Environment.MachineName}",
                connectedAt = info.ConnectedAt.ToString("O"),
                userName = info.UserName
            });
        });

        app.MapGet("/api/agents/{path}/user-id", async (string path, SessionManager sessionManager, ClientTableService clientTableService) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var uniqueId = await clientTableService.GetUserIdAsync(partitionKey);
            return Results.Ok(new { uniqueId });
        });

        app.MapPost("/api/agents/{path}/validate-sql", async (string path, HttpRequest request, SessionManager sessionManager, ClientTableService clientTableService, SqlOperationService sqlOps) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var conn = EndpointHelpers.ParseConnectionFromJson(root);
                var result = await sqlOps.ValidateSqlAsync(
                    conn.ServerName, conn.Authentication, conn.UserName, conn.Password,
                    conn.DatabaseName, conn.Encrypt, conn.TrustServerCertificate);

                var evtSummary = result.Success ? "SQL validation: success" : $"SQL validation: failed — {result.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql", evtSummary, result.Message);

                return EndpointHelpers.EventResult(result.Success, result.Message, "validate_sql", evtSummary, result.Message);
            }
            catch (Exception ex)
            {
                var evtSummary = $"SQL validation: error — {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "validate_sql", evtSummary);
                return EndpointHelpers.EventResult(false, $"Validation error: {ex.Message}", "validate_sql", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/save-connection", async (string path, HttpRequest request, SessionManager sessionManager, ClientTableService clientTableService) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var conn = EndpointHelpers.ParseConnectionFromJson(root);
                var entity = await clientTableService.SaveConnectionAsync(
                    partitionKey,
                    conn.ServerName, conn.Authentication, conn.UserName, conn.Password,
                    conn.DatabaseName, conn.Encrypt, conn.TrustServerCertificate);

                var serverName = conn.ServerName;
                var evtSummary = $"Connection saved: {serverName}";
                _ = clientTableService.AppendEventAsync(partitionKey, "save_connection", evtSummary);

                return EndpointHelpers.EventResultWithRowKey(true, "Connection saved.", entity.RowKey, "save_connection", evtSummary);
            }
            catch (Exception ex)
            {
                var pk = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
                var evtSummary = $"Save connection failed: {ex.Message}";
                _ = clientTableService.AppendEventAsync(pk, "save_connection", evtSummary);
                return EndpointHelpers.EventResult(false, $"Failed to save: {ex.Message}", "save_connection", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/list-tables", async (string path, HttpRequest request, SessionManager sessionManager, ClientTableService clientTableService, SqlOperationService sqlOps, ILogger<SessionManager> logger) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            string rowKey;
            bool refresh = false;
            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                rowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
                refresh = bodyDoc.RootElement.TryGetProperty("refresh", out var rfEl) && rfEl.GetBoolean();
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid request body." });
            }

            var connEntity = await clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
            if (connEntity == null)
                return Results.NotFound(new { error = "Connection not found." });

            if (!refresh)
            {
                var cached = await clientTableService.GetDataItemsAsync(partitionKey, connEntity.ServerName, connEntity.DatabaseName);
                if (cached.Count > 0)
                {
                    var cachedTables = cached.Select(d => new { schema = d.Schema, name = d.TableName, fileFormatId = d.FileFormatId }).ToList();
                    var evtSummary = $"Listed {cachedTables.Count} tables (cached)";
                    _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", evtSummary);
                    return Results.Ok(new
                    {
                        success = true,
                        tables = cachedTables,
                        @event = EndpointHelpers.EventPayload("list_tables", evtSummary)
                    });
                }
            }

            try
            {
                var sqlResult = await sqlOps.ExecuteSqlAsync(partitionKey, rowKey,
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME");

                var tableList = new List<(string schema, string name)>();
                foreach (var row in sqlResult.Rows)
                {
                    var schema = row.TryGetValue("TABLE_SCHEMA", out var sVal) ? sVal?.ToString() ?? "" : "";
                    var name = row.TryGetValue("TABLE_NAME", out var nVal) ? nVal?.ToString() ?? "" : "";
                    tableList.Add((schema, name));
                }

                var tables = tableList.Select(t => new { schema = t.schema, name = t.name }).ToList();
                var evtSummary = $"Listed {tableList.Count} tables";

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await clientTableService.SaveDataItemsAsync(partitionKey, connEntity.ServerName, connEntity.DatabaseName, rowKey, tableList);
                        await clientTableService.AppendEventAsync(partitionKey, "list_tables", evtSummary);
                    }
                    catch (Exception saveEx)
                    {
                        await clientTableService.AppendEventAsync(partitionKey, "list_tables",
                            $"Failed to load tables from {connEntity.DatabaseName}. Refresh the database to try again.");
                        await clientTableService.AppendEventAsync(partitionKey, "list_tables",
                            saveEx.Message, saveEx.ToString());
                    }
                });

                return Results.Ok(new
                {
                    success = true,
                    tables,
                    @event = EndpointHelpers.EventPayload("list_tables", evtSummary)
                });
            }
            catch (Exception ex)
            {
                var evtSummary = $"List tables error: {ex.Message}";
                _ = clientTableService.AppendEventAsync(partitionKey, "list_tables", evtSummary);
                return EndpointHelpers.EventResult(false, $"List tables error: {ex.Message}", "list_tables", evtSummary);
            }
        });

        app.MapPost("/api/agents/{path}/list-columns", async (string path, HttpRequest request, SessionManager sessionManager, SqlOperationService sqlOps) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var body = await request.ReadBodyAsync();

            string rowKey;
            string schema;
            string tableName;
            try
            {
                using var bodyDoc = JsonDocument.Parse(body);
                rowKey = bodyDoc.RootElement.TryGetProperty("rowKey", out var rkEl) ? rkEl.GetString() ?? "" : "";
                schema = bodyDoc.RootElement.TryGetProperty("schema", out var sEl) ? sEl.GetString() ?? "" : "";
                tableName = bodyDoc.RootElement.TryGetProperty("tableName", out var tnEl) ? tnEl.GetString() ?? "" : "";
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid request body." });
            }

            if (string.IsNullOrEmpty(rowKey) || string.IsNullOrEmpty(schema) || string.IsNullOrEmpty(tableName))
                return Results.BadRequest(new { error = "rowKey, schema and tableName are required." });

            try
            {
                var sqlResult = await sqlOps.ExecuteSqlAsync(partitionKey, rowKey,
                    "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName ORDER BY ORDINAL_POSITION",
                    new Dictionary<string, string> { ["@schema"] = schema, ["@tableName"] = tableName });

                var columns = new List<object>();
                foreach (var row in sqlResult.Rows)
                {
                    var colName = row.TryGetValue("COLUMN_NAME", out var cnVal) ? cnVal?.ToString() ?? "" : "";
                    var colType = row.TryGetValue("DATA_TYPE", out var ctVal) ? ctVal?.ToString() ?? "" : "";
                    columns.Add(new { name = colName, type = colType });
                }
                return Results.Ok(new { success = true, columns });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"List columns error: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/list-schemas", async (string path, string rowKey, SessionManager sessionManager, SqlOperationService sqlOps) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            if (string.IsNullOrEmpty(rowKey))
                return Results.BadRequest(new { error = "rowKey query parameter is required." });

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);

            try
            {
                var sqlResult = await sqlOps.ExecuteSqlAsync(partitionKey, rowKey,
                    "SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA");

                var schemas = new List<string>();
                foreach (var row in sqlResult.Rows)
                {
                    if (row.TryGetValue("TABLE_SCHEMA", out var sVal))
                        schemas.Add(sVal?.ToString() ?? "");
                }
                return Results.Ok(new { success = true, schemas });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"List schemas error: {ex.Message}" });
            }
        });

        app.MapGet("/api/agents/{path}/connections", async (string path, SessionManager sessionManager, ClientTableService clientTableService) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out var info, out var notFound))
                return notFound;

            var partitionKey = ClientEntity.BuildPartitionKey(info.Oid, info.Tid);
            var connections = await clientTableService.GetConnectionsAsync(partitionKey);

            var result = connections.Select(c => new
            {
                rowKey = c.RowKey,
                connectionType = c.ConnectionType,
                serverName = c.ServerName,
                authentication = c.Authentication,
                databaseName = c.DatabaseName,
                encrypt = c.Encrypt,
                trustServerCertificate = c.TrustServerCertificate,
                createdAt = c.CreatedAt.ToString("O")
            });

            return Results.Ok(result);
        });

        app.MapPost("/api/agents/{path}/http-request", async (string path, HttpRequest request, SessionManager sessionManager, EngineApiClient engineApi) =>
        {
            if (!EndpointHelpers.TryGetSession(sessionManager, path, out _, out var notFound))
                return notFound;

            var body = await request.ReadBodyAsync();

            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var method = root.GetProperty("method").GetString() ?? "GET";
                var url = root.GetProperty("url").GetString() ?? "";
                var bodyContent = root.TryGetProperty("body", out var bodyEl) ? bodyEl.GetString() : null;

                using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(100) };
                var requestMessage = new HttpRequestMessage(new HttpMethod(method), url);

                if (root.TryGetProperty("headers", out var headersEl) && headersEl.ValueKind == JsonValueKind.Object)
                {
                    foreach (var header in headersEl.EnumerateObject())
                    {
                        var headerValue = header.Value.GetString() ?? "";
                        if (!requestMessage.Headers.TryAddWithoutValidation(header.Name, headerValue))
                        {
                            requestMessage.Content ??= new StringContent(bodyContent ?? "");
                            requestMessage.Content.Headers.Remove(header.Name);
                            requestMessage.Content.Headers.TryAddWithoutValidation(header.Name, headerValue);
                        }
                    }
                }

                if (bodyContent != null && requestMessage.Content == null)
                    requestMessage.Content = new StringContent(bodyContent, System.Text.Encoding.UTF8);

                using var response = await httpClient.SendAsync(requestMessage);
                var responseBody = await response.Content.ReadAsStringAsync();

                var responseHeaders = new Dictionary<string, string>();
                foreach (var h in response.Headers)
                    responseHeaders[h.Key] = string.Join(", ", h.Value);
                if (response.Content != null)
                    foreach (var h in response.Content.Headers)
                        responseHeaders[h.Key] = string.Join(", ", h.Value);

                return Results.Ok(new
                {
                    success = response.IsSuccessStatusCode,
                    statusCode = (int)response.StatusCode,
                    headers = responseHeaders,
                    body = responseBody
                });
            }
            catch (Exception ex)
            {
                return Results.Ok(new { success = false, message = $"HTTP request error: {ex.Message}" });
            }
        });
    }
}
