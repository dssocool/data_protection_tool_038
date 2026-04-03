using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using DataProtectionTool.OneApp.Services;

namespace DataProtectionTool.OneApp.Helpers;

internal static class EndpointHelpers
{
    public static bool TryGetSession(
        SessionManager sessionManager, string path,
        out SessionInfo info, out IResult errorResult)
    {
        if (sessionManager.TryGet(path, out var sessionInfo) && sessionInfo is not null)
        {
            info = sessionInfo;
            errorResult = null!;
            return true;
        }

        info = null!;
        errorResult = Results.NotFound(new { error = "Agent not found." });
        return false;
    }

    public static async Task<SessionInfo?> RequireSessionAsync(
        SessionManager sessionManager, string path, HttpResponse response)
    {
        if (sessionManager.TryGet(path, out var info) && info is not null)
            return info;

        response.StatusCode = 404;
        await response.WriteAsJsonAsync(new { error = "Agent not found." });
        return null;
    }

    private static readonly Regex PreviewFilenameRegex = new(
        "^(?:dryrun_[0-9a-fA-F]{32}_)?(?:preview|fullrun)_(\\d+)_([0-9a-fA-F]{32})(?:_([2-9]\\d*))?\\.parquet$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static async Task<string> ReadBodyAsync(this HttpRequest request)
    {
        using var reader = new StreamReader(request.Body);
        return await reader.ReadToEndAsync();
    }

    public static object EventPayload(string type, string summary, string detail = "")
        => new { timestamp = DateTime.UtcNow.ToString("O"), type, summary, detail };

    public static IResult EventResult(bool success, string message, string eventType, string summary, string detail = "")
        => Results.Ok(new { success, message, @event = EventPayload(eventType, summary, detail) });

    public static IResult EventResultWithRowKey(bool success, string message, string rowKey, string eventType, string summary, string detail = "")
        => Results.Ok(new { success, message, rowKey, @event = EventPayload(eventType, summary, detail) });

    public static string InjectEventIntoResult(string agentResult, string eventType, string summary, string detail = "")
    {
        var evtJson = JsonSerializer.Serialize(EventPayload(eventType, summary, detail));
        return agentResult.TrimEnd().TrimEnd('}') + $",\"event\":{evtJson}}}";
    }

    public static bool IsDigitsOnly(string value) => value.All(char.IsDigit);

    public static bool IsValidPreviewFilename(string filename) => PreviewFilenameRegex.IsMatch(filename);

    public static string AddFieldsToPayload(string body, object fields)
    {
        JsonNode? payloadNode;
        try
        {
            payloadNode = JsonNode.Parse(body);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Invalid request payload: {ex.Message}", ex);
        }

        if (payloadNode is not JsonObject payloadObject)
            throw new InvalidOperationException("Invalid request payload.");

        var fieldsJson = JsonSerializer.SerializeToNode(fields);
        if (fieldsJson is JsonObject fieldsObject)
        {
            foreach (var prop in fieldsObject)
            {
                payloadObject[prop.Key] = prop.Value?.DeepClone();
            }
        }

        return payloadObject.ToJsonString();
    }

    public static ParsedConnection ParseConnectionFromJson(JsonElement root) => new(
        root.TryGetProperty("serverName", out var sn) ? sn.GetString() ?? "" : "",
        root.TryGetProperty("authentication", out var au) ? au.GetString() ?? "" : "",
        root.TryGetProperty("userName", out var un) ? un.GetString() ?? "" : "",
        root.TryGetProperty("password", out var pw) ? pw.GetString() ?? "" : "",
        root.TryGetProperty("databaseName", out var db) ? db.GetString() ?? "" : "",
        root.TryGetProperty("encrypt", out var en) ? en.GetString() ?? "" : "",
        root.TryGetProperty("trustServerCertificate", out var tsc) && tsc.GetBoolean());

    public record ParsedConnection(
        string ServerName, string Authentication, string UserName, string Password,
        string DatabaseName, string Encrypt, bool TrustServerCertificate);

    public static List<string> GetAllowedAlgorithmTypes(string sqlServerType)
    {
        var numericSqlTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "int", "bigint", "smallint", "tinyint", "float", "real",
            "decimal", "numeric", "money", "smallmoney", "bit"
        };

        if (numericSqlTypes.Contains(sqlServerType))
            return new List<string> { "BIG_DECIMAL" };

        return new List<string>
        {
            "BIG_DECIMAL", "LOCAL_DATE_TIME", "STRING", "BYTE_BUFFER", "GENERIC_DATA_ROW"
        };
    }
}
