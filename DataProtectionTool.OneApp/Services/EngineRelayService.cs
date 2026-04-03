using System.Text.Json;
using DataProtectionTool.OneApp.Models;

namespace DataProtectionTool.OneApp.Services;

public static class EngineRelayService
{
    public static async Task<JsonDocument> CallEngineAsync(
        EngineApiClient engineApi, string method, string relativeUrl, object? requestBody = null)
    {
        var url = $"{engineApi.BaseUrl}/{relativeUrl}";

        using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
        var requestMessage = new HttpRequestMessage(new HttpMethod(method), url);
        requestMessage.Headers.TryAddWithoutValidation("accept", "application/json");
        requestMessage.Headers.TryAddWithoutValidation("Authorization", engineApi.AuthorizationToken);
        requestMessage.Headers.TryAddWithoutValidation("Content-Type", "application/json");

        if (requestBody != null)
        {
            var bodyJson = JsonSerializer.Serialize(requestBody);
            requestMessage.Content = new StringContent(bodyJson, System.Text.Encoding.UTF8, "application/json");
        }

        using var response = await httpClient.SendAsync(requestMessage);
        var responseBody = await response.Content.ReadAsStringAsync();
        var statusCode = (int)response.StatusCode;

        var headers = new Dictionary<string, string>();
        foreach (var h in response.Headers)
            headers[h.Key] = string.Join(", ", h.Value);
        if (response.Content != null)
            foreach (var h in response.Content.Headers)
                headers[h.Key] = string.Join(", ", h.Value);

        var resultObj = new
        {
            success = response.IsSuccessStatusCode,
            statusCode,
            headers,
            body = responseBody
        };

        var resultJson = JsonSerializer.Serialize(resultObj);
        return JsonDocument.Parse(resultJson);
    }

    public static string ExtractBodyField(JsonDocument relayResponse, string fieldName)
    {
        if (!relayResponse.RootElement.TryGetProperty("body", out var bodyEl))
            return "";
        using var bodyDoc = JsonDocument.Parse(bodyEl.GetString() ?? "{}");
        return bodyDoc.RootElement.TryGetProperty(fieldName, out var valEl) ? valEl.ToString() : "";
    }

    public static async Task<string> PollExecutionAsync(
        EngineApiClient engineApi, string executionId,
        HttpResponse response, string statusLabel, int maxIterations = 300,
        List<string>? statusSteps = null, SemaphoreSlim? sseLock = null)
    {
        var status = "";
        for (var i = 0; i < maxIterations; i++)
        {
            await Task.Delay(2000);

            using var statusResp = await CallEngineAsync(engineApi, "GET", $"executions/{executionId}");
            if (!(statusResp.RootElement.TryGetProperty("success", out var sSuccessEl) && sSuccessEl.GetBoolean()))
                continue;

            status = ExtractBodyField(statusResp, "status");
            if (!string.IsNullOrEmpty(statusLabel))
            {
                var msg = $"Polling {statusLabel}: {status}...";
                if (sseLock != null)
                {
                    await sseLock.WaitAsync();
                    try
                    {
                        await SseWriter.WriteEventAsync(response, "status", msg);
                        statusSteps?.Add(msg);
                    }
                    finally
                    {
                        sseLock.Release();
                    }
                }
                else
                {
                    await SseWriter.WriteEventAsync(response, "status", msg);
                    statusSteps?.Add(msg);
                }
            }
            if (status is "SUCCEEDED" or "WARNING" or "FAILED" or "CANCELLED")
                break;
        }

        return status;
    }

    public static async Task<bool> ValidateEngineConfigAsync(
        DataEngineConfig config, HttpResponse response, bool requireProfileSetId = false)
    {
        if (string.IsNullOrEmpty(config.EngineUrl) || string.IsNullOrEmpty(config.AuthorizationToken))
        {
            await SseWriter.WriteErrorAsync(response, "Data engine is not configured. Set EngineUrl and AuthorizationToken in appsettings.json.");
            return false;
        }

        if (string.IsNullOrEmpty(config.ConnectorId))
        {
            await SseWriter.WriteErrorAsync(response, "Data engine ConnectorId is not configured. Set ConnectorId in appsettings.json.");
            return false;
        }

        if (requireProfileSetId && string.IsNullOrEmpty(config.ProfileSetId))
        {
            await SseWriter.WriteErrorAsync(response, "Data engine ProfileSetId is not configured. Set ProfileSetId in appsettings.json.");
            return false;
        }

        return true;
    }

    public static async Task<(bool success, List<string> metadataIds)> CreateFileMetadataBatchAsync(
        EngineApiClient engineApi, HttpResponse response,
        List<string> filenames, string fileRulesetId, string fileFormatId,
        List<string>? statusSteps = null)
    {
        var fileMetadataIds = new List<string>();
        for (var fi = 0; fi < filenames.Count; fi++)
        {
            var file = filenames[fi];
            var msg = $"Creating file metadata... ({fi + 1} of {filenames.Count})";
            await SseWriter.WriteEventAsync(response, "status", msg);
            statusSteps?.Add(msg);

            var (metaSuccess, fileMetadataId, _) = await engineApi.CreateFileMetadataAsync(file, fileRulesetId, fileFormatId);
            if (!metaSuccess)
            {
                await SseWriter.WriteErrorAsync(response, $"File metadata creation failed for {file}.");
                return (false, fileMetadataIds);
            }

            fileMetadataIds.Add(fileMetadataId);
        }

        return (true, fileMetadataIds);
    }

    public static async Task WriteStatusThreadSafeAsync(
        HttpResponse response, SemaphoreSlim sseLock, List<string> statusSteps, string msg)
    {
        await sseLock.WaitAsync();
        try
        {
            await SseWriter.WriteEventAsync(response, "status", msg);
            statusSteps.Add(msg);
        }
        finally
        {
            sseLock.Release();
        }
    }

}
