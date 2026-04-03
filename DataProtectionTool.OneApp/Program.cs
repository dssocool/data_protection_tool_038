using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Sockets;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using DataProtectionTool.OneApp.Endpoints;
using DataProtectionTool.OneApp.Models;
using DataProtectionTool.OneApp.Services;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8190);
});

// --- Identity resolution ---
// Default to test mode (OS username). Pass "azure" argument to use Azure Identity.
var useAzureIdentity = args.Any(a => a.Equals("azure", StringComparison.OrdinalIgnoreCase));

string oid, tid, userName;

if (useAzureIdentity)
{
    Console.WriteLine("Authenticating with Azure Identity (DefaultAzureCredential)...");
    var credential = new DefaultAzureCredential();
    var tokenResult = await credential.GetTokenAsync(
        new Azure.Core.TokenRequestContext(new[] { "https://graph.microsoft.com/.default" }));

    var handler = new JwtSecurityTokenHandler();
    var jwt = handler.ReadJwtToken(tokenResult.Token);

    oid = jwt.Claims.FirstOrDefault(c => c.Type == "oid")?.Value
        ?? throw new InvalidOperationException("Token does not contain an 'oid' claim.");
    tid = jwt.Claims.FirstOrDefault(c => c.Type == "tid")?.Value
        ?? throw new InvalidOperationException("Token does not contain a 'tid' claim.");
    userName = jwt.Claims.FirstOrDefault(c => c.Type == "name")?.Value ?? "";

    Console.WriteLine($"Authenticated. oid={oid}, tid={tid}, userName={userName}");
}
else
{
    oid = Environment.UserName;
    tid = GetLocalIpAddress();
    userName = Environment.UserName;
    Console.WriteLine($"[TEST MODE] Using OS user as oid: {oid}");
    Console.WriteLine($"[TEST MODE] Using local IP as tid: {tid}");
}

// --- Session manager ---
var sessionManager = new SessionManager();
builder.Services.AddSingleton(sessionManager);

// --- Azure Table Storage ---
var tableConnectionString = builder.Configuration.GetSection("AzureTableStorage")["ConnectionString"]
    ?? throw new InvalidOperationException("AzureTableStorage:ConnectionString is not configured.");
var tableServiceClient = new TableServiceClient(tableConnectionString);
builder.Services.AddSingleton(tableServiceClient);
builder.Services.AddSingleton(sp => new ClientTableService(
    sp.GetRequiredService<TableServiceClient>(),
    "Users",
    "ControlCenter",
    "DataItem",
    "Events",
    sp.GetRequiredService<ILogger<ClientTableService>>()));

// --- Azure Blob Storage ---
var blobSection = builder.Configuration.GetSection("AzureBlobStorage");
var blobStorageConfig = new BlobStorageConfig
{
    StorageAccount = blobSection["StorageAccount"] ?? "",
    Container = blobSection["Container"] ?? "",
    AccessKey = blobSection["AccessKey"] ?? "",
    PreviewContainer = blobSection["PreviewContainer"] ?? ""
};
builder.Services.AddSingleton(blobStorageConfig);

var blobCredential = new StorageSharedKeyCredential(blobStorageConfig.StorageAccount, blobStorageConfig.AccessKey);
var blobServiceUri = blobStorageConfig.StorageAccount == "devstoreaccount1"
    ? new Uri($"http://127.0.0.1:10000/{blobStorageConfig.StorageAccount}")
    : new Uri($"https://{blobStorageConfig.StorageAccount}.blob.core.windows.net");
var blobServiceClient = new BlobServiceClient(blobServiceUri, blobCredential);
builder.Services.AddSingleton(blobServiceClient);
builder.Services.AddSingleton(blobCredential);

// --- Data Engine ---
var dataEngineConfig = builder.Configuration.GetSection("DataEngine").Get<DataEngineConfig>() ?? new DataEngineConfig();
builder.Services.AddSingleton(dataEngineConfig);

var engineHttpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
builder.Services.AddSingleton(sp => new EngineApiClient(
    engineHttpClient, dataEngineConfig, sp.GetRequiredService<ILogger<EngineApiClient>>()));
builder.Services.AddSingleton<EngineMetadataService>();

// --- SQL Operation Service ---
builder.Services.AddSingleton<SqlOperationService>();

var app = builder.Build();

// --- Storage initialization ---
var isAzuriteMode = blobStorageConfig.StorageAccount == "devstoreaccount1"
    || tableConnectionString.Contains("devstoreaccount1", StringComparison.OrdinalIgnoreCase)
    || tableConnectionString.Contains("UseDevelopmentStorage=true", StringComparison.OrdinalIgnoreCase);

try
{
    var usersTable = tableServiceClient.GetTableClient("Users");
    await usersTable.CreateIfNotExistsAsync();
    var controlCenterTable = tableServiceClient.GetTableClient("ControlCenter");
    await controlCenterTable.CreateIfNotExistsAsync();
    var dataItemTable = tableServiceClient.GetTableClient("DataItem");
    await dataItemTable.CreateIfNotExistsAsync();
    var eventsTable = tableServiceClient.GetTableClient("Events");
    await eventsTable.CreateIfNotExistsAsync();
}
catch (Azure.RequestFailedException ex)
{
    Console.Error.WriteLine("=== Azure Storage initialization failed ===");
    Console.Error.WriteLine($"HTTP Status : {ex.Status}");
    Console.Error.WriteLine($"Error Code  : {ex.ErrorCode}");
    Console.Error.WriteLine($"Message     : {ex.Message}");
    Console.Error.WriteLine($"Stack Trace : {ex.StackTrace}");
    Console.Error.WriteLine($"Full Details: {ex}");
    if (!isAzuriteMode)
    {
        Console.Error.WriteLine("=== Running in Azure Storage mode (not Azurite). Cannot connect to the configured storage account. Exiting. ===");
        Environment.Exit(1);
    }
    throw;
}
catch (Exception ex)
{
    Console.Error.WriteLine("=== Storage initialization failed (unexpected error) ===");
    Console.Error.WriteLine(ex.ToString());
    if (!isAzuriteMode)
    {
        Console.Error.WriteLine("=== Running in Azure Storage mode (not Azurite). Cannot connect to the configured storage account. Exiting. ===");
        Environment.Exit(1);
    }
    throw;
}

if (!isAzuriteMode)
{
    try
    {
        var previewContainer = blobServiceClient.GetBlobContainerClient(blobStorageConfig.PreviewContainer);
        await previewContainer.CreateIfNotExistsAsync();
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine("=== Azure Blob Storage connectivity check failed ===");
        Console.Error.WriteLine(ex.ToString());
        Console.Error.WriteLine("=== Running in Azure Storage mode (not Azurite). Cannot connect to the configured storage account. Exiting. ===");
        Environment.Exit(1);
    }
}

// --- Register user session ---
var clientTableService = app.Services.GetRequiredService<ClientTableService>();
var sessionPath = sessionManager.Register(oid, tid, userName);
var agentId = $"oneapp-{Environment.MachineName}-{Process.GetCurrentProcess().Id}";
await clientTableService.CreateOrUpdateClientAsync(oid, tid, agentId, userName);

var host = GetServerHost(builder.Configuration);
var url = $"http://{host}:8190/agents/{sessionPath}";
Console.WriteLine($"DataProtectionTool OneApp ready. URL: {url}");
TryOpenBrowser(url);

// --- Middleware and routing ---
app.UseStaticFiles();

app.MapGet("/", () => "DataProtectionTool OneApp is running.");

app.MapGet("/agents/{path}", (string path, SessionManager sm, IWebHostEnvironment env) =>
{
    if (!sm.TryGet(path, out _))
        return Results.NotFound("Session not found.");

    if (string.IsNullOrEmpty(env.WebRootPath))
        return Results.NotFound("Frontend not built. No wwwroot directory found. Run 'npm run build' in frontend/.");

    var indexPath = Path.Combine(env.WebRootPath, "index.html");
    if (!File.Exists(indexPath))
        return Results.NotFound("Frontend not built. Run 'npm run build' in frontend/.");

    return Results.Content(File.ReadAllText(indexPath), "text/html");
});

app.MapAgentEndpoints();
app.MapTableEndpoints();
app.MapQueryEndpoints();
app.MapBlobEndpoints();
app.MapEngineEndpoints();
app.MapFlowEndpoints();

await app.RunAsync();

// --- Utility functions ---

static string GetLocalIpAddress()
{
    try
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.Connect("8.8.8.8", 80);
        if (socket.LocalEndPoint is IPEndPoint endPoint)
            return endPoint.Address.ToString();
    }
    catch
    {
    }

    var hostEntry = Dns.GetHostEntry(Dns.GetHostName());
    var ipv4 = hostEntry.AddressList.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork);
    return ipv4?.ToString() ?? "127.0.0.1";
}

static string GetServerHost(IConfiguration configuration)
{
    var configured = configuration["ControlCenter:PublicHost"];
    if (!string.IsNullOrWhiteSpace(configured))
        return configured;

    try
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.Connect("8.8.8.8", 80);
        if (socket.LocalEndPoint is IPEndPoint endPoint)
            return endPoint.Address.ToString();
    }
    catch
    {
    }

    return Dns.GetHostName();
}

static bool HasDisplay()
{
    if (OperatingSystem.IsWindows())
        return true;

    if (OperatingSystem.IsMacOS())
        return Environment.GetEnvironmentVariable("__CFBundleIdentifier") != null
            || !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("TERM_PROGRAM"))
            || File.Exists("/usr/bin/open");

    return !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DISPLAY"))
        || !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WAYLAND_DISPLAY"));
}

static void TryOpenBrowser(string url)
{
    if (string.IsNullOrWhiteSpace(url) || !HasDisplay())
        return;

    try
    {
        if (OperatingSystem.IsWindows())
            Process.Start(new ProcessStartInfo(url) { UseShellExecute = true });
        else if (OperatingSystem.IsMacOS())
            Process.Start("open", url);
        else
            Process.Start("xdg-open", url);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Could not open browser: {ex.Message}");
    }
}
