using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using Microsoft.Data.SqlClient;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using DataProtectionTool.OneApp.Models;

namespace DataProtectionTool.OneApp.Services;

public class SqlOperationService
{
    private readonly ClientTableService _clientTableService;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly BlobStorageConfig _blobStorageConfig;
    private readonly ILogger<SqlOperationService> _logger;

    private const int PreviewBatchSize = 10_000;

    public SqlOperationService(
        ClientTableService clientTableService,
        BlobServiceClient blobServiceClient,
        BlobStorageConfig blobStorageConfig,
        ILogger<SqlOperationService> logger)
    {
        _clientTableService = clientTableService;
        _blobServiceClient = blobServiceClient;
        _blobStorageConfig = blobStorageConfig;
        _logger = logger;
    }

    public async Task<ValidateSqlResult> ValidateSqlAsync(ConnectionEntity connEntity)
    {
        var details = ConnectionDetailsFromEntity(connEntity);
        return await ValidateSqlCoreAsync(details);
    }

    public async Task<ValidateSqlResult> ValidateSqlAsync(
        string serverName, string authentication, string userName, string password,
        string databaseName, string encrypt, bool trustServerCertificate)
    {
        var details = new ConnectionDetails
        {
            ServerName = serverName,
            Authentication = authentication,
            UserName = userName,
            Password = password,
            DatabaseName = databaseName,
            Encrypt = encrypt,
            TrustServerCertificate = trustServerCertificate
        };
        return await ValidateSqlCoreAsync(details);
    }

    private async Task<ValidateSqlResult> ValidateSqlCoreAsync(ConnectionDetails details)
    {
        if (string.IsNullOrEmpty(details.Encrypt)) details.Encrypt = "Mandatory";
        if (string.IsNullOrEmpty(details.Authentication)) details.Authentication = "Microsoft Entra Integrated";

        _logger.LogInformation("Testing SQL connection to {Server}...", details.ServerName);

        await using var conn = BuildSqlConnection(details);
        await conn.OpenAsync();

        _logger.LogInformation("SQL connection test succeeded.");
        return new ValidateSqlResult(true, $"Connection successful. Server version: {conn.ServerVersion}");
    }

    public async Task<ExecuteSqlResult> ExecuteSqlAsync(
        string partitionKey, string rowKey, string sqlStatement,
        Dictionary<string, string>? sqlParams = null)
    {
        var details = await GetConnectionDetailsAsync(partitionKey, rowKey);

        _logger.LogInformation("Executing SQL on connection {RowKey}...", rowKey);

        await using var conn = BuildSqlConnection(details);
        await conn.OpenAsync();

        var result = new List<Dictionary<string, object?>>();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sqlStatement;

        if (sqlParams != null)
        {
            foreach (var param in sqlParams)
                cmd.Parameters.AddWithValue(param.Key, param.Value);
        }

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            result.Add(row);
        }

        _logger.LogInformation("Executed SQL, returned {Count} rows for connection {RowKey}.", result.Count, rowKey);
        return new ExecuteSqlResult(true, result);
    }

    public async Task<SqlToParquetResult> SampleToParquetAsync(
        string partitionKey, string rowKey, string uniqueId, string sqlStatement,
        string filePrefix = "preview", string? containerName = null,
        bool unlimitedTimeout = false)
    {
        var details = await GetConnectionDetailsAsync(partitionKey, rowKey);

        _logger.LogInformation("SQL->Parquet for connection {RowKey} (timeout={Unlimited})...", rowKey, unlimitedTimeout);

        await using var conn = BuildSqlConnection(details);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sqlStatement;
        if (unlimitedTimeout) cmd.CommandTimeout = 0;

        await using var reader = await cmd.ExecuteReaderAsync();
        var filenames = await StreamReaderToParquetBlobs(reader, uniqueId, filePrefix, containerName);

        _logger.LogInformation("Uploaded {Count} Parquet file(s) for connection {RowKey}", filenames.Count, rowKey);
        return new SqlToParquetResult(true, filenames);
    }

    public async Task<ValidateQueryResult> ValidateQueryAsync(
        string partitionKey, string connectionRowKey,
        string queryText, string sqlStatementBefore, string sqlStatementAfter)
    {
        var details = await GetConnectionDetailsAsync(partitionKey, connectionRowKey);

        _logger.LogInformation("Validating query for connection {RowKey}...", connectionRowKey);

        await using var conn = BuildSqlConnection(details);
        await conn.OpenAsync();

        await using var cmdBefore = conn.CreateCommand();
        cmdBefore.CommandText = sqlStatementBefore;
        await cmdBefore.ExecuteNonQueryAsync();

        try
        {
            await using var cmdQuery = conn.CreateCommand();
            cmdQuery.CommandText = queryText;
            await cmdQuery.ExecuteNonQueryAsync();
        }
        finally
        {
            await using var cmdAfter = conn.CreateCommand();
            cmdAfter.CommandText = sqlStatementAfter;
            await cmdAfter.ExecuteNonQueryAsync();
        }

        _logger.LogInformation("Query validation succeeded.");
        return new ValidateQueryResult(true, "Query syntax is valid.");
    }

    public async Task<LoadMaskedResult> LoadMaskedToTableAsync(
        string partitionKey, string destRowKey, string destSchema, string tableName,
        string blobFilename, bool createTable, bool truncate, string? containerName = null)
    {
        var details = await GetConnectionDetailsAsync(partitionKey, destRowKey);

        _logger.LogInformation("Loading masked file {Blob} -> [{Schema}].[{Table}] (create={Create}, truncate={Truncate})",
            blobFilename, destSchema, tableName, createTable, truncate);

        var container = containerName ?? _blobStorageConfig.PreviewContainer;
        var containerClient = _blobServiceClient.GetBlobContainerClient(container);
        var blobClient = containerClient.GetBlobClient(blobFilename);

        var tempFile = Path.GetTempFileName();
        try
        {
            await blobClient.DownloadToAsync(tempFile);

            using var parquetReader = await ParquetReader.CreateAsync(tempFile);

            var parquetFields = parquetReader.Schema.GetDataFields();
            var columnNames = parquetFields.Select(f => f.Name).ToArray();

            string[] sqlTypes;
            if (parquetReader.CustomMetadata != null &&
                parquetReader.CustomMetadata.TryGetValue("sql_types", out var sqlTypesJson))
            {
                sqlTypes = JsonSerializer.Deserialize(sqlTypesJson, SqlJsonContext.Default.StringArray)
                    ?? columnNames.Select(_ => "nvarchar(max)").ToArray();
            }
            else
            {
                sqlTypes = columnNames.Select(_ => "nvarchar(max)").ToArray();
            }

            await using var conn = BuildSqlConnection(details);
            await conn.OpenAsync();

            if (createTable)
            {
                var columnDefs = new System.Text.StringBuilder();
                for (int i = 0; i < columnNames.Length; i++)
                {
                    if (i > 0) columnDefs.Append(", ");
                    columnDefs.Append($"[{columnNames[i]}] {sqlTypes[i]} NULL");
                }

                var createSql = $@"IF OBJECT_ID('[{destSchema}].[{tableName}]', 'U') IS NULL
                        CREATE TABLE [{destSchema}].[{tableName}] ({columnDefs})";
                await using var createCmd = conn.CreateCommand();
                createCmd.CommandText = createSql;
                await createCmd.ExecuteNonQueryAsync();
                _logger.LogInformation("Ensured table [{Schema}].[{Table}] exists.", destSchema, tableName);
            }

            if (truncate)
            {
                try
                {
                    await using var truncCmd = conn.CreateCommand();
                    truncCmd.CommandText = $"TRUNCATE TABLE [{destSchema}].[{tableName}]";
                    await truncCmd.ExecuteNonQueryAsync();
                }
                catch (SqlException)
                {
                    await using var delCmd = conn.CreateCommand();
                    delCmd.CommandText = $"DELETE FROM [{destSchema}].[{tableName}]";
                    await delCmd.ExecuteNonQueryAsync();
                }
                _logger.LogInformation("Truncated [{Schema}].[{Table}].", destSchema, tableName);
            }

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = $"[{destSchema}].[{tableName}]",
                BulkCopyTimeout = 0
            };

            for (int i = 0; i < columnNames.Length; i++)
                bulkCopy.ColumnMappings.Add(columnNames[i], columnNames[i]);

            for (int g = 0; g < parquetReader.RowGroupCount; g++)
            {
                using var rowGroupReader = parquetReader.OpenRowGroupReader(g);
                var columns = new DataColumn[columnNames.Length];
                for (int i = 0; i < columnNames.Length; i++)
                    columns[i] = await rowGroupReader.ReadColumnAsync(parquetFields[i]);

                var rowCount = columns[0].Data.Length;
                var dataTable = new System.Data.DataTable();
                for (int i = 0; i < columnNames.Length; i++)
                    dataTable.Columns.Add(columnNames[i], typeof(string));

                for (int r = 0; r < rowCount; r++)
                {
                    var row = dataTable.NewRow();
                    for (int c = 0; c < columnNames.Length; c++)
                    {
                        var val = columns[c].Data.GetValue(r);
                        row[c] = val ?? DBNull.Value;
                    }
                    dataTable.Rows.Add(row);
                }

                await bulkCopy.WriteToServerAsync(dataTable);
                _logger.LogInformation("Bulk-copied {Rows} row(s) from row group {Group}/{Total}",
                    rowCount, g + 1, parquetReader.RowGroupCount);
            }
        }
        finally
        {
            try { File.Delete(tempFile); } catch { }
        }

        return new LoadMaskedResult(true);
    }

    private async Task<ConnectionDetails> GetConnectionDetailsAsync(string partitionKey, string rowKey)
    {
        var entity = await _clientTableService.GetConnectionByRowKeyAsync(partitionKey, rowKey);
        if (entity == null)
            throw new InvalidOperationException($"Connection {rowKey} not found.");
        return ConnectionDetailsFromEntity(entity);
    }

    private static ConnectionDetails ConnectionDetailsFromEntity(ConnectionEntity entity) => new()
    {
        RowKey = entity.RowKey,
        ServerName = entity.ServerName,
        Authentication = entity.Authentication,
        UserName = entity.UserName,
        Password = entity.Password,
        DatabaseName = entity.DatabaseName,
        Encrypt = entity.Encrypt,
        TrustServerCertificate = entity.TrustServerCertificate
    };

    internal static SqlConnection BuildSqlConnection(ConnectionDetails details)
    {
        var csb = new SqlConnectionStringBuilder
        {
            DataSource = details.ServerName,
            Encrypt = details.Encrypt == "Mandatory" ? SqlConnectionEncryptOption.Mandatory
                    : details.Encrypt == "Strict" ? SqlConnectionEncryptOption.Strict
                    : SqlConnectionEncryptOption.Optional,
            TrustServerCertificate = details.TrustServerCertificate,
        };

        if (!string.IsNullOrEmpty(details.DatabaseName))
            csb.InitialCatalog = details.DatabaseName;

        if (details.Authentication == "Microsoft Entra Integrated")
        {
            csb.Authentication = SqlAuthenticationMethod.ActiveDirectoryIntegrated;
        }
        else
        {
            csb.UserID = details.UserName;
            csb.Password = details.Password;
        }

        return new SqlConnection(csb.ConnectionString);
    }

    private async Task<List<string>> StreamReaderToParquetBlobs(
        SqlDataReader reader, string uniqueId, string filePrefix = "preview",
        string? containerName = null)
    {
        var columnCount = reader.FieldCount;
        var columnNames = new string[columnCount];
        var sqlTypes = new string[columnCount];
        var dataFields = new DataField[columnCount];

        for (int i = 0; i < columnCount; i++)
        {
            columnNames[i] = reader.GetName(i);
            sqlTypes[i] = reader.GetDataTypeName(i);
            dataFields[i] = new DataField(columnNames[i], typeof(string), isNullable: true);
        }

        var sqlTypesMetadata = new Dictionary<string, string>
        {
            ["sql_types"] = JsonSerializer.Serialize(sqlTypes, SqlJsonContext.Default.StringArray)
        };

        var parquetSchema = new ParquetSchema(dataFields);
        var container = containerName ?? _blobStorageConfig.PreviewContainer;
        var containerClient = _blobServiceClient.GetBlobContainerClient(container);
        var filenames = new List<string>();
        var previewRequestUuid = Guid.NewGuid().ToString("N");
        var fileSequence = 1;
        bool hasMoreRows = true;

        while (hasMoreRows)
        {
            var columnData = new List<object?>[columnCount];
            for (int i = 0; i < columnCount; i++)
                columnData[i] = new List<object?>();

            int rowsInBatch = 0;
            while (rowsInBatch < PreviewBatchSize && (hasMoreRows = await reader.ReadAsync()))
            {
                for (int i = 0; i < columnCount; i++)
                {
                    if (reader.IsDBNull(i))
                        columnData[i].Add(null);
                    else
                        columnData[i].Add(reader.GetValue(i)?.ToString() ?? "");
                }
                rowsInBatch++;
            }

            if (rowsInBatch == 0 && filenames.Count > 0)
                break;

            using var ms = new MemoryStream();
            using (var writer = await ParquetWriter.CreateAsync(parquetSchema, ms))
            {
                writer.CustomMetadata = sqlTypesMetadata;
                using var rowGroup = writer.CreateRowGroup();
                for (int i = 0; i < columnCount; i++)
                {
                    var column = new DataColumn(dataFields[i], columnData[i].Select(v => (string?)v).ToArray());
                    await rowGroup.WriteColumnAsync(column);
                }
            }

            var fname = UploadParquetBlob(ms, containerClient, uniqueId, previewRequestUuid, fileSequence, filePrefix);
            filenames.Add(await fname);
            fileSequence++;

            _logger.LogInformation("Uploaded batch Parquet ({Rows} rows): {File}", rowsInBatch, filenames.Last());
        }

        if (filenames.Count == 0)
        {
            using var ms = new MemoryStream();
            using (var writer = await ParquetWriter.CreateAsync(parquetSchema, ms))
            {
                using var rowGroup = writer.CreateRowGroup();
                for (int i = 0; i < columnCount; i++)
                {
                    var column = new DataColumn(dataFields[i], Array.Empty<string?>());
                    await rowGroup.WriteColumnAsync(column);
                }
            }

            var fname = await UploadParquetBlob(ms, containerClient, uniqueId, previewRequestUuid, fileSequence, filePrefix);
            filenames.Add(fname);

            _logger.LogInformation("Uploaded empty preview Parquet: {File}", fname);
        }

        return filenames;
    }

    private static async Task<string> UploadParquetBlob(
        MemoryStream ms, BlobContainerClient containerClient,
        string uniqueId, string previewUuid, int fileSequence, string filePrefix)
    {
        var filename = fileSequence <= 1
            ? $"{filePrefix}_{uniqueId}_{previewUuid}.parquet"
            : $"{filePrefix}_{uniqueId}_{previewUuid}_{fileSequence}.parquet";

        var blobClient = containerClient.GetBlobClient(filename);
        ms.Position = 0;
        await blobClient.UploadAsync(ms, overwrite: true);
        return filename;
    }

    internal class ConnectionDetails
    {
        public string RowKey { get; set; } = "";
        public string ServerName { get; set; } = "";
        public string Authentication { get; set; } = "";
        public string UserName { get; set; } = "";
        public string Password { get; set; } = "";
        public string DatabaseName { get; set; } = "";
        public string Encrypt { get; set; } = "";
        public bool TrustServerCertificate { get; set; }
    }
}

public record ValidateSqlResult(bool Success, string Message);
public record ExecuteSqlResult(bool Success, List<Dictionary<string, object?>> Rows);
public record SqlToParquetResult(bool Success, List<string> Filenames);
public record ValidateQueryResult(bool Success, string Message);
public record LoadMaskedResult(bool Success);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(string[]))]
internal partial class SqlJsonContext : JsonSerializerContext { }
