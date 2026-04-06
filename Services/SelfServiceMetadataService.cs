using System.Data;
using DHRefreshAAS.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS.Services;

public class SelfServiceMetadataService
{
    private readonly IConfigurationService _config;
    private readonly IConnectionService _connectionService;
    private readonly ILogger<SelfServiceMetadataService> _logger;

    public SelfServiceMetadataService(
        IConfigurationService config,
        IConnectionService connectionService,
        ILogger<SelfServiceMetadataService> logger)
    {
        _config = config;
        _connectionService = connectionService;
        _logger = logger;
    }

    public virtual async Task<IReadOnlyList<SelfServiceModelSummary>> GetAllowedModelsAsync(CancellationToken cancellationToken)
    {
        var allowedModels = await LoadAllowedModelsAsync(cancellationToken);
        return allowedModels
            .Where(x => !string.IsNullOrWhiteSpace(x.DatabaseName))
            .OrderBy(x => x.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public virtual async Task<IReadOnlyList<SelfServiceTableSummary>> GetAllowedTablesAsync(string databaseName, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);

        var allowedTables = await LoadAllowedTablesAsync(databaseName, cancellationToken);
        if (allowedTables.Count == 0)
        {
            return Array.Empty<SelfServiceTableSummary>();
        }

        Server? server = null;
        try
        {
            server = await _connectionService.CreateServerConnectionAsync(cancellationToken, 30, 60, databaseName);
            var database = server.Databases.GetByName(databaseName);
            if (database?.Model == null)
            {
                return Array.Empty<SelfServiceTableSummary>();
            }

            var liveTables = database.Model.Tables
                .Cast<Table>()
                .ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

            return allowedTables
                .Where(x => liveTables.ContainsKey(x.TableName))
                .Select(x =>
                {
                    var liveTable = liveTables[x.TableName];
                    var partitionCount = liveTable.Partitions.Count;
                    var requirePartitionSelection = x.RequirePartition || partitionCount > 1;
                    return new SelfServiceTableSummary
                    {
                        TableName = liveTable.Name,
                        PartitionCount = partitionCount,
                        SupportsTableRefresh = !requirePartitionSelection,
                        DefaultRefreshType = x.DefaultRefreshType,
                        RequirePartitionSelection = requirePartitionSelection,
                        ConfiguredPartitionName = string.IsNullOrWhiteSpace(x.ConfiguredPartitionName) ? null : x.ConfiguredPartitionName,
                        MaxRowsPerRun = x.MaxRowsPerRun
                    };
                })
                .OrderBy(x => x.TableName, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
        finally
        {
            await _connectionService.SafeDisconnectAsync(server);
        }
    }

    public virtual async Task<SelfServicePartitionListResponse?> GetAllowedPartitionsAsync(
        string databaseName,
        string tableName,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var allowedTables = await LoadAllowedTablesAsync(databaseName, cancellationToken);
        var allowedTable = allowedTables.FirstOrDefault(x => x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        if (allowedTable == null)
        {
            return null;
        }

        Server? server = null;
        try
        {
            server = await _connectionService.CreateServerConnectionAsync(cancellationToken, 30, 60, databaseName);
            var database = server.Databases.GetByName(databaseName);
            var table = database?.Model?.Tables.Find(tableName);
            if (table == null)
            {
                return null;
            }

            var requirePartitionSelection = allowedTable.RequirePartition || table.Partitions.Count > 1;
            return new SelfServicePartitionListResponse
            {
                DatabaseName = databaseName,
                TableName = table.Name,
                SupportsTableRefresh = !requirePartitionSelection,
                DefaultRefreshType = allowedTable.DefaultRefreshType,
                Partitions = table.Partitions
                    .Cast<Partition>()
                    .Select(x => new SelfServicePartitionSummary
                    {
                        PartitionName = x.Name,
                        LastRefreshedTime = x.RefreshedTime
                    })
                    .OrderBy(x => x.PartitionName, StringComparer.OrdinalIgnoreCase)
                    .ToList()
            };
        }
        finally
        {
            await _connectionService.SafeDisconnectAsync(server);
        }
    }

    public virtual async Task<SelfServiceRefreshValidationResult> ValidateRefreshRequestAsync(PostData requestData, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(requestData);

        if (string.IsNullOrWhiteSpace(requestData.DatabaseName) || requestData.RefreshObjects == null || requestData.RefreshObjects.Length == 0)
        {
            return new SelfServiceRefreshValidationResult
            {
                IsAllowed = false,
                Message = "Database name and at least one refresh object are required."
            };
        }

        var allowedTables = await LoadAllowedTablesAsync(requestData.DatabaseName, cancellationToken);
        if (allowedTables.Count == 0)
        {
            return new SelfServiceRefreshValidationResult
            {
                IsAllowed = false,
                Message = $"Database '{requestData.DatabaseName}' is not enabled for self-service refresh."
            };
        }

        Server? server = null;
        try
        {
            server = await _connectionService.CreateServerConnectionAsync(cancellationToken, 30, 60, requestData.DatabaseName);
            var database = server.Databases.GetByName(requestData.DatabaseName);
            if (database?.Model == null)
            {
                return new SelfServiceRefreshValidationResult
                {
                    IsAllowed = false,
                    Message = $"Database '{requestData.DatabaseName}' was not found in AAS."
                };
            }

            foreach (var refreshObject in requestData.RefreshObjects)
            {
                if (string.IsNullOrWhiteSpace(refreshObject?.Table))
                {
                    return new SelfServiceRefreshValidationResult
                    {
                        IsAllowed = false,
                        Message = "Each refresh object must specify a table."
                    };
                }

                var allowedTable = allowedTables.FirstOrDefault(x => x.TableName.Equals(refreshObject.Table, StringComparison.OrdinalIgnoreCase));
                if (allowedTable == null)
                {
                    return new SelfServiceRefreshValidationResult
                    {
                        IsAllowed = false,
                        Message = $"Table '{refreshObject.Table}' is not enabled for self-service refresh in model '{requestData.DatabaseName}'."
                    };
                }

                var liveTable = database.Model.Tables.Find(refreshObject.Table);
                if (liveTable == null)
                {
                    return new SelfServiceRefreshValidationResult
                    {
                        IsAllowed = false,
                        Message = $"Table '{refreshObject.Table}' was not found in model '{requestData.DatabaseName}'."
                    };
                }

                var requirePartitionSelection = allowedTable.RequirePartition || liveTable.Partitions.Count > 1;
                if (requirePartitionSelection && string.IsNullOrWhiteSpace(refreshObject.Partition))
                {
                    return new SelfServiceRefreshValidationResult
                    {
                        IsAllowed = false,
                        Message = $"Table '{refreshObject.Table}' requires a partition selection."
                    };
                }

                if (!string.IsNullOrWhiteSpace(refreshObject.Partition) && !liveTable.Partitions.ContainsName(refreshObject.Partition))
                {
                    return new SelfServiceRefreshValidationResult
                    {
                        IsAllowed = false,
                        Message = $"Partition '{refreshObject.Partition}' was not found in table '{refreshObject.Table}'."
                    };
                }
            }

            return new SelfServiceRefreshValidationResult
            {
                IsAllowed = true,
                Message = "Refresh request is allowed."
            };
        }
        finally
        {
            await _connectionService.SafeDisconnectAsync(server);
        }
    }

    private async Task<List<SelfServiceModelSummary>> LoadAllowedModelsAsync(CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                src.CubeName,
                COUNT(*) AS AllowedTableCount
            FROM (
                SELECT DISTINCT
                    CubeName,
                    CubeTableName
                FROM etl.datawarehouseandcubemapping
                WHERE ISNULL(IsDisabled, 0) = 0
                  AND ISNULL(IsPolicyEnabled, 1) = 1
            ) src
            GROUP BY src.CubeName
            ORDER BY src.CubeName;
            """;

        await using var connection = await OpenSqlConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection)
        {
            CommandType = CommandType.Text
        };

        var results = new List<SelfServiceModelSummary>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new SelfServiceModelSummary
            {
                DatabaseName = reader.GetString(0),
                AllowedTableCount = reader.IsDBNull(1) ? 0 : reader.GetInt32(1)
            });
        }

        return results;
    }

    private async Task<List<AllowedTableRow>> LoadAllowedTablesAsync(string databaseName, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                CubeTableName,
                MAX(ISNULL(RefreshType, 'Full')) AS DefaultRefreshType,
                MAX(CASE WHEN NULLIF(LTRIM(RTRIM(ISNULL(Partition, ''))), '') IS NOT NULL THEN 1 ELSE 0 END) AS HasConfiguredPartition,
                MAX(CAST(ISNULL(RequirePartition, 0) AS INT)) AS RequirePartition,
                MAX(NULLIF(LTRIM(RTRIM(ISNULL(Partition, ''))), '')) AS ConfiguredPartitionName,
                MAX(CAST(MaxRowsPerRun AS BIGINT)) AS MaxRowsPerRun
            FROM etl.datawarehouseandcubemapping
            WHERE CubeName = @databaseName
              AND ISNULL(IsDisabled, 0) = 0
              AND ISNULL(IsPolicyEnabled, 1) = 1
            GROUP BY CubeTableName
            ORDER BY CubeTableName;
            """;

        await using var connection = await OpenSqlConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection)
        {
            CommandType = CommandType.Text
        };
        command.Parameters.AddWithValue("@databaseName", databaseName);

        var results = new List<AllowedTableRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new AllowedTableRow
            {
                TableName = reader.GetString(0),
                DefaultRefreshType = reader.IsDBNull(1) ? "Full" : reader.GetString(1),
                HasConfiguredPartition = !reader.IsDBNull(2) && reader.GetInt32(2) == 1,
                RequirePartition = !reader.IsDBNull(3) && reader.GetInt32(3) == 1,
                ConfiguredPartitionName = reader.IsDBNull(4) ? null : reader.GetString(4),
                MaxRowsPerRun = reader.IsDBNull(5) ? null : reader.GetInt64(5)
            });
        }

        return results;
    }

    private async Task<SqlConnection> OpenSqlConnectionAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_config.SelfServiceSqlConnectionString))
        {
            throw new InvalidOperationException(
                $"SELF_SERVICE_SQL_CONNECTION_STRING is required to read self-service metadata from '{_config.SelfServiceSqlDatabaseName}'.");
        }

        var connection = new SqlConnection(_config.SelfServiceSqlConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    private sealed class AllowedTableRow
    {
        public string TableName { get; set; } = "";
        public string DefaultRefreshType { get; set; } = "Full";
        public bool HasConfiguredPartition { get; set; }
        public bool RequirePartition { get; set; }
        public string? ConfiguredPartitionName { get; set; }
        public long? MaxRowsPerRun { get; set; }
    }
}
