using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;
using System.Data;

namespace DHRefreshAAS;

/// <summary>
/// Queries ADOMD storage table metadata to resolve per-table and per-partition row counts.
/// </summary>
public class RowCountQueryService
{
    private readonly IConnectionService _connectionService;
    private readonly ILogger<RowCountQueryService> _logger;

    public RowCountQueryService(IConnectionService connectionService, ILogger<RowCountQueryService> logger)
    {
        _connectionService = connectionService;
        _logger = logger;
    }

    public virtual async Task<Dictionary<string, long>> QueryTableRowCountsAsync(
        string databaseName,
        List<(RefreshObject refreshObj, RefreshResult result)> refreshedObjects,
        CancellationToken cancellationToken)
    {
        var requestedTables = refreshedObjects
            .Select(x => x.refreshObj.Table?.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var results = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        if (requestedTables.Count == 0)
        {
            return results;
        }

        var queries = new[]
        {
            "SELECT * FROM $System.DISCOVER_STORAGE_TABLES",
            "EVALUATE INFO.STORAGETABLES()"
        };

        try
        {
            var connectionString = _connectionService.GetAdomdConnectionString(databaseName);
            using var connection = new AdomdConnection(connectionString);
            await Task.Run(() => connection.Open(), cancellationToken);

            var querySucceeded = false;
            foreach (var query in queries)
            {
                try
                {
                    using var command = connection.CreateCommand();
                    command.CommandText = query;
                    using var reader = await Task.Run(() => command.ExecuteReader(), cancellationToken);
                    ReadRowCounts(reader, databaseName, requestedTables, results);
                    querySucceeded = true;
                    if (results.Count > 0)
                    {
                        break;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Row-count query failed for database '{DatabaseName}' using query '{Query}'.", databaseName, query);
                }
            }

            if (!querySucceeded)
            {
                _logger.LogWarning(
                    "All row-count queries failed for database '{DatabaseName}'. Continuing without row counts for: {Tables}",
                    databaseName,
                    string.Join(", ", requestedTables.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Unable to query row counts from AAS for database '{DatabaseName}'. Continuing without row counts for: {Tables}",
                databaseName,
                string.Join(", ", requestedTables.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)));
            return results;
        }

        foreach (var tableName in requestedTables.OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
        {
            var count = ResolveRowCount(results, tableName, null);
            if (count.HasValue)
            {
                _logger.LogInformation(
                    "Resolved row count for table '{TableName}' in database '{DatabaseName}': {RowCount:N0}",
                    tableName,
                    databaseName,
                    count.Value);
            }
            else
            {
                _logger.LogInformation(
                    "No row count was resolved for table '{TableName}' in database '{DatabaseName}'",
                    tableName,
                    databaseName);
            }
        }

        return results;
    }

    private void ReadRowCounts(
        IDataReader reader,
        string databaseName,
        HashSet<string> requestedTables,
        Dictionary<string, long> results)
    {
        var ordinals = Enumerable.Range(0, reader.FieldCount)
            .ToDictionary(reader.GetName, i => i, StringComparer.OrdinalIgnoreCase);

        while (reader.Read())
        {
            var currentDatabase = GetStringValue(reader, ordinals, "DATABASE_NAME");
            if (!string.IsNullOrWhiteSpace(currentDatabase) &&
                !string.Equals(currentDatabase, databaseName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var rowCount = GetInt64Value(reader, ordinals, "ROWS_COUNT");
            if (!rowCount.HasValue || rowCount.Value < 0)
            {
                continue;
            }

            var tableName = ResolveRequestedTableName(reader, ordinals, requestedTables);
            if (string.IsNullOrWhiteSpace(tableName))
            {
                continue;
            }

            var partitionName = GetStringValue(reader, ordinals, "PARTITION_NAME");
            SetMaximumRowCount(results, BuildRowCountKey(tableName, partitionName), rowCount.Value);
            SetMaximumRowCount(results, BuildRowCountKey(tableName, null), rowCount.Value);
        }
    }

    private static string? ResolveRequestedTableName(
        IDataRecord record,
        IReadOnlyDictionary<string, int> ordinals,
        HashSet<string> requestedTables)
    {
        var candidates = new[]
        {
            GetStringValue(record, ordinals, "MEASURE_GROUP_NAME"),
            GetStringValue(record, ordinals, "DIMENSION_NAME"),
            GetStringValue(record, ordinals, "TABLE_ID")
        }
        .Where(value => !string.IsNullOrWhiteSpace(value))
        .Select(value => value!.Trim())
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToList();

        foreach (var candidate in candidates)
        {
            if (requestedTables.Contains(candidate))
            {
                return candidate;
            }

            var suffixMatch = requestedTables.FirstOrDefault(t =>
                candidate.EndsWith(t, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrWhiteSpace(suffixMatch))
            {
                return suffixMatch;
            }
        }

        return null;
    }

    public static string BuildRowCountKey(string tableName, string? partitionName)
    {
        var normalizedTable = tableName.Trim();
        var normalizedPartition = string.IsNullOrWhiteSpace(partitionName) ? "" : partitionName.Trim();
        return string.IsNullOrEmpty(normalizedPartition)
            ? normalizedTable
            : $"{normalizedTable}|{normalizedPartition}";
    }

    public static void SetMaximumRowCount(IDictionary<string, long> results, string key, long value)
    {
        if (results.TryGetValue(key, out var existing))
        {
            if (value > existing)
            {
                results[key] = value;
            }
            return;
        }

        results[key] = value;
    }

    public static long? ResolveRowCount(IReadOnlyDictionary<string, long> rowCounts, string tableName, string? partitionName)
    {
        var partitionKey = BuildRowCountKey(tableName, partitionName);
        if (rowCounts.TryGetValue(partitionKey, out var partitionCount))
        {
            return partitionCount;
        }

        var tableKey = BuildRowCountKey(tableName, null);
        if (rowCounts.TryGetValue(tableKey, out var tableCount))
        {
            return tableCount;
        }

        return null;
    }

    private static string? GetStringValue(IDataRecord record, IReadOnlyDictionary<string, int> ordinals, string columnName)
    {
        if (!ordinals.TryGetValue(columnName, out var ordinal) || record.IsDBNull(ordinal))
        {
            return null;
        }

        return record.GetValue(ordinal)?.ToString();
    }

    private static long? GetInt64Value(IDataRecord record, IReadOnlyDictionary<string, int> ordinals, string columnName)
    {
        if (!ordinals.TryGetValue(columnName, out var ordinal) || record.IsDBNull(ordinal))
        {
            return null;
        }

        return record.GetValue(ordinal) switch
        {
            long longValue => longValue,
            int intValue => intValue,
            short shortValue => shortValue,
            byte byteValue => byteValue,
            decimal decimalValue => (long)decimalValue,
            double doubleValue => (long)doubleValue,
            float floatValue => (long)floatValue,
            string stringValue when long.TryParse(stringValue, out var parsed) => parsed,
            _ => null
        };
    }
}
