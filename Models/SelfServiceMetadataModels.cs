using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

public sealed class SelfServiceModelSummary
{
    [JsonPropertyName("databaseName")]
    public string DatabaseName { get; set; } = "";

    [JsonPropertyName("allowedTableCount")]
    public int AllowedTableCount { get; set; }
}

public sealed class SelfServiceTableSummary
{
    [JsonPropertyName("tableName")]
    public string TableName { get; set; } = "";

    [JsonPropertyName("partitionCount")]
    public int PartitionCount { get; set; }

    [JsonPropertyName("supportsTableRefresh")]
    public bool SupportsTableRefresh { get; set; }

    [JsonPropertyName("defaultRefreshType")]
    public string DefaultRefreshType { get; set; } = "Full";

    [JsonPropertyName("requirePartitionSelection")]
    public bool RequirePartitionSelection { get; set; }

    [JsonPropertyName("configuredPartitionName")]
    public string? ConfiguredPartitionName { get; set; }

    [JsonPropertyName("maxRowsPerRun")]
    public long? MaxRowsPerRun { get; set; }
}

public sealed class SelfServicePartitionSummary
{
    [JsonPropertyName("partitionName")]
    public string PartitionName { get; set; } = "";

    [JsonPropertyName("lastRefreshedTime")]
    public DateTime? LastRefreshedTime { get; set; }
}

public sealed class SelfServicePartitionListResponse
{
    [JsonPropertyName("databaseName")]
    public string DatabaseName { get; set; } = "";

    [JsonPropertyName("tableName")]
    public string TableName { get; set; } = "";

    [JsonPropertyName("supportsTableRefresh")]
    public bool SupportsTableRefresh { get; set; }

    [JsonPropertyName("defaultRefreshType")]
    public string DefaultRefreshType { get; set; } = "Full";

    [JsonPropertyName("partitions")]
    public List<SelfServicePartitionSummary> Partitions { get; set; } = new();
}

public sealed class SelfServiceRefreshValidationResult
{
    [JsonPropertyName("isAllowed")]
    public bool IsAllowed { get; set; }

    [JsonPropertyName("message")]
    public string Message { get; set; } = "";
}
