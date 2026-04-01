using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Threshold-based performance warning for a refreshed table/partition.
/// </summary>
public class PerformanceWarningItem
{
    [JsonPropertyName("databaseName")]
    public string DatabaseName { get; set; } = "";

    [JsonPropertyName("tableName")]
    public string TableName { get; set; } = "";

    [JsonPropertyName("partitionName")]
    public string PartitionName { get; set; } = "";

    [JsonPropertyName("processingTimeSeconds")]
    public double ProcessingTimeSeconds { get; set; }

    /// <summary>warning | critical</summary>
    [JsonPropertyName("severity")]
    public string Severity { get; set; } = "";
}
