using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Enhanced detailed results for a single refresh attempt with performance metrics
/// </summary>
public class RefreshResult
{
    [JsonPropertyName("tableName")]
    public string TableName { get; set; } = "";
    
    [JsonPropertyName("partitionName")]
    public string PartitionName { get; set; } = "";
    
    [JsonPropertyName("isSuccess")]
    public bool IsSuccess { get; set; }
    
    [JsonPropertyName("errorMessage")]
    public string ErrorMessage { get; set; } = "";
    
    [JsonPropertyName("stackTrace")]
    public string StackTrace { get; set; } = "";
    
    [JsonPropertyName("attemptCount")]
    public int AttemptCount { get; set; }
    
    [JsonPropertyName("executionTimeSeconds")]
    public double ExecutionTimeSeconds { get; set; }

    [JsonPropertyName("rowCount")]
    public long? RowCount { get; set; }

    [JsonPropertyName("refreshedTime")]
    public DateTime? RefreshedTime { get; set; }
}
