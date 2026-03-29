using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Enhanced activity response with performance metrics
/// </summary>
public class ActivityResponse
{
    [JsonPropertyName("isSuccess")]
    public bool IsSuccess { get; set; }
    
    [JsonPropertyName("message")]
    public string Message { get; set; } = "";
    
    [JsonPropertyName("stackTrace")]
    public string StackTrace { get; set; } = "";
    
    [JsonPropertyName("refreshResults")]
    public List<RefreshResult> RefreshResults { get; set; } = new();
    
    [JsonPropertyName("startTime")]
    public DateTime StartTime { get; set; }
    
    [JsonPropertyName("endTime")]
    public DateTime EndTime { get; set; }
    
    [JsonPropertyName("executionTimeSeconds")]
    public double ExecutionTimeSeconds { get; set; }

    [JsonPropertyName("topSlowTables")]
    public List<RefreshResult>? TopSlowTables { get; set; }

    [JsonPropertyName("executionSettings")]
    public RefreshExecutionSettings? ExecutionSettings { get; set; }

    [JsonPropertyName("lastBatchDiagnostic")]
    public SaveChangesDiagnostic? LastBatchDiagnostic { get; set; }
}
