using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Operation tracking for monitoring async operations with real-time progress
/// </summary>
public class OperationStatus
{
    [JsonPropertyName("operationId")]
    public string OperationId { get; set; } = "";
    
    [JsonPropertyName("status")]
    public string Status { get; set; } = ""; // "queued", "running", "completed", "failed"

    [JsonPropertyName("enqueuedTime")]
    public DateTime EnqueuedTime { get; set; }
    
    [JsonPropertyName("startTime")]
    public DateTime StartTime { get; set; }
    
    [JsonPropertyName("endTime")]
    public DateTime? EndTime { get; set; }
    
    [JsonPropertyName("result")]
    public string? Result { get; set; }
    
    [JsonPropertyName("errorMessage")]
    public string? ErrorMessage { get; set; }
    
    [JsonPropertyName("tablesCount")]
    public int TablesCount { get; set; }
    
    [JsonPropertyName("estimatedDurationMinutes")]
    public double EstimatedDurationMinutes { get; set; }
    
    [JsonPropertyName("tablesCompleted")]
    public int TablesCompleted { get; set; } = 0;
    
    [JsonPropertyName("tablesFailed")]
    public int TablesFailed { get; set; } = 0;
    
    [JsonPropertyName("tablesInProgress")]
    public int TablesInProgress { get; set; } = 0;
    
    [JsonPropertyName("progressPercentage")]
    public double ProgressPercentage { get; set; } = 0.0;
    
    [JsonPropertyName("completedTables")]
    public List<string> CompletedTables { get; set; } = new();
    
    [JsonPropertyName("failedTables")]
    public List<string> FailedTables { get; set; } = new();
    
    [JsonPropertyName("inProgressTables")]
    public List<string> InProgressTables { get; set; } = new();
    
    [JsonPropertyName("currentPhase")]
    public string CurrentPhase { get; set; } = "Initializing";

    [JsonPropertyName("queueScope")]
    public string QueueScope { get; set; } = "";

    [JsonPropertyName("leaseAcquiredTime")]
    public DateTime? LeaseAcquiredTime { get; set; }

    [JsonPropertyName("leaseHeartbeatTime")]
    public DateTime? LeaseHeartbeatTime { get; set; }

    [JsonPropertyName("lastBatchIndex")]
    public int? LastBatchIndex { get; set; }

    [JsonPropertyName("lastBatchTables")]
    public List<string> LastBatchTables { get; set; } = new();

    [JsonPropertyName("lastBatchError")]
    public string? LastBatchError { get; set; }

    [JsonPropertyName("lastBatchFailureCategory")]
    public string? LastBatchFailureCategory { get; set; }

    [JsonPropertyName("lastBatchFailureSource")]
    public string? LastBatchFailureSource { get; set; }

    [JsonPropertyName("requestedByUserId")]
    public string? RequestedByUserId { get; set; }

    [JsonPropertyName("requestedByDisplayName")]
    public string? RequestedByDisplayName { get; set; }

    [JsonPropertyName("requestedByEmail")]
    public string? RequestedByEmail { get; set; }

    [JsonPropertyName("requestSource")]
    public string? RequestSource { get; set; }
    
    /// <summary>
    /// Seeds in-progress table names for <c>ProgressTrackingService.InitializeProgress</c>; omitted from JSON via <see cref="JsonIgnoreAttribute"/>.
    /// </summary>
    [JsonIgnore]
    public RefreshObject[]? RefreshObjects { get; set; }

    [JsonIgnore]
    public string? RequestPayloadJson { get; set; }

    [JsonIgnore]
    public string? LeaseOwner { get; set; }
}
