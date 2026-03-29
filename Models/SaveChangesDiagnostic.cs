using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Structured diagnostics for a SaveChanges batch execution.
/// </summary>
public class SaveChangesDiagnostic
{
    [JsonPropertyName("batchIndex")]
    public int BatchIndex { get; set; }

    [JsonPropertyName("totalBatches")]
    public int TotalBatches { get; set; }

    [JsonPropertyName("tables")]
    public List<string> Tables { get; set; } = new();

    [JsonPropertyName("saveChangesTimeoutMinutes")]
    public int SaveChangesTimeoutMinutes { get; set; }

    [JsonPropertyName("maxParallelism")]
    public int MaxParallelism { get; set; }

    [JsonPropertyName("isSuccess")]
    public bool IsSuccess { get; set; }

    [JsonPropertyName("errorMessage")]
    public string? ErrorMessage { get; set; }

    [JsonPropertyName("exceptionType")]
    public string? ExceptionType { get; set; }

    [JsonPropertyName("failureCategory")]
    public string? FailureCategory { get; set; }

    [JsonPropertyName("failureSource")]
    public string? FailureSource { get; set; }

    [JsonPropertyName("matchedSignals")]
    public List<string> MatchedSignals { get; set; } = new();
}
