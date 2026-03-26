using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Enhanced input model with stability and retry configuration
/// </summary>
public class PostData
{
    [JsonPropertyName("database_name")]
    public string? DatabaseName { get; set; }
    
    [JsonPropertyName("refresh_objects")]
    public RefreshObject[]? RefreshObjects { get; set; }

    // Enhanced configuration options
    [JsonPropertyName("maxRetryAttempts")]
    public int? MaxRetryAttempts { get; set; }
    
    [JsonPropertyName("baseDelaySeconds")]
    public int? BaseDelaySeconds { get; set; }
    
    [JsonPropertyName("connectionTimeoutMinutes")]
    public int? ConnectionTimeoutMinutes { get; set; }
    
    [JsonPropertyName("operationTimeoutMinutes")]
    public int? OperationTimeoutMinutes { get; set; }

    public override string ToString()
    {
        return $"{base.ToString()}: {DatabaseName ?? "null"}: {RefreshObjects?.Length ?? 0} objects";
    }
}
