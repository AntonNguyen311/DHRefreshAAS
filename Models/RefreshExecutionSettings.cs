using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Effective refresh settings captured for diagnostics.
/// </summary>
public class RefreshExecutionSettings
{
    [JsonPropertyName("operationTimeoutMinutes")]
    public int OperationTimeoutMinutes { get; set; }

    [JsonPropertyName("saveChangesTimeoutMinutes")]
    public int SaveChangesTimeoutMinutes { get; set; }

    [JsonPropertyName("saveChangesBatchSize")]
    public int SaveChangesBatchSize { get; set; }

    [JsonPropertyName("saveChangesMaxParallelism")]
    public int SaveChangesMaxParallelism { get; set; }

    [JsonPropertyName("maxRetryAttempts")]
    public int MaxRetryAttempts { get; set; }
}
