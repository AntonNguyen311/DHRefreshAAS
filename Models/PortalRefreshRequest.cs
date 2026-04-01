using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

public sealed class PortalRefreshRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("refreshObjects")]
    public List<PortalRefreshObject>? RefreshObjects { get; set; }

    [JsonPropertyName("operationTimeoutMinutes")]
    public int? OperationTimeoutMinutes { get; set; }

    [JsonPropertyName("connectionTimeoutMinutes")]
    public int? ConnectionTimeoutMinutes { get; set; }

    [JsonPropertyName("maxRetryAttempts")]
    public int? MaxRetryAttempts { get; set; }

    [JsonPropertyName("baseDelaySeconds")]
    public int? BaseDelaySeconds { get; set; }

    public PostData ToPostData()
    {
        return new PostData
        {
            DatabaseName = DatabaseName,
            OperationTimeoutMinutes = OperationTimeoutMinutes,
            ConnectionTimeoutMinutes = ConnectionTimeoutMinutes,
            MaxRetryAttempts = MaxRetryAttempts,
            BaseDelaySeconds = BaseDelaySeconds,
            RefreshObjects = RefreshObjects?
                .Select(x => new RefreshObject
                {
                    Table = x.Table,
                    Partition = x.Partition,
                    RefreshType = x.RefreshType
                })
                .ToArray()
        };
    }
}

public sealed class PortalRefreshObject
{
    [JsonPropertyName("table")]
    public string? Table { get; set; }

    [JsonPropertyName("partition")]
    public string? Partition { get; set; }

    [JsonPropertyName("refreshType")]
    public string? RefreshType { get; set; }
}
