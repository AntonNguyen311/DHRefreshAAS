using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Represents a single table/partition to refresh
/// </summary>
public class RefreshObject
{
    [JsonPropertyName("table")]
    public string? Table { get; set; }
    
    [JsonPropertyName("partition")]
    public string? Partition { get; set; }

    public override string ToString()
    {
        return $"{base.ToString()}: {Table ?? "null"}: {Partition ?? "null"}";
    }
}
