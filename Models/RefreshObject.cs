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

    /// <summary>
    /// "Full" or "DataOnly". Defaults to "Full" if not specified.
    /// Full = data + calculate per partition (safe, no report errors during refresh).
    /// DataOnly = load data only, calculate once at the end (faster for large tables).
    /// </summary>
    [JsonPropertyName("refreshType")]
    public string? RefreshType { get; set; }

    public bool IsFullRefresh =>
        !string.Equals(RefreshType, "DataOnly", StringComparison.OrdinalIgnoreCase);

    public override string ToString()
    {
        return $"{base.ToString()}: {Table ?? "null"}: {Partition ?? "null"}: {RefreshType ?? "Full"}";
    }
}
