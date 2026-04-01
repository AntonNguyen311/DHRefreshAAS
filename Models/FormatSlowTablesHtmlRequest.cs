using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

public class FormatSlowTablesHtmlRequest
{
    [JsonPropertyName("rows")]
    public List<SlowTableEmailRow>? Rows { get; set; }
}
