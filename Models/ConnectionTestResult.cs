using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Result of connection testing with detailed diagnostic information
/// </summary>
public class ConnectionTestResult
{
    [JsonPropertyName("isSuccessful")]
    public bool IsSuccessful { get; set; }
    
    [JsonPropertyName("serverUrl")]
    public string ServerUrl { get; set; } = "";
    
    [JsonPropertyName("database")]
    public string Database { get; set; } = "";
    
    [JsonPropertyName("authenticationMode")]
    public string AuthenticationMode { get; set; } = "";
    
    [JsonPropertyName("connectionString")]
    public string ConnectionString { get; set; } = "";
    
    [JsonPropertyName("testTimestamp")]
    public DateTime TestTimestamp { get; set; }
    
    [JsonPropertyName("connectionTimeMs")]
    public double ConnectionTimeMs { get; set; }
    
    // Server information (when connection succeeds)
    [JsonPropertyName("serverVersion")]
    public string ServerVersion { get; set; } = "";
    
    [JsonPropertyName("serverEdition")]
    public string ServerEdition { get; set; } = "";
    
    [JsonPropertyName("databaseFound")]
    public bool DatabaseFound { get; set; }
    
    [JsonPropertyName("databaseLastUpdate")]
    public DateTime? DatabaseLastUpdate { get; set; }
    
    [JsonPropertyName("tablesCount")]
    public int TablesCount { get; set; }
    
    // Error information (when connection fails)
    [JsonPropertyName("errorMessage")]
    public string ErrorMessage { get; set; } = "";
    
    [JsonPropertyName("exceptionType")]
    public string ExceptionType { get; set; } = "";
}
