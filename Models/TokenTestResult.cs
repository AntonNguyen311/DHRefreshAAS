using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

/// <summary>
/// Result of Azure AD token acquisition testing for Service Principal
/// </summary>
public class TokenTestResult
{
    [JsonPropertyName("isSuccessful")]
    public bool IsSuccessful { get; set; }
    
    [JsonPropertyName("authenticationMode")]
    public string AuthenticationMode { get; set; } = "";
    
    [JsonPropertyName("testTimestamp")]
    public DateTime TestTimestamp { get; set; }
    
    [JsonPropertyName("testDurationMs")]
    public double TestDurationMs { get; set; }
    
    // Service Principal information
    [JsonPropertyName("clientId")]
    public string ClientId { get; set; } = "";
    
    [JsonPropertyName("tenantId")]
    public string TenantId { get; set; } = "";
    
    // Token acquisition details
    [JsonPropertyName("tokenEndpoint")]
    public string TokenEndpoint { get; set; } = "";
    
    [JsonPropertyName("httpStatusCode")]
    public int HttpStatusCode { get; set; }
    
    [JsonPropertyName("responseBody")]
    public string ResponseBody { get; set; } = "";
    
    [JsonPropertyName("tokenAcquired")]
    public bool TokenAcquired { get; set; }
    
    [JsonPropertyName("tokenLength")]
    public int TokenLength { get; set; }
    
    [JsonPropertyName("tokenType")]
    public string TokenType { get; set; } = "";
    
    [JsonPropertyName("tokenExpiresInSeconds")]
    public int TokenExpiresInSeconds { get; set; }
    
    // Error information
    [JsonPropertyName("errorMessage")]
    public string ErrorMessage { get; set; } = "";
    
    [JsonPropertyName("exceptionType")]
    public string ExceptionType { get; set; } = "";
}
