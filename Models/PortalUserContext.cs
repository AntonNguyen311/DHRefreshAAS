using System.Text.Json.Serialization;

namespace DHRefreshAAS.Models;

public sealed class PortalUserContext
{
    [JsonPropertyName("userId")]
    public string UserId { get; set; } = "";

    [JsonPropertyName("displayName")]
    public string DisplayName { get; set; } = "";

    [JsonPropertyName("email")]
    public string Email { get; set; } = "";

    [JsonPropertyName("authenticationType")]
    public string AuthenticationType { get; set; } = "";

    [JsonPropertyName("roles")]
    public List<string> Roles { get; set; } = new();

    [JsonPropertyName("groupIds")]
    public List<string> GroupIds { get; set; } = new();

    [JsonPropertyName("isAdmin")]
    public bool IsAdmin { get; set; }
}
