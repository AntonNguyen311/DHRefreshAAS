using System.Text;
using System.Text.Json;
using DHRefreshAAS.Models;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS.Services;

public class PortalAuthService
{
    private static readonly JsonSerializerOptions PrincipalJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly IConfigurationService _config;
    private readonly ILogger<PortalAuthService> _logger;

    public PortalAuthService(IConfigurationService config, ILogger<PortalAuthService> logger)
    {
        _config = config;
        _logger = logger;
    }

    public virtual PortalUserContext? GetPortalUser(HttpRequestData request)
    {
        if (!TryGetHeaderValue(request, "X-MS-CLIENT-PRINCIPAL", out var encodedPrincipal) ||
            string.IsNullOrWhiteSpace(encodedPrincipal))
        {
            return null;
        }

        try
        {
            var bytes = Convert.FromBase64String(encodedPrincipal);
            var json = Encoding.UTF8.GetString(bytes);
            var principal = JsonSerializer.Deserialize<ClientPrincipalPayload>(json, PrincipalJsonOptions);
            if (principal == null)
            {
                return null;
            }

            var claims = principal.Claims ?? new List<ClientPrincipalClaim>();
            var roles = ExtractClaimValues(claims, "roles", "role", "http://schemas.microsoft.com/ws/2008/06/identity/claims/role");
            var groups = ExtractClaimValues(claims, "groups", "group");
            var user = new PortalUserContext
            {
                UserId = FirstNonEmpty(
                    ExtractClaimValue(claims, "http://schemas.microsoft.com/identity/claims/objectidentifier"),
                    ExtractClaimValue(claims, "oid"),
                    GetHeaderFallback(request, "X-MS-CLIENT-PRINCIPAL-ID")),
                DisplayName = FirstNonEmpty(
                    ExtractClaimValue(claims, "name"),
                    GetHeaderFallback(request, "X-MS-CLIENT-PRINCIPAL-NAME")),
                Email = FirstNonEmpty(
                    ExtractClaimValue(claims, "preferred_username"),
                    ExtractClaimValue(claims, "upn"),
                    ExtractClaimValue(claims, "email"),
                    ExtractClaimValue(claims, "emails"),
                    GetHeaderFallback(request, "X-MS-CLIENT-PRINCIPAL-NAME")),
                AuthenticationType = principal.AuthTyp ?? "",
                Roles = roles,
                GroupIds = groups
            };

            user.IsAdmin = IsAdmin(user);
            return user;
        }
        catch (FormatException ex)
        {
            _logger.LogWarning(ex, "Failed to decode X-MS-CLIENT-PRINCIPAL header.");
            return null;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse X-MS-CLIENT-PRINCIPAL header.");
            return null;
        }
    }

    public virtual bool CanReadMetadata(PortalUserContext user)
    {
        ArgumentNullException.ThrowIfNull(user);
        var roles = _config.PortalMetadataRoles ?? Array.Empty<string>();
        var groups = _config.PortalMetadataGroups ?? Array.Empty<string>();
        return user.IsAdmin || MatchesAny(user, roles, groups) || IsOpenAccess(roles, groups);
    }

    public virtual bool CanSubmitRefresh(PortalUserContext user)
    {
        ArgumentNullException.ThrowIfNull(user);
        var roles = _config.PortalRefreshRoles ?? Array.Empty<string>();
        var groups = _config.PortalRefreshGroups ?? Array.Empty<string>();
        return user.IsAdmin || MatchesAny(user, roles, groups) || IsOpenAccess(roles, groups);
    }

    public virtual bool IsAdmin(PortalUserContext user)
    {
        ArgumentNullException.ThrowIfNull(user);
        var roles = _config.PortalAdminRoles ?? Array.Empty<string>();
        var groups = _config.PortalAdminGroups ?? Array.Empty<string>();
        return MatchesAny(user, roles, groups);
    }

    private static bool TryGetHeaderValue(HttpRequestData request, string name, out string value)
    {
        value = string.Empty;
        if (!request.Headers.TryGetValues(name, out var values))
        {
            return false;
        }

        value = values.FirstOrDefault() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(value);
    }

    private static string GetHeaderFallback(HttpRequestData request, string name)
    {
        return TryGetHeaderValue(request, name, out var value) ? value : string.Empty;
    }

    private static string FirstNonEmpty(params string?[] values)
    {
        return values.FirstOrDefault(x => !string.IsNullOrWhiteSpace(x)) ?? string.Empty;
    }

    private static List<string> ExtractClaimValues(IEnumerable<ClientPrincipalClaim> claims, params string[] claimTypes)
    {
        var allowed = new HashSet<string>(claimTypes, StringComparer.OrdinalIgnoreCase);
        return claims
            .Where(x => !string.IsNullOrWhiteSpace(x.Typ) && allowed.Contains(x.Typ))
            .SelectMany(x => SplitClaimValue(x.Val))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string ExtractClaimValue(IEnumerable<ClientPrincipalClaim> claims, params string[] claimTypes)
    {
        var values = ExtractClaimValues(claims, claimTypes);
        return values.FirstOrDefault() ?? string.Empty;
    }

    private static IEnumerable<string> SplitClaimValue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return Array.Empty<string>();
        }

        return value
            .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(x => !string.IsNullOrWhiteSpace(x));
    }

    private static bool IsOpenAccess(IReadOnlyList<string> roles, IReadOnlyList<string> groups) =>
        roles.Count == 0 && groups.Count == 0;

    private static bool MatchesAny(PortalUserContext user, IReadOnlyList<string> roles, IReadOnlyList<string> groups)
    {
        var roleMatch = roles.Count > 0 && user.Roles.Any(role => roles.Contains(role, StringComparer.OrdinalIgnoreCase));
        var groupMatch = groups.Count > 0 && user.GroupIds.Any(group => groups.Contains(group, StringComparer.OrdinalIgnoreCase));
        return roleMatch || groupMatch;
    }

    private sealed class ClientPrincipalPayload
    {
        public string? AuthTyp { get; set; }
        public List<ClientPrincipalClaim>? Claims { get; set; }
    }

    private sealed class ClientPrincipalClaim
    {
        public string Typ { get; set; } = "";
        public string Val { get; set; } = "";
    }
}
