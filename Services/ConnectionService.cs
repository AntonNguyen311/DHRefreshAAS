using Microsoft.AnalysisServices.Tabular;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Net.Http;
using System.Text.Json;
using DHRefreshAAS.Models;

namespace DHRefreshAAS;

/// <summary>
/// Service for managing AAS connections with configurable, non-interactive authentication
/// </summary>
public class ConnectionService
{
    private readonly ILogger<ConnectionService> _logger;
    private readonly ConfigurationService _config;

    public ConnectionService(ConfigurationService config, ILogger<ConnectionService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(logger);
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Build connection string based on Azure best practices and research
    /// Supports Managed Identity (recommended), Service Principal, and User/Password auth
    /// </summary>
    private string BuildConnectionString(int connectTimeoutSeconds, int commandTimeoutSeconds)
    {
        var dataSource = _config.AasServerUrl;
        var initialCatalog = _config.AasDatabase;
        var authMode = _config.AasAuthMode; // "ManagedIdentity", "ServicePrincipal", or "UserPassword"
        var userId = _config.AasUserId;
        var password = _config.AasPassword;
        var tenantId = _config.AasTenantId;

        if (string.IsNullOrWhiteSpace(dataSource))
        {
            throw new InvalidOperationException("AAS_SERVER_URL is not configured.");
        }

        var builder = new StringBuilder();
        builder.Append("Provider=MSOLAP;");
        builder.Append($"Data Source={dataSource};");
        
        if (!string.IsNullOrWhiteSpace(initialCatalog))
        {
            builder.Append($"Initial Catalog={initialCatalog};");
        }

        if (string.Equals(authMode, "ManagedIdentity", StringComparison.OrdinalIgnoreCase))
        {
            // Managed Identity - Most secure for Azure Functions
            // No credentials needed, uses Function App's system-assigned or user-assigned identity
            _logger.LogInformation("Using Managed Identity authentication for AAS connection");
            
            // For Managed Identity, we rely on Azure.Identity DefaultAzureCredential
            // The connection string uses integrated authentication
            builder.Append("Integrated Security=SSPI;");
            builder.Append("Persist Security Info=True;");
            builder.Append("Impersonation Level=Impersonate;");
        }
        else         if (string.Equals(authMode, "ServicePrincipal", StringComparison.OrdinalIgnoreCase))
        {
            // Service Principal - Good for automation scenarios
            if (string.IsNullOrWhiteSpace(userId) || string.IsNullOrWhiteSpace(password) || string.IsNullOrWhiteSpace(tenantId))
            {
                throw new InvalidOperationException("Service Principal auth requires AAS_USER_ID (client id), AAS_PASSWORD (client secret), and AAS_TENANT_ID.");
            }

            _logger.LogInformation("Using Service Principal authentication for AAS connection");
            
            // Service Principal format: app:<clientId>@<tenantId>
            var spUserId = userId.StartsWith("app:", StringComparison.OrdinalIgnoreCase) || userId.Contains("@")
                ? userId
                : $"app:{userId}@{tenantId}";

            builder.Append($"User ID={spUserId};");
            builder.Append($"Password={password};");
            builder.Append("Persist Security Info=True;");
            builder.Append("Impersonation Level=Impersonate;");
        }
        else if (string.Equals(authMode, "UserPassword", StringComparison.OrdinalIgnoreCase))
        {
            // Azure AD User/Password authentication
            // Based on research: requires Azure AD user (not Microsoft Account)
            if (string.IsNullOrWhiteSpace(userId) || string.IsNullOrWhiteSpace(password))
            {
                throw new InvalidOperationException("User/Password auth requires AAS_USER_ID (UPN format like user@domain.com) and AAS_PASSWORD.");
            }

            // Validate UPN format for Azure AD authentication
            if (!userId.Contains("@"))
            {
                throw new InvalidOperationException("AAS_USER_ID must be in UPN format (user@domain.com) for Azure AD authentication.");
            }

            _logger.LogInformation("Using User/Password authentication for AAS connection");
            
            builder.Append($"User ID={userId};");
            builder.Append($"Password={password};");
            builder.Append("Persist Security Info=True;");
            builder.Append("Impersonation Level=Impersonate;");
        }
        else
        {
            throw new InvalidOperationException($"Unsupported authentication mode: {authMode}. Supported modes: ManagedIdentity, ServicePrincipal, UserPassword");
        }

        builder.Append($"Connect Timeout={connectTimeoutSeconds};");
        builder.Append($"Command Timeout={commandTimeoutSeconds};");
        
        return builder.ToString();
    }

    private string GetSanitizedConnectionString(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString)) return connectionString;
        var startIdx = connectionString.IndexOf("Password=", StringComparison.OrdinalIgnoreCase);
        if (startIdx < 0) return connectionString;
        var endIdx = connectionString.IndexOf(';', startIdx);
        if (endIdx >= 0)
        {
            // Remove through the semicolon to avoid duplicating it when inserting the masked value
            return connectionString.Remove(startIdx, (endIdx - startIdx) + 1)
                                   .Insert(startIdx, "Password=***;");
        }
        else
        {
            // No trailing semicolon found after Password=; append masked value at the end
            return connectionString.Substring(0, startIdx) + "Password=***;";
        }
    }

    /// <summary>
    /// Test Azure AD token acquisition for Service Principal
    /// </summary>
    public virtual async Task<TokenTestResult> TestTokenAcquisitionAsync(CancellationToken cancellationToken)
    {
        var result = new TokenTestResult
        {
            IsSuccessful = false,
            AuthenticationMode = _config.AasAuthMode,
            TestTimestamp = DateTime.UtcNow
        };

        var startTime = DateTime.UtcNow;

        try
        {
            if (string.Equals(_config.AasAuthMode, "ServicePrincipal", StringComparison.OrdinalIgnoreCase))
            {
                var clientId = _config.AasUserId;
                var clientSecret = _config.AasPassword;
                var tenantId = _config.AasTenantId;

                if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret) || string.IsNullOrWhiteSpace(tenantId))
                {
                    result.ErrorMessage = "Service Principal credentials are incomplete";
                    return result;
                }

                result.ClientId = clientId;
                result.TenantId = tenantId;

                // Test token acquisition using OAuth2 client credentials flow
                using var httpClient = new HttpClient();
                var tokenEndpoint = $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token";
                
                var tokenRequest = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("grant_type", "client_credentials"),
                    new KeyValuePair<string, string>("client_id", clientId),
                    new KeyValuePair<string, string>("client_secret", clientSecret),
                    new KeyValuePair<string, string>("scope", "https://*.asazure.windows.net/.default")
                });

                _logger.LogInformation("Testing token acquisition for Service Principal: {ClientId}", clientId);
                
                var response = await httpClient.PostAsync(tokenEndpoint, tokenRequest, cancellationToken);
                var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);

                result.TokenEndpoint = tokenEndpoint;
                result.HttpStatusCode = (int)response.StatusCode;
                result.ResponseBody = responseContent;

                if (response.IsSuccessStatusCode)
                {
                    var tokenResponse = System.Text.Json.JsonSerializer.Deserialize<JsonElement>(responseContent);
                    if (tokenResponse.TryGetProperty("access_token", out var tokenElement))
                    {
                        var token = tokenElement.GetString();
                        result.IsSuccessful = true;
                        result.TokenAcquired = !string.IsNullOrEmpty(token);
                        result.TokenLength = token?.Length ?? 0;
                        
                        // Extract token info
                        if (tokenResponse.TryGetProperty("expires_in", out var expiresElement))
                        {
                            result.TokenExpiresInSeconds = expiresElement.GetInt32();
                        }
                        if (tokenResponse.TryGetProperty("token_type", out var typeElement))
                        {
                            result.TokenType = typeElement.GetString() ?? "";
                        }
                    }
                }
                else
                {
                    result.ErrorMessage = $"Token acquisition failed with status {response.StatusCode}";
                    _logger.LogError("Token acquisition failed: {StatusCode} - {Content}", response.StatusCode, responseContent);
                }
            }
            else
            {
                result.ErrorMessage = $"Token test only supports ServicePrincipal mode, current mode: {_config.AasAuthMode}";
            }
        }
        catch (Exception ex)
        {
            result.ErrorMessage = ex.Message;
            result.ExceptionType = ex.GetType().Name;
            _logger.LogError(ex, "Token acquisition test failed: {ErrorMessage}", ex.Message);
        }

        result.TestDurationMs = (DateTime.UtcNow - startTime).TotalMilliseconds;
        return result;
    }

    /// <summary>
    /// Test connection without keeping it open - for validation purposes
    /// </summary>
    public virtual async Task<ConnectionTestResult> TestConnectionAsync(CancellationToken cancellationToken)
    {
        var result = new ConnectionTestResult
        {
            IsSuccessful = false,
            ServerUrl = _config.AasServerUrl,
            Database = _config.AasDatabase,
            AuthenticationMode = _config.AasAuthMode,
            TestTimestamp = DateTime.UtcNow
        };

        var startTime = DateTime.UtcNow;
        Server? server = null;

        try
        {
            var (connectSec, commandSec) = _config.GetDefaultClientTimeouts();
            var connectionString = BuildConnectionString(connectSec, commandSec);
            result.ConnectionString = GetSanitizedConnectionString(connectionString);
            
            _logger.LogInformation("Testing AAS connection using auth mode '{AuthMode}' with Data Source '{DataSource}'.",
                _config.AasAuthMode, _config.AasServerUrl);

            server = new Server();
            await Task.Run(() => server.Connect(connectionString), cancellationToken);

            if (server.Connected)
            {
                result.IsSuccessful = true;
                result.ServerVersion = server.Version;
                result.ServerEdition = server.Edition.ToString();
                result.ConnectionTimeMs = (DateTime.UtcNow - startTime).TotalMilliseconds;
                
                // Test database access if specified
                if (!string.IsNullOrWhiteSpace(_config.AasDatabase))
                {
                    var database = server.Databases.GetByName(_config.AasDatabase);
                    if (database != null)
                    {
                        result.DatabaseFound = true;
                        result.DatabaseLastUpdate = database.LastUpdate;
                        result.TablesCount = database.Model?.Tables?.Count ?? 0;
                    }
                    else
                    {
                        result.DatabaseFound = false;
                        result.ErrorMessage = $"Database '{_config.AasDatabase}' not found on server";
                    }
                }

                _logger.LogInformation("AAS connection test successful. Server: {ServerVersion}, Database found: {DatabaseFound}",
                    result.ServerVersion, result.DatabaseFound);
            }
            else
            {
                result.ErrorMessage = "Failed to connect to AAS server - connection returned false";
                _logger.LogWarning("AAS connection test failed - server.Connected returned false");
            }
        }
        catch (Exception ex)
        {
            result.ErrorMessage = ex.Message;
            result.ExceptionType = ex.GetType().Name;
            result.ConnectionTimeMs = (DateTime.UtcNow - startTime).TotalMilliseconds;
            
            _logger.LogError(ex, "AAS connection test failed. Error: {ErrorMessage}", ex.Message);
        }
        finally
        {
            if (server?.Connected == true)
            {
                try
                {
                    await Task.Run(() => server.Disconnect());
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disconnecting test connection");
                }
            }
            server?.Dispose();
        }

        return result;
    }

    /// <summary>
    /// Create server connection (non-interactive) for refresh operations
    /// </summary>
    public virtual async Task<Server> CreateServerConnectionAsync(
        CancellationToken cancellationToken,
        int connectTimeoutSeconds,
        int commandTimeoutSeconds)
    {
        var server = new Server();
        var connectionString = BuildConnectionString(connectTimeoutSeconds, commandTimeoutSeconds);
        _logger.LogInformation("Connecting to AAS using auth mode '{AuthMode}' with Data Source '{DataSource}'.",
            _config.AasAuthMode, _config.AasServerUrl);

        try
        {
            await Task.Run(() => server.Connect(connectionString), cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to AAS. Connection string (sanitized): {ConnectionString}",
                GetSanitizedConnectionString(connectionString));
            throw;
        }

        if (!server.Connected)
        {
            _logger.LogError("Failed to connect to AAS. Connection string (sanitized): {ConnectionString}",
                GetSanitizedConnectionString(connectionString));
            throw new InvalidOperationException("Failed to connect to AAS server");
        }

        _logger.LogInformation("Connected to AAS successfully.");
        return server;
    }

    /// <summary>
    /// Safely disconnect from server with error handling
    /// </summary>
    public virtual async Task SafeDisconnectAsync(Server? server)
    {
        if (server?.Connected == true)
        {
            try
            {
                _logger.LogInformation("Disconnecting from AAS server...");
                await Task.Run(() => server.Disconnect());
                _logger.LogInformation("Successfully disconnected from AAS server.");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error during server disconnection, but continuing...");
            }
        }

        server?.Dispose();
    }
}