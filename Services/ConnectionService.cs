using Microsoft.AnalysisServices.Tabular;
using Microsoft.Identity.Client;
using Microsoft.Extensions.Logging;
using System.Text;
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
        return BuildConnectionString(connectTimeoutSeconds, commandTimeoutSeconds, null);
    }

    private string BuildConnectionString(int connectTimeoutSeconds, int commandTimeoutSeconds, string? initialCatalogOverride)
    {
        var dataSource = _config.AasServerUrl;
        var initialCatalog = string.IsNullOrWhiteSpace(initialCatalogOverride)
            ? _config.AasDatabase
            : initialCatalogOverride.Trim();
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
    /// Get connection string for ADOMD queries (e.g. row count after refresh)
    /// </summary>
    public string GetAdomdConnectionString(string? databaseName = null, int connectTimeoutSeconds = 60, int commandTimeoutSeconds = 120)
    {
        return BuildConnectionString(connectTimeoutSeconds, commandTimeoutSeconds, databaseName);
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

                var tokenEndpoint = $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token";
                _logger.LogInformation("Testing token acquisition for Service Principal: {ClientId}", clientId);

                result.TokenEndpoint = tokenEndpoint;

                var app = ConfidentialClientApplicationBuilder
                    .Create(clientId)
                    .WithClientSecret(clientSecret)
                    .WithTenantId(tenantId)
                    .Build();

                var authResult = await app
                    .AcquireTokenForClient(new[] { "https://*.asazure.windows.net/.default" })
                    .ExecuteAsync(cancellationToken);

                result.HttpStatusCode = 200;
                result.IsSuccessful = true;
                result.TokenAcquired = !string.IsNullOrWhiteSpace(authResult.AccessToken);
                result.TokenLength = authResult.AccessToken?.Length ?? 0;
                result.TokenType = authResult.TokenType;
                result.TokenExpiresInSeconds = (int)Math.Max(0, (authResult.ExpiresOn.UtcDateTime - DateTime.UtcNow).TotalSeconds);
                result.ResponseBody = "MSAL token acquisition succeeded.";
            }
            else
            {
                result.ErrorMessage = $"Token test only supports ServicePrincipal mode, current mode: {_config.AasAuthMode}";
            }
        }
        catch (MsalServiceException ex)
        {
            result.ErrorMessage = ex.Message;
            result.ExceptionType = ex.GetType().Name;
            result.HttpStatusCode = ex.StatusCode;
            _logger.LogError(ex, "Token acquisition test failed with MSAL service error: {ErrorMessage}", ex.Message);
        }
        catch (MsalClientException ex)
        {
            result.ErrorMessage = ex.Message;
            result.ExceptionType = ex.GetType().Name;
            _logger.LogError(ex, "Token acquisition test failed with MSAL client error: {ErrorMessage}", ex.Message);
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
        return await CreateServerConnectionAsync(cancellationToken, connectTimeoutSeconds, commandTimeoutSeconds, null);
    }

    public virtual async Task<Server> CreateServerConnectionAsync(
        CancellationToken cancellationToken,
        int connectTimeoutSeconds,
        int commandTimeoutSeconds,
        string? initialCatalogOverride)
    {
        var server = new Server();
        var connectionString = BuildConnectionString(connectTimeoutSeconds, commandTimeoutSeconds, initialCatalogOverride);
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
    /// Wait for AAS model to be fully loaded into memory after scaling.
    /// Scaling restarts the server and clears in-memory data; this method
    /// polls until the database is accessible and the model is loaded.
    /// </summary>
    public virtual async Task<bool> WaitForModelReadyAsync(CancellationToken cancellationToken, int maxRetries = 12, int delaySeconds = 15)
    {
        _logger.LogInformation("Waiting for AAS model '{Database}' to be ready (max {MaxWait}s)...",
            _config.AasDatabase, maxRetries * delaySeconds);

        for (int i = 0; i < maxRetries; i++)
        {
            Server? server = null;
            try
            {
                server = new Server();
                var connectionString = BuildConnectionString(connectTimeoutSeconds: 30, commandTimeoutSeconds: 30);
                await Task.Run(() => server.Connect(connectionString), cancellationToken);

                if (server.Connected)
                {
                    var database = server.Databases.GetByName(_config.AasDatabase);
                    if (database != null)
                    {
                        var tableCount = database.Model?.Tables?.Count ?? 0;
                        _logger.LogInformation(
                            "Model ready: Database '{Database}' loaded with {TableCount} tables (attempt {Attempt}/{Max})",
                            _config.AasDatabase, tableCount, i + 1, maxRetries);
                        return true;
                    }
                    _logger.LogWarning("Model ready check: Database '{Database}' not found (attempt {Attempt}/{Max})",
                        _config.AasDatabase, i + 1, maxRetries);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Model ready check failed (attempt {Attempt}/{Max}): {Message}",
                    i + 1, maxRetries, ex.Message);
            }
            finally
            {
                await SafeDisconnectAsync(server);
            }

            await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
        }

        _logger.LogWarning("Model readiness check timed out after {MaxWait}s", maxRetries * delaySeconds);
        return false;
    }

    /// <summary>
    /// Wait until the AAS server accepts connections (any database).
    /// Use after scale operations to avoid "server starting" errors.
    /// </summary>
    public virtual async Task<bool> WaitForServerReadyAsync(CancellationToken cancellationToken, int maxRetries = 18, int delaySeconds = 10)
    {
        _logger.LogInformation("Probing AAS server readiness (max {MaxWait}s)...", maxRetries * delaySeconds);

        for (int i = 0; i < maxRetries; i++)
        {
            Server? server = null;
            try
            {
                server = new Server();
                var connectionString = BuildConnectionString(connectTimeoutSeconds: 15, commandTimeoutSeconds: 15);
                await Task.Run(() => server.Connect(connectionString), cancellationToken);

                if (server.Connected)
                {
                    _logger.LogInformation("AAS server ready (attempt {Attempt}/{Max})", i + 1, maxRetries);
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Server readiness probe attempt {Attempt}/{Max}: {Message}", i + 1, maxRetries, ex.Message);
            }
            finally
            {
                await SafeDisconnectAsync(server);
            }

            await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
        }

        _logger.LogWarning("AAS server readiness probe timed out after {MaxWait}s", maxRetries * delaySeconds);
        return false;
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