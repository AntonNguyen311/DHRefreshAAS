using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using DHRefreshAAS.Services;

namespace DHRefreshAAS;

/// <summary>
/// Configuration service for reading environment variables and settings
/// </summary>
public class ConfigurationService : IConfigurationService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<ConfigurationService> _logger;

    public ConfigurationService(IConfiguration configuration, ILogger<ConfigurationService> logger)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(logger);
        _configuration = configuration;
        _logger = logger;
    }

    public virtual int GetConfigValue(string key, int defaultValue)
    {
        var value = _configuration[key];
        if (int.TryParse(value, out var result))
        {
            _logger.LogDebug("Configuration {Key} = {Value}", key, result);
            return result;
        }
        
        _logger.LogDebug("Configuration {Key} not found or invalid, using default {DefaultValue}", key, defaultValue);
        return defaultValue;
    }

    public virtual string GetConfigValue(string key, string defaultValue)
    {
        var value = _configuration[key];
        if (!string.IsNullOrWhiteSpace(value))
        {
            _logger.LogDebug("Configuration {Key} found", key);
            return value;
        }
        
        _logger.LogDebug("Configuration {Key} not found, using default", key);
        return defaultValue;
    }

    public virtual bool GetConfigValue(string key, bool defaultValue)
    {
        var value = _configuration[key];
        if (bool.TryParse(value, out var result))
        {
            _logger.LogDebug("Configuration {Key} = {Value}", key, result);
            return result;
        }
        
        _logger.LogDebug("Configuration {Key} not found or invalid, using default {DefaultValue}", key, defaultValue);
        return defaultValue;
    }

    public virtual IReadOnlyList<string> GetConfigList(string key)
    {
        var value = _configuration[key];
        if (string.IsNullOrWhiteSpace(value))
        {
            return Array.Empty<string>();
        }

        var parts = value
            .Split(new[] { ',', ';', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        _logger.LogDebug("Configuration {Key} loaded with {Count} entries", key, parts.Length);
        return parts;
    }

    // Configuration constants with fallback values
    public virtual int MaxRetryAttempts => GetConfigValue("MAX_RETRY_ATTEMPTS", 3);
    public virtual int BaseDelaySeconds => GetConfigValue("BASE_DELAY_SECONDS", 30);
    public virtual int ConnectionTimeoutMinutes => GetConfigValue("CONNECTION_TIMEOUT_MINUTES", 10);
    public virtual int OperationTimeoutMinutes => GetConfigValue("OPERATION_TIMEOUT_MINUTES", 60);
    public virtual int SaveChangesTimeoutMinutes => GetConfigValue("SAVE_CHANGES_TIMEOUT_MINUTES", 15);
    public virtual int SaveChangesMaxParallelism => GetConfigValue("SAVE_CHANGES_MAX_PARALLELISM", 2);
    public virtual int SaveChangesBatchSize => GetConfigValue("SAVE_CHANGES_BATCH_SIZE", 3);

    // AAS Auto-Scaling settings
    public virtual bool EnableAasAutoScaling => GetConfigValue("ENABLE_AAS_AUTO_SCALING", false);
    public virtual string AasScaleUpSku => GetConfigValue("AAS_SCALE_UP_SKU", "S4");
    public virtual string AasOriginalSku => GetConfigValue("AAS_ORIGINAL_SKU", "S2");
    public virtual string AasResourceGroup => GetConfigValue("AAS_RESOURCE_GROUP", "vn-rg-sa-sdp-solution-p");
    public virtual string AasServerName => GetConfigValue("AAS_SERVER_NAME", "vnaassasdpp01");
    public virtual string AasSubscriptionId => GetConfigValue("AAS_SUBSCRIPTION_ID", "8730775e-045c-47d1-a080-e3b9882cec01");
    public virtual int HeartbeatIntervalSeconds => GetConfigValue("HEARTBEAT_INTERVAL_SECONDS", 30);
    public virtual int ZombieTimeoutMinutes => GetConfigValue("ZOMBIE_TIMEOUT_MINUTES", 30);
    public virtual int MaxConcurrentRefreshes => GetConfigValue("MAX_CONCURRENT_REFRESHES", 5);
    public virtual int SlowTableWarningSeconds => GetConfigValue("SLOW_TABLE_WARNING_SECONDS", 120);
    public virtual int SlowTableCriticalSeconds => GetConfigValue("SLOW_TABLE_CRITICAL_SECONDS", 300);

    // Elastic Pool Auto-Scaling settings
    public virtual bool EnableElasticPoolAutoScaling => GetConfigValue("ENABLE_ELASTIC_POOL_AUTO_SCALING", false);
    public virtual int ElasticPoolScaleUpDtu => GetConfigValue("ELASTIC_POOL_SCALE_UP_DTU", 1600);
    public virtual int ElasticPoolOriginalDtu => GetConfigValue("ELASTIC_POOL_ORIGINAL_DTU", 800);
    public virtual string ElasticPoolResourceGroup => GetConfigValue("ELASTIC_POOL_RESOURCE_GROUP", "vn-rg-sa-sdp-solution-p");
    public virtual string ElasticPoolServerName => GetConfigValue("ELASTIC_POOL_SERVER_NAME", "vn-sql-sa-sdp-solution-p-01");
    public virtual string ElasticPoolName => GetConfigValue("ELASTIC_POOL_NAME", "vn-sql-sa-sdp-pool");
    public virtual bool EnableDetailedLogging => GetConfigValue("ENABLE_DETAILED_LOGGING", true);

    // AAS Connection settings
    public virtual string AasServerUrl => GetConfigValue("AAS_SERVER_URL", "asazure://southeastasia.asazure.windows.net/deheusaas");
    public virtual string AasDatabase => GetConfigValue("AAS_DATABASE", "DAModel");
    public virtual string AasAuthMode => GetConfigValue("AAS_AUTH_MODE", "ManagedIdentity"); // ManagedIdentity, ServicePrincipal, or UserPassword
    public virtual string AasUserId => GetConfigValue("AAS_USER_ID", ""); // For ServicePrincipal: client id; For UserPassword: UPN
    public virtual string AasPassword => GetConfigValue("AAS_PASSWORD", ""); // For ServicePrincipal: client secret; For UserPassword: password
    public virtual string AasTenantId => GetConfigValue("AAS_TENANT_ID", "");

    // Self-service portal settings
    public virtual string SelfServiceSqlConnectionString => GetConfigValue("SELF_SERVICE_SQL_CONNECTION_STRING", "");
    public virtual string SelfServiceSqlDatabaseName => GetConfigValue("SELF_SERVICE_SQL_DATABASE_NAME", "datalakeprod");
    public virtual IReadOnlyList<string> PortalMetadataRoles => GetConfigList("PORTAL_METADATA_ROLES");
    public virtual IReadOnlyList<string> PortalRefreshRoles => GetConfigList("PORTAL_REFRESH_ROLES");
    public virtual IReadOnlyList<string> PortalAdminRoles => GetConfigList("PORTAL_ADMIN_ROLES");
    public virtual IReadOnlyList<string> PortalMetadataGroups => GetConfigList("PORTAL_METADATA_GROUP_IDS");
    public virtual IReadOnlyList<string> PortalRefreshGroups => GetConfigList("PORTAL_REFRESH_GROUP_IDS");
    public virtual IReadOnlyList<string> PortalAdminGroups => GetConfigList("PORTAL_ADMIN_GROUP_IDS");

    /// <summary>MSOLAP Connect Timeout (seconds); clamp 30–3600.</summary>
    public virtual int GetConnectTimeoutSeconds(int connectionTimeoutMinutes)
    {
        var seconds = connectionTimeoutMinutes * 60;
        return Math.Clamp(seconds, 30, 3600);
    }

    /// <summary>MSOLAP Command Timeout (seconds); derived from refresh budget, clamp 120–7200 (2h).</summary>
    public virtual int GetCommandTimeoutSeconds(int operationTimeoutMinutes, int saveChangesTimeoutMinutes)
    {
        var budgetMinutes = Math.Max(operationTimeoutMinutes, saveChangesTimeoutMinutes);
        var seconds = budgetMinutes * 60 + 120;
        return Math.Clamp(seconds, 120, 7200);
    }

    /// <summary>Defaults for diagnostics (e.g. test connection) from host configuration.</summary>
    public virtual (int ConnectTimeoutSeconds, int CommandTimeoutSeconds) GetDefaultClientTimeouts()
    {
        var connect = GetConnectTimeoutSeconds(ConnectionTimeoutMinutes);
        var command = GetCommandTimeoutSeconds(OperationTimeoutMinutes, SaveChangesTimeoutMinutes);
        return (connect, command);
    }
}