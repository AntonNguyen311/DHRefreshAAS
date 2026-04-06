namespace DHRefreshAAS.Services;

public interface IConfigurationService
{
    int GetConfigValue(string key, int defaultValue);
    string GetConfigValue(string key, string defaultValue);
    bool GetConfigValue(string key, bool defaultValue);
    IReadOnlyList<string> GetConfigList(string key);

    int MaxRetryAttempts { get; }
    int BaseDelaySeconds { get; }
    int ConnectionTimeoutMinutes { get; }
    int OperationTimeoutMinutes { get; }
    int SaveChangesTimeoutMinutes { get; }
    int SaveChangesMaxParallelism { get; }
    int SaveChangesBatchSize { get; }

    bool EnableAasAutoScaling { get; }
    string AasScaleUpSku { get; }
    string AasOriginalSku { get; }
    string AasResourceGroup { get; }
    string AasServerName { get; }
    string AasSubscriptionId { get; }
    int HeartbeatIntervalSeconds { get; }
    int ZombieTimeoutMinutes { get; }
    int MaxConcurrentRefreshes { get; }
    int SlowTableWarningSeconds { get; }
    int SlowTableCriticalSeconds { get; }
    int OperationRetentionDays { get; }

    bool EnableElasticPoolAutoScaling { get; }
    int ElasticPoolScaleUpDtu { get; }
    int ElasticPoolOriginalDtu { get; }
    string ElasticPoolResourceGroup { get; }
    string ElasticPoolServerName { get; }
    string ElasticPoolName { get; }
    bool EnableDetailedLogging { get; }

    string AasServerUrl { get; }
    string AasDatabase { get; }
    string AasAuthMode { get; }
    string AasUserId { get; }
    string AasPassword { get; }
    string AasTenantId { get; }

    string SelfServiceSqlConnectionString { get; }
    string SelfServiceSqlDatabaseName { get; }
    IReadOnlyList<string> PortalMetadataRoles { get; }
    IReadOnlyList<string> PortalRefreshRoles { get; }
    IReadOnlyList<string> PortalAdminRoles { get; }
    IReadOnlyList<string> PortalMetadataGroups { get; }
    IReadOnlyList<string> PortalRefreshGroups { get; }
    IReadOnlyList<string> PortalAdminGroups { get; }

    int GetConnectTimeoutSeconds(int connectionTimeoutMinutes);
    int GetCommandTimeoutSeconds(int operationTimeoutMinutes, int saveChangesTimeoutMinutes);
    (int ConnectTimeoutSeconds, int CommandTimeoutSeconds) GetDefaultClientTimeouts();
}
