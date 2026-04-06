using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Azure.Functions.Worker;
using DHRefreshAAS;
using DHRefreshAAS.Controllers;
using DHRefreshAAS.Services;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {
        services.AddHttpClient();
        // Register configuration services
        services.AddSingleton<IConfigurationService, ConfigurationService>();
        services.AddSingleton<IConnectionService, ConnectionService>();
        // Stateless orchestration service; Singleton matches controller lifetime and avoids captive Scoped dependency.
        services.AddSingleton<IAasRefreshService, AasRefreshService>();
        services.AddSingleton<AasScalingService>();
        services.AddSingleton<ElasticPoolScalingService>();
        services.AddSingleton<RefreshConcurrencyService>();
        services.AddSingleton<RowCountQueryService>();
        services.AddSingleton<SlowTableMetricsService>();
        services.AddSingleton<IOperationStorageService, OperationStorageService>();
        services.AddSingleton<OperationCleanupService>();
        services.AddHostedService(sp => sp.GetRequiredService<OperationCleanupService>());
        services.AddSingleton<ProgressTrackingService>();
        services.AddSingleton<ErrorHandlingService>();
        services.AddSingleton<RequestProcessingService>();
        services.AddSingleton<ResponseService>();
        services.AddSingleton<PortalAuthService>();
        services.AddSingleton<SelfServiceMetadataService>();
        services.AddSingleton<AdfOrchestratorGateService>();
        services.AddSingleton<QueueExecutionService>();
        services.AddSingleton<StatusResponseBuilder>();
        services.AddSingleton<AdfOrchestratorController>();
        services.AddSingleton<DiagnosticsController>();
        services.AddSingleton<RefreshController>();
        services.AddSingleton<PortalController>();
    })
    .Build();

await host.Services.GetRequiredService<IOperationStorageService>().InitializeAsync();
host.Run();
