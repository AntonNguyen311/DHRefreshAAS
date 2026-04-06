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
        services.AddSingleton<IAasScalingService, AasScalingService>();
        services.AddSingleton<IElasticPoolScalingService, ElasticPoolScalingService>();
        services.AddSingleton<IRefreshConcurrencyService, RefreshConcurrencyService>();
        services.AddSingleton<RowCountQueryService>();
        services.AddSingleton<SlowTableMetricsService>();
        services.AddSingleton<IOperationStorageService, OperationStorageService>();
        services.AddSingleton<OperationCleanupService>();
        services.AddHostedService(sp => sp.GetRequiredService<OperationCleanupService>());
        services.AddSingleton<IProgressTrackingService, ProgressTrackingService>();
        services.AddSingleton<IErrorHandlingService, ErrorHandlingService>();
        services.AddSingleton<IRequestProcessingService, RequestProcessingService>();
        services.AddSingleton<IResponseService, ResponseService>();
        services.AddSingleton<IPortalAuthService, PortalAuthService>();
        services.AddSingleton<ISelfServiceMetadataService, SelfServiceMetadataService>();
        services.AddSingleton<IAdfOrchestratorGateService, AdfOrchestratorGateService>();
        services.AddSingleton<IQueueExecutionService, QueueExecutionService>();
        services.AddSingleton<IStatusResponseBuilder, StatusResponseBuilder>();
        services.AddSingleton<AdfOrchestratorController>();
        services.AddSingleton<DiagnosticsController>();
        services.AddSingleton<RefreshController>();
        services.AddSingleton<PortalController>();
    })
    .Build();

await host.Services.GetRequiredService<IOperationStorageService>().InitializeAsync();
host.Run();
