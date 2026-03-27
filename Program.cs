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
        // Register configuration services
        services.AddSingleton<ConfigurationService>();
        services.AddSingleton<ConnectionService>();
        // Stateless orchestration service; Singleton matches DHRefreshAASController and avoids captive Scoped dependency.
        services.AddSingleton<AasRefreshService>();
        services.AddSingleton<AasScalingService>();
        services.AddSingleton<ElasticPoolScalingService>();
        services.AddSingleton<RefreshConcurrencyService>();
        services.AddSingleton<OperationStorageService>();
        services.AddSingleton<OperationCleanupService>();
        services.AddHostedService(sp => sp.GetRequiredService<OperationCleanupService>());
        services.AddSingleton<ProgressTrackingService>();
        services.AddSingleton<ErrorHandlingService>();
        services.AddSingleton<RequestProcessingService>();
        services.AddSingleton<ResponseService>();
        services.AddSingleton<DHRefreshAASController>();
    })
    .Build();

await host.Services.GetRequiredService<OperationStorageService>().InitializeAsync();
host.Run();