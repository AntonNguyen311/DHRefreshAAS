using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Azure.Functions.Worker;
using DHRefreshAAS;
using DHRefreshAAS.Controllers;
using DHRefreshAAS.Services;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication(builder =>
    {
        // Configure CORS for Azure Portal and other origins
        builder.Services.AddCors(options =>
        {
            options.AddDefaultPolicy(policy =>
            {
                policy.WithOrigins(
                    "https://portal.azure.com",
                    "https://ms.portal.azure.com", 
                    "https://preview.portal.azure.com",
                    "https://rc.portal.azure.com",
                    "https://canary.portal.azure.com",
                    "https://localhost:3000",
                    "http://localhost:3000"
                )
                .AllowAnyMethod()
                .AllowAnyHeader()
                .SetIsOriginAllowedToAllowWildcardSubdomains();
            });
        });
    })
    .ConfigureServices(services =>
    {
        // Register configuration services
        services.AddSingleton<ConfigurationService>();
        services.AddSingleton<ConnectionService>();
        // Stateless orchestration service; Singleton matches DHRefreshAASController and avoids captive Scoped dependency.
        services.AddSingleton<AasRefreshService>();
        services.AddSingleton<OperationStorageService>();
        services.AddSingleton<ProgressTrackingService>();
        services.AddSingleton<ErrorHandlingService>();
        services.AddSingleton<RequestProcessingService>();
        services.AddSingleton<ResponseService>();
        services.AddSingleton<DHRefreshAASController>();
    })
    .Build();

host.Run();