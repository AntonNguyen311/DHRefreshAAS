using Microsoft.AnalysisServices.Tabular;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IConnectionService
{
    Task<TokenTestResult> TestTokenAcquisitionAsync(CancellationToken cancellationToken);
    Task<ConnectionTestResult> TestConnectionAsync(CancellationToken cancellationToken);
    Task<Server> CreateServerConnectionAsync(CancellationToken cancellationToken, int connectTimeoutSeconds, int commandTimeoutSeconds);
    Task<Server> CreateServerConnectionAsync(CancellationToken cancellationToken, int connectTimeoutSeconds, int commandTimeoutSeconds, string? initialCatalogOverride);
    Task<bool> WaitForModelReadyAsync(CancellationToken cancellationToken, int maxRetries = 12, int delaySeconds = 15);
    Task<bool> WaitForServerReadyAsync(CancellationToken cancellationToken, int maxRetries = 18, int delaySeconds = 10);
    Task SafeDisconnectAsync(Server? server);
    string GetAdomdConnectionString(string? databaseName = null, int connectTimeoutSeconds = 60, int commandTimeoutSeconds = 120);
}
