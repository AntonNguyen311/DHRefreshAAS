using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IAdfOrchestratorGateService
{
    Task<AdfOrchestratorDispatchResult> TryStartElJobIfIdleAsync(
        AdfOrchestratorGateRequest? request,
        CancellationToken cancellationToken);
}
