using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IAasRefreshService
{
    Task<ActivityResponse> ExecuteRefreshWithRetryAsync(
        EnhancedPostData requestData,
        CancellationToken cancellationToken = default,
        Action<string, bool, string>? progressCallback = null,
        Action<SaveChangesDiagnostic>? saveChangesCallback = null);
}
