using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IQueueExecutionService
{
    Task<QueueOperationResult> StartAsyncOperationAsync(
        PostData requestData,
        EnhancedPostData enhancedRequestData,
        int estimatedDurationMinutes,
        PortalUserContext? requester = null,
        string requestSource = "api");
}
