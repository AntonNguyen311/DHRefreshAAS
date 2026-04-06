using Microsoft.Azure.Functions.Worker.Http;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IRequestProcessingService
{
    Task<PostData?> ParseAndValidateRequestAsync(HttpRequestData request);
    PostData? ValidateRequestData(PostData? requestData);
    EnhancedPostData CreateEnhancedRequestData(PostData requestData, IConfigurationService config);
    int EstimateOperationDuration(PostData requestData);
}
