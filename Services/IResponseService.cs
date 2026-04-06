using System.Net;
using Microsoft.Azure.Functions.Worker.Http;

namespace DHRefreshAAS.Services;

public interface IResponseService
{
    Task<HttpResponseData> CreateSuccessResponseAsync<T>(HttpRequestData request, T data, HttpStatusCode statusCode = HttpStatusCode.OK);
    Task<HttpResponseData> CreateAcceptedResponseAsync(HttpRequestData request, string operationId, int estimatedDurationMinutes, string operationStatus = "running", string? message = null, int? queuePosition = null, string? queueScope = null);
    Task<HttpResponseData> CreateAcceptedResponseAsync(HttpRequestData request, string operationId, int estimatedDurationMinutes, string operationStatus, string? message, int? queuePosition, string? queueScope, string statusPath);
    Task<HttpResponseData> CreateBadRequestResponseAsync(HttpRequestData request, string message);
    Task<HttpResponseData> CreateUnauthorizedResponseAsync(HttpRequestData request, string message);
    Task<HttpResponseData> CreateForbiddenResponseAsync(HttpRequestData request, string message);
    Task<HttpResponseData> CreateStatusResponseAsync(HttpRequestData request, object statusData);
    Task<HttpResponseData> CreateNotFoundResponseAsync(HttpRequestData request, string resourceName, string resourceId);
}
