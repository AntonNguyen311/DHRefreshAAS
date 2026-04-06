using System.Net;
using Microsoft.Azure.Functions.Worker.Http;

namespace DHRefreshAAS.Services;

public interface IErrorHandlingService
{
    Task<HttpResponseData> CreateErrorResponseAsync(HttpRequestData request, HttpStatusCode statusCode, string errorMessage, Exception? exception = null);
    Task<HttpResponseData> CreateValidationErrorResponseAsync(HttpRequestData request, string validationMessage);
    Task<HttpResponseData> CreateNotFoundResponseAsync(HttpRequestData request, string resourceName, string resourceId);
    Task<HttpResponseData> HandleExceptionAsync(HttpRequestData request, Exception exception, string operation);
}
