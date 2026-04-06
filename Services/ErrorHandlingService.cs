using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS.Services;

/// <summary>
/// Service for standardized error handling and response creation
/// </summary>
public class ErrorHandlingService : IErrorHandlingService
{
    private readonly ILogger<ErrorHandlingService> _logger;

    public ErrorHandlingService(ILogger<ErrorHandlingService> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <summary>
    /// Creates a standardized error response
    /// </summary>
    public virtual async Task<HttpResponseData> CreateErrorResponseAsync(
        HttpRequestData request, 
        HttpStatusCode statusCode, 
        string errorMessage, 
        Exception? exception = null)
    {
        var response = request.CreateResponse(statusCode);
        response.Headers.Add("Content-Type", "application/json");

        var errorResult = new
        {
            error = errorMessage,
            timestamp = DateTime.UtcNow,
            statusCode = (int)statusCode,
            exceptionType = exception?.GetType().Name,
            details = exception?.Message
        };

        await response.WriteStringAsync(JsonSerializer.Serialize(errorResult, new JsonSerializerOptions 
        { 
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        }));

        _logger.LogError(exception, "Error response created: {StatusCode} - {ErrorMessage}", statusCode, errorMessage);
        return response;
    }

    /// <summary>
    /// Creates a bad request response for validation errors
    /// </summary>
    public virtual async Task<HttpResponseData> CreateValidationErrorResponseAsync(
        HttpRequestData request, 
        string validationMessage)
    {
        return await CreateErrorResponseAsync(request, HttpStatusCode.BadRequest, validationMessage);
    }

    /// <summary>
    /// Creates a not found response
    /// </summary>
    public virtual async Task<HttpResponseData> CreateNotFoundResponseAsync(
        HttpRequestData request, 
        string resourceName, 
        string resourceId)
    {
        var message = $"{resourceName} '{resourceId}' not found";
        return await CreateErrorResponseAsync(request, HttpStatusCode.NotFound, message);
    }

    /// <summary>
    /// Logs and creates appropriate error response based on exception type
    /// </summary>
    public virtual async Task<HttpResponseData> HandleExceptionAsync(
        HttpRequestData request, 
        Exception exception, 
        string operation)
    {
        _logger.LogError(exception, "Error in {Operation}: {ErrorMessage}", operation, exception.Message);

        // Determine appropriate status code based on exception type
        var statusCode = exception switch
        {
            ArgumentException => HttpStatusCode.BadRequest,
            InvalidOperationException => HttpStatusCode.BadRequest,
            UnauthorizedAccessException => HttpStatusCode.Unauthorized,
            _ => HttpStatusCode.InternalServerError
        };

        var message = statusCode == HttpStatusCode.InternalServerError 
            ? "An internal server error occurred" 
            : exception.Message;

        return await CreateErrorResponseAsync(request, statusCode, message, exception);
    }
}
