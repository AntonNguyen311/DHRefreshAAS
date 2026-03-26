using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker.Http;

namespace DHRefreshAAS.Services;

/// <summary>
/// Service for creating standardized HTTP responses
/// </summary>
public class ResponseService
{
    private readonly JsonSerializerOptions _jsonOptions;

    public ResponseService()
    {
        _jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }

    /// <summary>
    /// Creates a successful response with JSON content
    /// </summary>
    public virtual async Task<HttpResponseData> CreateSuccessResponseAsync<T>(
        HttpRequestData request, 
        T data, 
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        var response = request.CreateResponse(statusCode);
        response.Headers.Add("Content-Type", "application/json");
        
        await response.WriteStringAsync(JsonSerializer.Serialize(data, _jsonOptions));
        return response;
    }

    /// <summary>
    /// Creates an accepted response for async operations
    /// </summary>
    public virtual async Task<HttpResponseData> CreateAcceptedResponseAsync(
        HttpRequestData request, 
        string operationId, 
        int estimatedDurationMinutes)
    {
        var responseData = new
        {
            operationId = operationId,
            status = "accepted",
            message = "Refresh operation started in background. Use status endpoint to monitor progress.",
            estimatedDurationMinutes = estimatedDurationMinutes
        };

        return await CreateSuccessResponseAsync(request, responseData, HttpStatusCode.Accepted);
    }

    /// <summary>
    /// Creates a status response for operation monitoring
    /// </summary>
    public virtual async Task<HttpResponseData> CreateStatusResponseAsync(
        HttpRequestData request, 
        object statusData)
    {
        return await CreateSuccessResponseAsync(request, statusData, HttpStatusCode.OK);
    }

    /// <summary>
    /// Creates a not found response
    /// </summary>
    public virtual async Task<HttpResponseData> CreateNotFoundResponseAsync(
        HttpRequestData request, 
        string resourceName, 
        string resourceId)
    {
        var responseData = new
        {
            error = $"{resourceName} {resourceId} not found",
            message = "Operation may have expired or never existed"
        };

        return await CreateSuccessResponseAsync(request, responseData, HttpStatusCode.NotFound);
    }
}
