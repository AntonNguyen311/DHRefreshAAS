using System.Text.Json;
using DHRefreshAAS.Models;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS.Services;

/// <summary>
/// Service for processing and validating HTTP requests
/// </summary>
public class RequestProcessingService
{
    private readonly ILogger<RequestProcessingService> _logger;
    private static readonly JsonSerializerOptions RequestJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public RequestProcessingService(ILogger<RequestProcessingService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Parses and validates the request body
    /// </summary>
    public virtual async Task<PostData?> ParseAndValidateRequestAsync(HttpRequestData request)
    {
        using var reader = new StreamReader(request.Body);
        string requestBody = await reader.ReadToEndAsync();
        
        if (string.IsNullOrWhiteSpace(requestBody))
        {
            _logger.LogWarning("Request body is empty");
            return null;
        }

        PostData? requestData;
        try
        {
            requestData = JsonSerializer.Deserialize<PostData>(requestBody, RequestJsonOptions);
        }
        catch (JsonException jsonEx)
        {
            _logger.LogError(jsonEx, "Invalid JSON in request body");
            return null;
        }

        return ValidateRequestData(requestData);
    }

    public virtual PostData? ValidateRequestData(PostData? requestData)
    {
        if (requestData?.DatabaseName == null)
        {
            _logger.LogWarning("Database name is null");
            return null;
        }

        if (requestData.RefreshObjects == null || requestData.RefreshObjects.Length == 0)
        {
            _logger.LogWarning("No refresh objects specified");
            return null;
        }

        var invalidObjects = requestData.RefreshObjects
            .Where(ro => string.IsNullOrWhiteSpace(ro?.Table))
            .ToList();

        if (invalidObjects.Any())
        {
            _logger.LogWarning("Found {InvalidCount} invalid refresh objects", invalidObjects.Count);
            return null;
        }

        _logger.LogInformation("Request validated successfully: Database={Database}, Tables={TableCount}", 
            requestData.DatabaseName, requestData.RefreshObjects.Length);
        
        return requestData;
    }

    /// <summary>
    /// Creates enhanced request data with configuration defaults
    /// </summary>
    public virtual EnhancedPostData CreateEnhancedRequestData(PostData requestData, IConfigurationService config)
    {
        return new EnhancedPostData
        {
            OriginalRequest = requestData,
            MaxRetryAttempts = requestData.MaxRetryAttempts ?? config.MaxRetryAttempts,
            BaseDelaySeconds = requestData.BaseDelaySeconds ?? config.BaseDelaySeconds,
            ConnectionTimeoutMinutes = requestData.ConnectionTimeoutMinutes ?? config.ConnectionTimeoutMinutes,
            OperationTimeoutMinutes = requestData.OperationTimeoutMinutes ?? config.OperationTimeoutMinutes
        };
    }

    /// <summary>
    /// Estimates operation duration based on request data
    /// </summary>
    public virtual int EstimateOperationDuration(PostData requestData)
    {
        var refreshCount = requestData.RefreshObjects?.Length ?? 0;
        var estimatedMinutes = 5 + (refreshCount * 2);
        return Math.Min(estimatedMinutes, 90);
    }
}
