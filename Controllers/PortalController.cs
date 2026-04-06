using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;

namespace DHRefreshAAS.Controllers;

/// <summary>
/// Self-service portal endpoints (Entra ID authenticated).
/// </summary>
public class PortalController
{
    private readonly IPortalAuthService _portalAuthService;
    private readonly ISelfServiceMetadataService _selfServiceMetadataService;
    private readonly IRequestProcessingService _requestProcessing;
    private readonly IConfigurationService _config;
    private readonly IQueueExecutionService _queueExecution;
    private readonly IStatusResponseBuilder _statusResponseBuilder;
    private readonly IResponseService _responseService;
    private readonly IErrorHandlingService _errorHandling;
    private readonly ILogger<PortalController> _logger;

    public PortalController(
        IPortalAuthService portalAuthService,
        ISelfServiceMetadataService selfServiceMetadataService,
        IRequestProcessingService requestProcessing,
        IConfigurationService config,
        IQueueExecutionService queueExecution,
        IStatusResponseBuilder statusResponseBuilder,
        IResponseService responseService,
        IErrorHandlingService errorHandling,
        ILogger<PortalController> logger)
    {
        _portalAuthService = portalAuthService;
        _selfServiceMetadataService = selfServiceMetadataService;
        _requestProcessing = requestProcessing;
        _config = config;
        _queueExecution = queueExecution;
        _statusResponseBuilder = statusResponseBuilder;
        _responseService = responseService;
        _errorHandling = errorHandling;
        _logger = logger;
    }

    [Function("DHRefreshAAS_PortalModels")]
    public async Task<HttpResponseData> PortalListModels(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
        FunctionContext context)
    {
        var user = _portalAuthService.GetPortalUser(req);
        if (user == null)
        {
            return await _responseService.CreateUnauthorizedResponseAsync(req, "Portal authentication is required.");
        }

        if (!_portalAuthService.CanReadMetadata(user))
        {
            return await _responseService.CreateForbiddenResponseAsync(req, "You are not allowed to browse refresh metadata.");
        }

        try
        {
            var models = await _selfServiceMetadataService.GetAllowedModelsAsync(context.CancellationToken);
            return await _responseService.CreateSuccessResponseAsync(req, new { user, models });
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "portal model discovery");
        }
    }

    [Function("DHRefreshAAS_PortalTables")]
    public async Task<HttpResponseData> PortalListTables(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
        FunctionContext context)
    {
        var user = _portalAuthService.GetPortalUser(req);
        if (user == null)
        {
            return await _responseService.CreateUnauthorizedResponseAsync(req, "Portal authentication is required.");
        }

        if (!_portalAuthService.CanReadMetadata(user))
        {
            return await _responseService.CreateForbiddenResponseAsync(req, "You are not allowed to browse refresh metadata.");
        }

        var query = HttpUtility.ParseQueryString(req.Url.Query);
        var databaseName = query["databaseName"];
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return await _responseService.CreateBadRequestResponseAsync(req, "Query parameter 'databaseName' is required.");
        }

        try
        {
            var tables = await _selfServiceMetadataService.GetAllowedTablesAsync(databaseName, context.CancellationToken);
            return await _responseService.CreateSuccessResponseAsync(req, new
            {
                databaseName,
                user,
                tables
            });
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "portal table discovery");
        }
    }

    [Function("DHRefreshAAS_PortalPartitions")]
    public async Task<HttpResponseData> PortalListPartitions(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
        FunctionContext context)
    {
        var user = _portalAuthService.GetPortalUser(req);
        if (user == null)
        {
            return await _responseService.CreateUnauthorizedResponseAsync(req, "Portal authentication is required.");
        }

        if (!_portalAuthService.CanReadMetadata(user))
        {
            return await _responseService.CreateForbiddenResponseAsync(req, "You are not allowed to browse refresh metadata.");
        }

        var query = HttpUtility.ParseQueryString(req.Url.Query);
        var databaseName = query["databaseName"];
        var tableName = query["tableName"];
        if (string.IsNullOrWhiteSpace(databaseName) || string.IsNullOrWhiteSpace(tableName))
        {
            return await _responseService.CreateBadRequestResponseAsync(req, "Query parameters 'databaseName' and 'tableName' are required.");
        }

        try
        {
            var partitions = await _selfServiceMetadataService.GetAllowedPartitionsAsync(databaseName, tableName, context.CancellationToken);
            if (partitions == null)
            {
                return await _responseService.CreateNotFoundResponseAsync(req, "Allowed table", $"{databaseName}/{tableName}");
            }

            return await _responseService.CreateSuccessResponseAsync(req, new { user, data = partitions });
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "portal partition discovery");
        }
    }

    [Function("DHRefreshAAS_PortalSubmitRefresh")]
    public async Task<HttpResponseData> PortalSubmitRefresh(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,
        FunctionContext context)
    {
        var user = _portalAuthService.GetPortalUser(req);
        if (user == null)
        {
            return await _responseService.CreateUnauthorizedResponseAsync(req, "Portal authentication is required.");
        }

        if (!_portalAuthService.CanSubmitRefresh(user))
        {
            return await _responseService.CreateForbiddenResponseAsync(req, "You are not allowed to submit refreshes.");
        }

        try
        {
            string body;
            using (var reader = new StreamReader(req.Body))
            {
                body = await reader.ReadToEndAsync();
            }

            if (string.IsNullOrWhiteSpace(body))
            {
                return await _responseService.CreateBadRequestResponseAsync(req, "Request body is required.");
            }

            var portalRequest = JsonSerializer.Deserialize<PortalRefreshRequest>(body, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
            var requestData = _requestProcessing.ValidateRequestData(portalRequest?.ToPostData());
            if (requestData == null)
            {
                return await _responseService.CreateBadRequestResponseAsync(req, "Invalid refresh request.");
            }

            var validation = await _selfServiceMetadataService.ValidateRefreshRequestAsync(requestData, context.CancellationToken);
            if (!validation.IsAllowed)
            {
                return await _responseService.CreateForbiddenResponseAsync(req, validation.Message);
            }

            var enhancedRequestData = _requestProcessing.CreateEnhancedRequestData(requestData, _config);
            var estimatedDurationMinutes = _requestProcessing.EstimateOperationDuration(requestData);
            _logger.LogInformation(
                "Portal refresh requested by {UserEmail} for database {DatabaseName}",
                user.Email,
                requestData.DatabaseName);

            var queueResult = await _queueExecution.StartAsyncOperationAsync(
                requestData, enhancedRequestData, estimatedDurationMinutes,
                user, "portal");

            return await _responseService.CreateAcceptedResponseAsync(
                req,
                queueResult.OperationId,
                queueResult.EstimatedDurationMinutes,
                queueResult.Status,
                queueResult.Message,
                queueResult.QueuePosition,
                queueResult.QueueScope,
                "/api/DHRefreshAAS_PortalStatus");
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "portal refresh submission");
        }
    }

    [Function("DHRefreshAAS_PortalStatus")]
    public async Task<HttpResponseData> PortalStatus(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
        FunctionContext context)
    {
        var user = _portalAuthService.GetPortalUser(req);
        if (user == null)
        {
            return await _responseService.CreateUnauthorizedResponseAsync(req, "Portal authentication is required.");
        }

        if (!_portalAuthService.CanReadMetadata(user))
        {
            return await _responseService.CreateForbiddenResponseAsync(req, "You are not allowed to view refresh status.");
        }

        try
        {
            var query = HttpUtility.ParseQueryString(req.Url.Query);
            var operationId = query["operationId"];

            if (!string.IsNullOrWhiteSpace(operationId))
            {
                return await _statusResponseBuilder.GetSpecificOperationStatusAsync(operationId, req, user, true);
            }

            return await _statusResponseBuilder.GetGeneralStatusAsync(req, user, true);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "portal status check");
        }
    }
}
