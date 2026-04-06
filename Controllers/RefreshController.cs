using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;
using DHRefreshAAS.Enums;

namespace DHRefreshAAS.Controllers;

/// <summary>
/// Core refresh endpoints: start, status, scaling, and slow-table formatting.
/// </summary>
public class RefreshController
{
    private readonly IConfigurationService _config;
    private readonly IConnectionService _connectionService;
    private readonly IAasScalingService _scalingService;
    private readonly IElasticPoolScalingService _elasticPoolScalingService;
    private readonly IOperationStorageService _operationStorage;
    private readonly IRequestProcessingService _requestProcessing;
    private readonly IResponseService _responseService;
    private readonly IErrorHandlingService _errorHandling;
    private readonly IQueueExecutionService _queueExecution;
    private readonly IStatusResponseBuilder _statusResponseBuilder;
    private readonly ILogger<RefreshController> _logger;

    public RefreshController(
        IConfigurationService config,
        IConnectionService connectionService,
        IAasScalingService scalingService,
        IElasticPoolScalingService elasticPoolScalingService,
        IOperationStorageService operationStorage,
        IRequestProcessingService requestProcessing,
        IResponseService responseService,
        IErrorHandlingService errorHandling,
        IQueueExecutionService queueExecution,
        IStatusResponseBuilder statusResponseBuilder,
        ILogger<RefreshController> logger)
    {
        _config = config;
        _connectionService = connectionService;
        _scalingService = scalingService;
        _elasticPoolScalingService = elasticPoolScalingService;
        _operationStorage = operationStorage;
        _requestProcessing = requestProcessing;
        _responseService = responseService;
        _errorHandling = errorHandling;
        _queueExecution = queueExecution;
        _statusResponseBuilder = statusResponseBuilder;
        _logger = logger;
    }

    /// <summary>
    /// Main HTTP trigger function that handles AAS refresh requests.
    /// </summary>
    [Function("DHRefreshAAS_HttpStart")]
    public async Task<HttpResponseData> HttpStart(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("AAS refresh HTTP trigger initiated.");

        try
        {
            var requestData = await _requestProcessing.ParseAndValidateRequestAsync(req);
            if (requestData == null)
            {
                return await _errorHandling.CreateValidationErrorResponseAsync(req, "Invalid request data");
            }

            var enhancedRequestData = _requestProcessing.CreateEnhancedRequestData(requestData, _config);
            var estimatedDurationMinutes = _requestProcessing.EstimateOperationDuration(requestData);
            _logger.LogInformation("Using async response pattern for operation tracking (estimated duration: {EstimatedDuration} minutes)", estimatedDurationMinutes);

            var queueResult = await _queueExecution.StartAsyncOperationAsync(
                requestData, enhancedRequestData, estimatedDurationMinutes);

            return await _responseService.CreateAcceptedResponseAsync(
                req,
                queueResult.OperationId,
                queueResult.EstimatedDurationMinutes,
                queueResult.Status,
                queueResult.Message,
                queueResult.QueuePosition,
                queueResult.QueueScope);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "AAS refresh");
        }
    }

    /// <summary>
    /// Enhanced status endpoint to check specific operations or all operations.
    /// </summary>
    [Function("DHRefreshAAS_Status")]
    public async Task<HttpResponseData> GetStatus(
        [HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("Status check requested");

        try
        {
            var query = HttpUtility.ParseQueryString(req.Url.Query);
            var operationId = query["operationId"];

            if (!string.IsNullOrEmpty(operationId))
            {
                return await _statusResponseBuilder.GetSpecificOperationStatusAsync(operationId, req);
            }

            return await _statusResponseBuilder.GetGeneralStatusAsync(req);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "status check");
        }
    }

    /// <summary>
    /// Scale up AAS and Elastic Pool before parallel refresh.
    /// Called by Logic App before For_each loop.
    /// </summary>
    [Function("DHRefreshAAS_ScaleUp")]
    public async Task<HttpResponseData> ScaleUp(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("Scale-up requested by orchestrator");

        var aasScaledUp = false;
        var elasticPoolScaledUp = false;
        var modelReady = false;

        try
        {
            // Scale Elastic Pool first (SQL needs capacity before AAS reads from it)
            try
            {
                elasticPoolScaledUp = await _elasticPoolScalingService.ScaleUpAsync(CancellationToken.None);
                _logger.LogInformation("Elastic Pool scale-up result: {ScaledUp}", elasticPoolScaledUp);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Elastic Pool scale-up failed");
            }

            // Scale AAS (scaling restarts the server and clears in-memory model data)
            try
            {
                aasScaledUp = await _scalingService.ScaleUpAsync(CancellationToken.None);
                _logger.LogInformation("AAS scale-up result: {ScaledUp}", aasScaledUp);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "AAS scale-up failed");
            }

            // Wait for model to reload into memory after scaling
            if (aasScaledUp)
            {
                _logger.LogInformation("AAS scaled up, waiting for model to reload into memory...");
                modelReady = await _connectionService.WaitForModelReadyAsync(CancellationToken.None);
                if (!modelReady)
                {
                    _logger.LogWarning("Model readiness check timed out — model may not be fully loaded");
                }
            }

            return await _responseService.CreateSuccessResponseAsync(req, new
            {
                aasScaledUp,
                elasticPoolScaledUp,
                modelReady
            }, HttpStatusCode.OK);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "scale-up");
        }
    }

    /// <summary>
    /// Scale down AAS and Elastic Pool after all refreshes complete.
    /// Called by Logic App after For_each loop.
    /// </summary>
    [Function("DHRefreshAAS_ScaleDown")]
    public async Task<HttpResponseData> ScaleDown(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("Scale-down requested by orchestrator");

        var aasScaledDown = false;
        var elasticPoolScaledDown = false;
        var skippedDueToRunningOps = false;

        try
        {
            // Check if there are still running operations before scaling down
            var runningOps = await _operationStorage.GetRunningOperationsAsync();

            // Filter out zombie operations (running longer than timeout) — don't let them block scale-down
            var zombieTimeout = TimeSpan.FromMinutes(_config.ZombieTimeoutMinutes);
            var activeOps = runningOps.Where(op => (DateTime.UtcNow - op.StartTime) < zombieTimeout).ToList();
            var zombieOps = runningOps.Where(op => (DateTime.UtcNow - op.StartTime) >= zombieTimeout).ToList();

            // Clean up zombie operations inline
            foreach (var zombie in zombieOps)
            {
                var age = (DateTime.UtcNow - zombie.StartTime).TotalMinutes;
                _logger.LogWarning("Marking zombie operation {OperationId} as failed (running for {Age:F0} min)", zombie.OperationId, age);
                await _operationStorage.MarkOperationAsFailedAsync(zombie.OperationId,
                    $"Operation terminated: marked as zombie during scale-down (running for {age:F0} min, exceeded {_config.ZombieTimeoutMinutes} min timeout)");
            }

            if (activeOps.Count > 0)
            {
                _logger.LogWarning(
                    "Scale-down SKIPPED: {RunningCount} active operation(s) still running. IDs: {OperationIds}",
                    activeOps.Count,
                    string.Join(", ", activeOps.Select(op => op.OperationId)));
                skippedDueToRunningOps = true;

                return await _responseService.CreateSuccessResponseAsync(req, new
                {
                    aasScaledDown,
                    elasticPoolScaledDown,
                    skippedDueToRunningOps,
                    runningOperationsCount = activeOps.Count,
                    zombiesCleaned = zombieOps.Count
                }, HttpStatusCode.OK);
            }

            // No running operations — safe to scale down
            try
            {
                await _scalingService.ScaleDownAsync(CancellationToken.None);
                aasScaledDown = true;
                _logger.LogInformation("AAS scale-down completed");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "CRITICAL: AAS scale-down failed! Manual intervention required.");
            }

            try
            {
                await _elasticPoolScalingService.ScaleDownAsync(CancellationToken.None);
                elasticPoolScaledDown = true;
                _logger.LogInformation("Elastic Pool scale-down completed");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "CRITICAL: Elastic Pool scale-down failed! Manual intervention required.");
            }

            return await _responseService.CreateSuccessResponseAsync(req, new
            {
                aasScaledDown,
                elasticPoolScaledDown,
                skippedDueToRunningOps
            }, HttpStatusCode.OK);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "scale-down");
        }
    }

    /// <summary>
    /// Builds per-database HTML for slow-table summary emails (used by Logic Apps).
    /// </summary>
    [Function("DHRefreshAAS_FormatSlowTablesHtml")]
    public async Task<HttpResponseData> FormatSlowTablesHtml(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        FunctionContext context)
    {
        _logger.LogInformation("Format slow tables HTML requested");

        try
        {
            string body;
            using (var reader = new StreamReader(req.Body))
            {
                body = await reader.ReadToEndAsync();
            }

            if (string.IsNullOrWhiteSpace(body))
            {
                var emptyHtml = SlowTablesHtmlFormatter.BuildHtml(null);
                return await _responseService.CreateSuccessResponseAsync(req, new { html = emptyHtml });
            }

            var parsed = JsonSerializer.Deserialize<FormatSlowTablesHtmlRequest>(body, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            var html = SlowTablesHtmlFormatter.BuildHtml(parsed?.Rows);
            return await _responseService.CreateSuccessResponseAsync(req, new { html });
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "format slow tables html");
        }
    }
}
