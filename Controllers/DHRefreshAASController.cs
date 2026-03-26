using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;
using DHRefreshAAS.Enums;

namespace DHRefreshAAS.Controllers;

/// <summary>
/// Main controller for AAS refresh operations
/// </summary>
public class DHRefreshAASController
{
    private readonly ConfigurationService _config;
    private readonly ConnectionService _connectionService;
    private readonly AasRefreshService _aasRefreshService;
    private readonly OperationStorageService _operationStorage;
    private readonly ProgressTrackingService _progressTracking;
    private readonly ErrorHandlingService _errorHandling;
    private readonly RequestProcessingService _requestProcessing;
    private readonly ResponseService _responseService;
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private readonly ILogger<DHRefreshAASController> _logger;

    public DHRefreshAASController(
        ConfigurationService config,
        ConnectionService connectionService,
        AasRefreshService aasRefreshService,
        OperationStorageService operationStorage,
        ProgressTrackingService progressTracking,
        ErrorHandlingService errorHandling,
        RequestProcessingService requestProcessing,
        ResponseService responseService,
        IHostApplicationLifetime hostApplicationLifetime,
        ILogger<DHRefreshAASController> logger)
    {
        _config = config;
        _connectionService = connectionService;
        _aasRefreshService = aasRefreshService;
        _operationStorage = operationStorage;
        _progressTracking = progressTracking;
        _errorHandling = errorHandling;
        _requestProcessing = requestProcessing;
        _responseService = responseService;
        _hostApplicationLifetime = hostApplicationLifetime;
        _logger = logger;
    }

    /// <summary>
    /// Token acquisition test endpoint - validates Service Principal can get Azure AD tokens
    /// </summary>
    [Function("DHRefreshAAS_TestToken")]
    public async Task<HttpResponseData> TestToken(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req,
        FunctionContext context)
    {
        var logger = context.GetLogger<DHRefreshAASController>();
        logger.LogInformation("Azure AD token acquisition test initiated.");

        try
        {
            var testResult = await _connectionService.TestTokenAcquisitionAsync(context.CancellationToken);

            return await _responseService.CreateSuccessResponseAsync(req, testResult);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "token acquisition test");
        }
    }

    /// <summary>
    /// Connection test endpoint - tests AAS connection and returns detailed diagnostic information
    /// </summary>
    [Function("DHRefreshAAS_TestConnection")]
    public async Task<HttpResponseData> TestConnection(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req,
        FunctionContext context)
    {
        var logger = context.GetLogger<DHRefreshAASController>();
        logger.LogInformation("AAS connection test initiated.");

        try
        {
            var testResult = await _connectionService.TestConnectionAsync(context.CancellationToken);

            return await _responseService.CreateSuccessResponseAsync(req, testResult);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "AAS connection test");
        }
    }

    /// <summary>
    /// Main HTTP trigger function that handles AAS refresh requests
    /// </summary>
    [Function("DHRefreshAAS_HttpStart")]
    public async Task<HttpResponseData> HttpStart(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        FunctionContext context)
    {
        var logger = context.GetLogger<DHRefreshAASController>();
        logger.LogInformation("AAS refresh HTTP trigger initiated.");

        try
        {
            // Parse and validate request
            var requestData = await _requestProcessing.ParseAndValidateRequestAsync(req);
            if (requestData == null) 
            {
                return await _errorHandling.CreateValidationErrorResponseAsync(req, "Invalid request data");
            }

            // Create enhanced request data
            var enhancedRequestData = _requestProcessing.CreateEnhancedRequestData(requestData, _config);

            // Always use async response pattern to ensure operationId is generated for Logic App tracking
            var estimatedDurationMinutes = _requestProcessing.EstimateOperationDuration(requestData);
            logger.LogInformation("Using async response pattern for operation tracking (estimated duration: {EstimatedDuration} minutes)", estimatedDurationMinutes);
            
            return await StartAsyncOperation(requestData, enhancedRequestData, estimatedDurationMinutes, req, logger);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "AAS refresh");
        }
    }

    /// <summary>
    /// Enhanced status endpoint to check specific operations or all operations
    /// </summary>
    [Function("DHRefreshAAS_Status")]
    public async Task<HttpResponseData> GetStatus(
        [HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequestData req,
        FunctionContext context)
    {
        var logger = context.GetLogger<DHRefreshAASController>();
        logger.LogInformation("Status check requested");

        try 
        {
            var query = HttpUtility.ParseQueryString(req.Url.Query);
            var operationId = query["operationId"];
            
            if (!string.IsNullOrEmpty(operationId))
            {
                return await GetSpecificOperationStatus(operationId, req);
            }
            
            return await GetGeneralStatus(req);
        }
        catch (Exception ex)
        {
            return await _errorHandling.HandleExceptionAsync(req, ex, "status check");
        }
    }

    #region Private Helper Methods

    private async Task<HttpResponseData> StartAsyncOperation(
        PostData requestData, 
        EnhancedPostData enhancedRequestData, 
        int estimatedDurationMinutes, 
        HttpRequestData req, 
        ILogger logger)
    {
        var operationId = Guid.NewGuid().ToString();
        logger.LogInformation("Starting background refresh operation with ID: {OperationId}", operationId);
        
        // Track operation status
        var operationStatus = new OperationStatus
        {
            OperationId = operationId,
            Status = OperationStatusEnum.Running,
            StartTime = DateTime.UtcNow,
            TablesCount = requestData.RefreshObjects?.Length ?? 0,
            EstimatedDurationMinutes = estimatedDurationMinutes,
            RefreshObjects = requestData.RefreshObjects
        };
        
        // Initialize progress tracking
        _progressTracking.InitializeProgress(operationStatus);
        
        await _operationStorage.UpsertOperationAsync(operationStatus);
        
        // Start background task with progress tracking
        _ = Task.Run(async () =>
        {
            try
            {
                // Create progress callback to update operation status
                var progressCallback = new Action<string, bool, string>((tableName, isSuccess, errorMessage) =>
                {
                    _ = Task.Run(async () =>
                    {
                        var op = await _operationStorage.GetOperationAsync(operationId);
                        if (op != null)
                        {
                            if (isSuccess)
                            {
                                _progressTracking.CompleteTable(op, tableName);
                            }
                            else
                            {
                                _progressTracking.FailTable(op, tableName, errorMessage);
                            }
                            
                            await _operationStorage.UpsertOperationAsync(op);
                            
                            logger.LogInformation("Operation {OperationId} progress: {Completed}/{Total} tables completed ({Failed} failed, {InProgress} in progress)", 
                                operationId, op.TablesCompleted, op.TablesCount, op.TablesFailed, op.TablesInProgress);
                        }
                    });
                });
                
                // Create SaveChanges callback to update phase
                var saveChangesCallback = new Action(() =>
                {
                    _ = Task.Run(async () =>
                    {
                        var op = await _operationStorage.GetOperationAsync(operationId);
                        if (op != null)
                        {
                            _progressTracking.StartSaveChanges(op);
                            await _operationStorage.UpsertOperationAsync(op);
                            logger.LogInformation("Operation {OperationId} entering SaveChanges phase", operationId);
                        }
                    });
                });
                
                var stoppingToken = _hostApplicationLifetime.ApplicationStopping;
                var result = await _aasRefreshService.ExecuteRefreshWithRetryAsync(
                    enhancedRequestData,
                    stoppingToken,
                    progressCallback,
                    saveChangesCallback);
                
                var op = await _operationStorage.GetOperationAsync(operationId);
                if (op != null)
                {
                    op.Status = result.IsSuccess ? OperationStatusEnum.Completed : OperationStatusEnum.Failed;
                    op.CurrentPhase = result.IsSuccess ? OperationPhaseEnum.Completed : OperationPhaseEnum.Failed;
                    op.EndTime = DateTime.UtcNow;
                    op.Result = System.Text.Json.JsonSerializer.Serialize(result, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
                    if (!result.IsSuccess)
                    {
                        op.ErrorMessage = result.Message;
                    }
                    
                    // Final progress update - now we can show 100%
                    if (result.IsSuccess)
                    {
                        op.ProgressPercentage = 100.0;
                    }
                    _progressTracking.UpdateProgress(op);
                    
                    await _operationStorage.UpsertOperationAsync(op);
                }
                
                logger.LogInformation("Background refresh operation {OperationId} completed", operationId);
            }
            catch (Exception ex)
            {
                var op = await _operationStorage.GetOperationAsync(operationId);
                if (op != null)
                {
                    op.Status = OperationStatusEnum.Failed;
                    op.EndTime = DateTime.UtcNow;
                    op.ErrorMessage = ex.Message;
                    _progressTracking.UpdateProgress(op); // Final progress update
                    await _operationStorage.UpsertOperationAsync(op);
                }
                
                logger.LogError(ex, "Background refresh operation {OperationId} failed: {ErrorMessage}", operationId, ex.Message);
            }
        });
        
        return await _responseService.CreateAcceptedResponseAsync(req, operationId, estimatedDurationMinutes);
    }

    private async Task<HttpResponseData> GetSpecificOperationStatus(string operationId, HttpRequestData req)
    {
        var operation = await _operationStorage.GetOperationAsync(operationId);
        if (operation != null)
        {
            var elapsedMinutes = operation.EndTime.HasValue 
                ? (operation.EndTime.Value - operation.StartTime).TotalMinutes
                : (DateTime.UtcNow - operation.StartTime).TotalMinutes;
                
            // Update progress before returning
            _progressTracking.UpdateProgress(operation);
            
            var statusData = new
            {
                operationId = operation.OperationId,
                status = operation.Status,
                startTime = operation.StartTime,
                endTime = operation.EndTime,
                elapsedMinutes = Math.Round(elapsedMinutes, 2),
                estimatedDurationMinutes = operation.EstimatedDurationMinutes,
                tablesCount = operation.TablesCount,
                
                // Enhanced progress tracking
                progress = new {
                    percentage = operation.ProgressPercentage,
                    completed = operation.TablesCompleted,
                    failed = operation.TablesFailed,
                    inProgress = operation.TablesInProgress,
                    completedTables = operation.CompletedTables,
                    failedTables = operation.FailedTables,
                    inProgressTables = operation.InProgressTables,
                    currentPhase = operation.CurrentPhase
                },
                
                result = operation.Result,
                errorMessage = operation.ErrorMessage,
                isCompleted = operation.Status != OperationStatusEnum.Running
            };
            
            return await _responseService.CreateStatusResponseAsync(req, statusData);
        }
        
        return await _responseService.CreateNotFoundResponseAsync(req, "Operation", operationId);
    }

    private async Task<HttpResponseData> GetGeneralStatus(HttpRequestData req)
    {
        var recentOperations = await _operationStorage.GetRecentOperationsAsync(10);
        var operationCounts = await _operationStorage.GetOperationCountsAsync();
        
        var recentOperationsData = recentOperations.Select(op => {
            _progressTracking.UpdateProgress(op); // Update progress for each operation
            return new {
                operationId = op.OperationId,
                status = op.Status,
                startTime = op.StartTime,
                tablesCount = op.TablesCount,
                elapsedMinutes = op.EndTime.HasValue 
                    ? Math.Round((op.EndTime.Value - op.StartTime).TotalMinutes, 2)
                    : Math.Round((DateTime.UtcNow - op.StartTime).TotalMinutes, 2),
                progress = new {
                    percentage = op.ProgressPercentage,
                    completed = op.TablesCompleted,
                    failed = op.TablesFailed,
                    inProgress = op.TablesInProgress
                }
            };
        }).ToList();
            
        var statusData = new
        {
            timestamp = DateTime.UtcNow,
            status = "Function is running",
            totalOperations = operationCounts.total,
            runningOperations = operationCounts.running,
            completedOperations = operationCounts.completed,
            failedOperations = operationCounts.failed,
            recentOperations = recentOperationsData,
            endpoints = new {
                test_token = "/api/DHRefreshAAS_TestToken",
                test_connection = "/api/DHRefreshAAS_TestConnection", 
                refresh = "/api/DHRefreshAAS_HttpStart",
                status = "/api/DHRefreshAAS_Status",
                specific_status = "/api/DHRefreshAAS_Status?operationId=YOUR_OPERATION_ID"
            }
        };
        
        return await _responseService.CreateStatusResponseAsync(req, statusData);
    }

    #endregion
}
