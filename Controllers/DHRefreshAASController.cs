using System;
using System.Linq;
using System.Net;
using System.Text.Json;
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
    private readonly AasScalingService _scalingService;
    private readonly ElasticPoolScalingService _elasticPoolScalingService;
    private readonly OperationStorageService _operationStorage;
    private readonly ProgressTrackingService _progressTracking;
    private readonly ErrorHandlingService _errorHandling;
    private readonly RequestProcessingService _requestProcessing;
    private readonly ResponseService _responseService;
    private readonly OperationCleanupService _cleanupService;
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private readonly ILogger<DHRefreshAASController> _logger;

    public DHRefreshAASController(
        ConfigurationService config,
        ConnectionService connectionService,
        AasRefreshService aasRefreshService,
        AasScalingService scalingService,
        ElasticPoolScalingService elasticPoolScalingService,
        OperationStorageService operationStorage,
        ProgressTrackingService progressTracking,
        ErrorHandlingService errorHandling,
        RequestProcessingService requestProcessing,
        ResponseService responseService,
        OperationCleanupService cleanupService,
        IHostApplicationLifetime hostApplicationLifetime,
        ILogger<DHRefreshAASController> logger)
    {
        _config = config;
        _connectionService = connectionService;
        _aasRefreshService = aasRefreshService;
        _scalingService = scalingService;
        _elasticPoolScalingService = elasticPoolScalingService;
        _operationStorage = operationStorage;
        _progressTracking = progressTracking;
        _errorHandling = errorHandling;
        _requestProcessing = requestProcessing;
        _responseService = responseService;
        _cleanupService = cleanupService;
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

    /// <summary>
    /// Scale up AAS and Elastic Pool before parallel refresh.
    /// Called by Logic App before For_each loop.
    /// </summary>
    [Function("DHRefreshAAS_ScaleUp")]
    public async Task<HttpResponseData> ScaleUp(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        FunctionContext context)
    {
        var logger = context.GetLogger<DHRefreshAASController>();
        logger.LogInformation("Scale-up requested by orchestrator");

        var aasScaledUp = false;
        var elasticPoolScaledUp = false;
        var modelReady = false;

        try
        {
            // Scale Elastic Pool first (SQL needs capacity before AAS reads from it)
            try
            {
                elasticPoolScaledUp = await _elasticPoolScalingService.ScaleUpAsync(CancellationToken.None);
                logger.LogInformation("Elastic Pool scale-up result: {ScaledUp}", elasticPoolScaledUp);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Elastic Pool scale-up failed");
            }

            // Scale AAS (scaling restarts the server and clears in-memory model data)
            try
            {
                aasScaledUp = await _scalingService.ScaleUpAsync(CancellationToken.None);
                logger.LogInformation("AAS scale-up result: {ScaledUp}", aasScaledUp);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "AAS scale-up failed");
            }

            // Wait for model to reload into memory after scaling
            if (aasScaledUp)
            {
                logger.LogInformation("AAS scaled up, waiting for model to reload into memory...");
                modelReady = await _connectionService.WaitForModelReadyAsync(CancellationToken.None);
                if (!modelReady)
                {
                    logger.LogWarning("Model readiness check timed out — model may not be fully loaded");
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
        var logger = context.GetLogger<DHRefreshAASController>();
        logger.LogInformation("Scale-down requested by orchestrator");

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
                logger.LogWarning("Marking zombie operation {OperationId} as failed (running for {Age:F0} min)", zombie.OperationId, age);
                await _operationStorage.MarkOperationAsFailedAsync(zombie.OperationId,
                    $"Operation terminated: marked as zombie during scale-down (running for {age:F0} min, exceeded {_config.ZombieTimeoutMinutes} min timeout)");
            }

            if (activeOps.Count > 0)
            {
                logger.LogWarning(
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
            // Scale AAS down first
            try
            {
                await _scalingService.ScaleDownAsync(CancellationToken.None);
                aasScaledDown = true;
                logger.LogInformation("AAS scale-down completed");
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "CRITICAL: AAS scale-down failed! Manual intervention required.");
            }

            // Scale Elastic Pool down
            try
            {
                await _elasticPoolScalingService.ScaleDownAsync(CancellationToken.None);
                elasticPoolScaledDown = true;
                logger.LogInformation("Elastic Pool scale-down completed");
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "CRITICAL: Elastic Pool scale-down failed! Manual intervention required.");
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

    #region Private Helper Methods

    private async Task<HttpResponseData> StartAsyncOperation(
        PostData requestData, 
        EnhancedPostData enhancedRequestData, 
        int estimatedDurationMinutes, 
        HttpRequestData req, 
        ILogger logger)
    {
        var operationId = Guid.NewGuid().ToString();
        var queueScope = BuildQueueScope(requestData.DatabaseName);
        var enqueuedTime = DateTime.UtcNow;
        logger.LogInformation("Queueing refresh operation {OperationId} in scope {QueueScope}", operationId, queueScope);
        
        var operationStatus = new OperationStatus
        {
            OperationId = operationId,
            Status = OperationStatusEnum.Queued,
            EnqueuedTime = enqueuedTime,
            StartTime = enqueuedTime,
            TablesCount = requestData.RefreshObjects?.Length ?? 0,
            EstimatedDurationMinutes = estimatedDurationMinutes,
            RefreshObjects = requestData.RefreshObjects,
            QueueScope = queueScope,
            CurrentPhase = OperationPhaseEnum.Queued,
            RequestPayloadJson = JsonSerializer.Serialize(enhancedRequestData)
        };

        await _operationStorage.UpsertOperationAsync(operationStatus);

        var startedOperationId = await TryStartNextQueuedOperationAsync(queueScope, logger);
        var queuePosition = await _operationStorage.GetQueuePositionAsync(operationId);
        var startedImmediately = string.Equals(startedOperationId, operationId, StringComparison.Ordinal);
        var acceptedMessage = startedImmediately
            ? "Refresh operation started in background. Use status endpoint to monitor progress."
            : "Refresh request queued behind another active refresh. Use status endpoint to monitor progress and queue position.";

        return await _responseService.CreateAcceptedResponseAsync(
            req,
            operationId,
            estimatedDurationMinutes,
            startedImmediately ? OperationStatusEnum.Running : OperationStatusEnum.Queued,
            acceptedMessage,
            queuePosition,
            queueScope);
    }

    private void StartQueuedOperationExecution(string operationId, ILogger logger)
    {
        _cleanupService.TrackOperation(operationId);
        _ = Task.Run(async () =>
        {
            var leaseHeartbeatCts = new CancellationTokenSource();
            Task? heartbeatTask = null;
            OperationStatus? operation = null;
            try
            {
                operation = await _operationStorage.GetOperationAsync(operationId);
                if (operation == null)
                {
                    logger.LogWarning("Queued operation {OperationId} could not be loaded for execution", operationId);
                    return;
                }

                if (string.IsNullOrWhiteSpace(operation.RequestPayloadJson))
                {
                    await _operationStorage.MarkOperationAsFailedAsync(operationId, "Queued request payload missing; cannot execute refresh.");
                    return;
                }

                var enhancedRequestData = JsonSerializer.Deserialize<EnhancedPostData>(operation.RequestPayloadJson);
                if (enhancedRequestData?.OriginalRequest == null)
                {
                    await _operationStorage.MarkOperationAsFailedAsync(operationId, "Queued request payload is invalid; cannot execute refresh.");
                    return;
                }

                operation.RefreshObjects = enhancedRequestData.OriginalRequest.RefreshObjects;
                operation.CurrentPhase = OperationPhaseEnum.Initializing;
                _progressTracking.InitializeProgress(operation);
                await _operationStorage.UpsertOperationAsync(operation);

                heartbeatTask = StartQueueLeaseHeartbeatAsync(
                    operation.QueueScope,
                    operation.LeaseOwner,
                    operation.OperationId,
                    leaseHeartbeatCts.Token,
                    logger);

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
                
                // Track the batch currently entering SaveChanges so zombie operations keep useful context.
                var saveChangesCallback = new Action<SaveChangesDiagnostic>(diagnostic =>
                {
                    _ = Task.Run(async () =>
                    {
                        var op = await _operationStorage.GetOperationAsync(operationId);
                        if (op != null)
                        {
                            _progressTracking.StartSaveChanges(op);
                            op.LastBatchIndex = diagnostic.BatchIndex;
                            op.LastBatchTables = diagnostic.Tables.ToList();
                            op.LastBatchError = diagnostic.ErrorMessage;
                            op.LastBatchFailureCategory = diagnostic.FailureCategory;
                            op.LastBatchFailureSource = diagnostic.FailureSource;
                            await _operationStorage.UpsertOperationAsync(op);
                            logger.LogInformation(
                                "Operation {OperationId} entering SaveChanges phase for batch {BatchIndex}/{TotalBatches}: {Tables}",
                                operationId,
                                diagnostic.BatchIndex,
                                diagnostic.TotalBatches,
                                string.Join(", ", diagnostic.Tables));
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
                    if (result.LastBatchDiagnostic != null)
                    {
                        op.LastBatchIndex = result.LastBatchDiagnostic.BatchIndex;
                        op.LastBatchTables = result.LastBatchDiagnostic.Tables.ToList();
                        op.LastBatchError = result.LastBatchDiagnostic.ErrorMessage;
                        op.LastBatchFailureCategory = result.LastBatchDiagnostic.FailureCategory;
                        op.LastBatchFailureSource = result.LastBatchDiagnostic.FailureSource;
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
            finally
            {
                leaseHeartbeatCts.Cancel();
                if (heartbeatTask != null)
                {
                    try
                    {
                        await heartbeatTask;
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }

                if (operation != null)
                {
                    await _operationStorage.ReleaseQueueLeaseForOperationAsync(operation.OperationId);
                }

                _cleanupService.UntrackOperation(operationId);

                var finalScope = operation?.QueueScope;
                if (!string.IsNullOrWhiteSpace(finalScope))
                {
                    await TryStartNextQueuedOperationAsync(finalScope, logger);
                }
            }
        });
    }

    private async Task<string?> TryStartNextQueuedOperationAsync(string queueScope, ILogger logger)
    {
        var totalRunning = await _operationStorage.GetRunningOperationCountAsync();
        if (totalRunning >= _config.MaxConcurrentRefreshes)
        {
            logger.LogInformation("Skipping queue promotion: {Running} operations already running (max {Max})", totalRunning, _config.MaxConcurrentRefreshes);
            return null;
        }

        var leaseOwner = Guid.NewGuid().ToString("N");
        if (!await _operationStorage.TryAcquireQueueLeaseAsync(queueScope, leaseOwner, GetQueueLeaseStaleAfter()))
        {
            return null;
        }

        var claimedOperationId = await _operationStorage.TryPromoteNextQueuedOperationAsync(queueScope, leaseOwner);
        if (string.IsNullOrWhiteSpace(claimedOperationId))
        {
            await _operationStorage.ReleaseQueueLeaseAsync(queueScope, leaseOwner);
            return null;
        }

        logger.LogInformation("Claimed queued refresh operation {OperationId} for execution", claimedOperationId);
        StartQueuedOperationExecution(claimedOperationId, logger);
        return claimedOperationId;
    }

    private async Task StartQueueLeaseHeartbeatAsync(
        string queueScope,
        string? leaseOwner,
        string operationId,
        CancellationToken cancellationToken,
        ILogger logger)
    {
        if (string.IsNullOrWhiteSpace(leaseOwner))
        {
            return;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(Math.Max(5, _config.HeartbeatIntervalSeconds)), cancellationToken);
            var renewed = await _operationStorage.RenewQueueLeaseAsync(queueScope, leaseOwner, operationId, cancellationToken);
            if (!renewed)
            {
                logger.LogWarning("Queue lease heartbeat could not be renewed for operation {OperationId}", operationId);
                return;
            }
        }
    }

    private string BuildQueueScope(string? databaseName = null)
    {
        var scope = $"aas:{_config.AasServerName}";
        if (!string.IsNullOrWhiteSpace(databaseName))
            scope += $":{databaseName}";
        return scope.ToLowerInvariant();
    }

    private TimeSpan GetQueueLeaseStaleAfter() =>
        TimeSpan.FromMinutes(Math.Max(_config.ZombieTimeoutMinutes, _config.OperationTimeoutMinutes) + 5);

    private async Task<HttpResponseData> GetSpecificOperationStatus(string operationId, HttpRequestData req)
    {
        var operation = await _operationStorage.GetOperationAsync(operationId);
        if (operation != null)
        {
            var referenceStart = string.Equals(operation.Status, OperationStatusEnum.Queued, StringComparison.OrdinalIgnoreCase)
                ? operation.EnqueuedTime
                : operation.StartTime;
            var elapsedMinutes = operation.EndTime.HasValue 
                ? (operation.EndTime.Value - referenceStart).TotalMinutes
                : (DateTime.UtcNow - referenceStart).TotalMinutes;
                
            // Update progress before returning
            _progressTracking.UpdateProgress(operation);
            var queuePosition = await _operationStorage.GetQueuePositionAsync(operationId);
            
            var statusData = new
            {
                operationId = operation.OperationId,
                status = operation.Status,
                enqueuedTime = operation.EnqueuedTime,
                startTime = operation.StartTime,
                endTime = operation.EndTime,
                elapsedMinutes = Math.Round(elapsedMinutes, 2),
                estimatedDurationMinutes = operation.EstimatedDurationMinutes,
                tablesCount = operation.TablesCount,
                queue = new {
                    scope = operation.QueueScope,
                    position = queuePosition,
                    leaseAcquiredTime = operation.LeaseAcquiredTime,
                    leaseHeartbeatTime = operation.LeaseHeartbeatTime
                },
                
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
                topSlowTables = ParseTopSlowTables(operation.Result),
                lastBatch = operation.LastBatchIndex.HasValue
                    ? new
                    {
                        index = operation.LastBatchIndex,
                        tables = operation.LastBatchTables,
                        error = operation.LastBatchError,
                        failureCategory = operation.LastBatchFailureCategory,
                        failureSource = operation.LastBatchFailureSource
                    }
                    : null,
                errorMessage = operation.ErrorMessage,
                isCompleted = operation.Status == OperationStatusEnum.Completed || operation.Status == OperationStatusEnum.Failed
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
            var referenceStart = string.Equals(op.Status, OperationStatusEnum.Queued, StringComparison.OrdinalIgnoreCase)
                ? op.EnqueuedTime
                : op.StartTime;
            return new {
                operationId = op.OperationId,
                status = op.Status,
                enqueuedTime = op.EnqueuedTime,
                startTime = op.StartTime,
                tablesCount = op.TablesCount,
                elapsedMinutes = op.EndTime.HasValue 
                    ? Math.Round((op.EndTime.Value - referenceStart).TotalMinutes, 2)
                    : Math.Round((DateTime.UtcNow - referenceStart).TotalMinutes, 2),
                queue = new {
                    scope = op.QueueScope,
                    leaseAcquiredTime = op.LeaseAcquiredTime
                },
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
            queuedOperations = operationCounts.queued,
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

    private static object[]? ParseTopSlowTables(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson)) return null;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(resultJson);
            if (!doc.RootElement.TryGetProperty("topSlowTables", out var slowTables))
                return null;

            var list = new List<object>();
            foreach (var item in slowTables.EnumerateArray())
            {
                list.Add(new
                {
                    tableName = item.TryGetProperty("tableName", out var tn) ? tn.GetString() ?? "" : "",
                    partitionName = item.TryGetProperty("partitionName", out var p) ? p.GetString() ?? "" : "",
                    processingTimeSeconds = item.TryGetProperty("processingTimeSeconds", out var t) ? Math.Round(t.GetDouble(), 1) : 0,
                    rowCount = item.TryGetProperty("rowCount", out var r) && r.ValueKind != System.Text.Json.JsonValueKind.Null ? r.GetInt64() : (long?)null
                });
            }
            return list.Count > 0 ? list.ToArray() : null;
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }

    #endregion
}
