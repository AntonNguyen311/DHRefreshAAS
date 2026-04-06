using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Enums;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

/// <summary>
/// Manages the lifecycle of queued refresh operations: enqueue, lease acquisition,
/// background execution, heartbeat, and queue promotion.
/// </summary>
public class QueueExecutionService
{
    private readonly IConfigurationService _config;
    private readonly IAasRefreshService _aasRefreshService;
    private readonly IOperationStorageService _operationStorage;
    private readonly ProgressTrackingService _progressTracking;
    private readonly OperationCleanupService _cleanupService;
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private readonly ILogger<QueueExecutionService> _logger;

    public QueueExecutionService(
        IConfigurationService config,
        IAasRefreshService aasRefreshService,
        IOperationStorageService operationStorage,
        ProgressTrackingService progressTracking,
        OperationCleanupService cleanupService,
        IHostApplicationLifetime hostApplicationLifetime,
        ILogger<QueueExecutionService> logger)
    {
        _config = config;
        _aasRefreshService = aasRefreshService;
        _operationStorage = operationStorage;
        _progressTracking = progressTracking;
        _cleanupService = cleanupService;
        _hostApplicationLifetime = hostApplicationLifetime;
        _logger = logger;
    }

    /// <summary>
    /// Enqueues a refresh operation and attempts to start it immediately if the queue is free.
    /// Returns a result object describing queue position and whether execution started.
    /// </summary>
    public virtual async Task<QueueOperationResult> StartAsyncOperationAsync(
        PostData requestData,
        EnhancedPostData enhancedRequestData,
        int estimatedDurationMinutes,
        PortalUserContext? requester = null,
        string requestSource = "api")
    {
        var operationId = Guid.NewGuid().ToString();
        var queueScope = BuildQueueScope(requestData.DatabaseName);
        var enqueuedTime = DateTime.UtcNow;
        _logger.LogInformation("Queueing refresh operation {OperationId} in scope {QueueScope}", operationId, queueScope);

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
            RequestPayloadJson = JsonSerializer.Serialize(enhancedRequestData),
            RequestedByUserId = requester?.UserId,
            RequestedByDisplayName = requester?.DisplayName,
            RequestedByEmail = requester?.Email,
            RequestSource = requestSource
        };

        await _operationStorage.UpsertOperationAsync(operationStatus);

        var startedOperationId = await TryStartNextQueuedOperationAsync(queueScope);
        var queuePosition = await _operationStorage.GetQueuePositionAsync(operationId);
        var startedImmediately = string.Equals(startedOperationId, operationId, StringComparison.Ordinal);
        var acceptedMessage = startedImmediately
            ? "Refresh operation started in background. Use status endpoint to monitor progress."
            : "Refresh request queued behind another active refresh. Use status endpoint to monitor progress and queue position.";

        return new QueueOperationResult
        {
            OperationId = operationId,
            EstimatedDurationMinutes = estimatedDurationMinutes,
            StartedImmediately = startedImmediately,
            Status = startedImmediately ? OperationStatusEnum.Running : OperationStatusEnum.Queued,
            Message = acceptedMessage,
            QueuePosition = queuePosition,
            QueueScope = queueScope
        };
    }

    internal virtual void StartQueuedOperationExecution(string operationId)
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
                    _logger.LogWarning("Queued operation {OperationId} could not be loaded for execution", operationId);
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
                    leaseHeartbeatCts.Token);

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

                            _logger.LogInformation("Operation {OperationId} progress: {Completed}/{Total} tables completed ({Failed} failed, {InProgress} in progress)",
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
                            _logger.LogInformation(
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
                    op.Result = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
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

                _logger.LogInformation("Background refresh operation {OperationId} completed", operationId);
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

                _logger.LogError(ex, "Background refresh operation {OperationId} failed: {ErrorMessage}", operationId, ex.Message);
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
                    await TryStartNextQueuedOperationAsync(finalScope);
                }
            }
        });
    }

    internal virtual async Task<string?> TryStartNextQueuedOperationAsync(string queueScope)
    {
        var totalRunning = await _operationStorage.GetRunningOperationCountAsync();
        if (totalRunning >= _config.MaxConcurrentRefreshes)
        {
            _logger.LogInformation("Skipping queue promotion: {Running} operations already running (max {Max})", totalRunning, _config.MaxConcurrentRefreshes);
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

        _logger.LogInformation("Claimed queued refresh operation {OperationId} for execution", claimedOperationId);
        StartQueuedOperationExecution(claimedOperationId);
        return claimedOperationId;
    }

    internal virtual async Task StartQueueLeaseHeartbeatAsync(
        string queueScope,
        string? leaseOwner,
        string operationId,
        CancellationToken cancellationToken)
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
                _logger.LogWarning("Queue lease heartbeat could not be renewed for operation {OperationId}", operationId);
                return;
            }
        }
    }

    internal string BuildQueueScope(string? databaseName = null)
    {
        var scope = $"aas:{_config.AasServerName}";
        if (!string.IsNullOrWhiteSpace(databaseName))
            scope += $":{databaseName}";
        return scope.ToLowerInvariant();
    }

    internal TimeSpan GetQueueLeaseStaleAfter() =>
        TimeSpan.FromMinutes(Math.Max(_config.ZombieTimeoutMinutes, _config.OperationTimeoutMinutes) + 5);
}
