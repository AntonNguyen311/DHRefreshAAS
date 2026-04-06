using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Enums;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

/// <summary>
/// Builds HTTP status responses for operation monitoring.
/// Shared by RefreshController and PortalController.
/// </summary>
public class StatusResponseBuilder : IStatusResponseBuilder
{
    private readonly IOperationStorageService _operationStorage;
    private readonly IProgressTrackingService _progressTracking;
    private readonly IResponseService _responseService;
    private readonly ILogger<StatusResponseBuilder> _logger;

    public StatusResponseBuilder(
        IOperationStorageService operationStorage,
        IProgressTrackingService progressTracking,
        IResponseService responseService,
        ILogger<StatusResponseBuilder> logger)
    {
        _operationStorage = operationStorage;
        _progressTracking = progressTracking;
        _responseService = responseService;
        _logger = logger;
    }

    public virtual async Task<HttpResponseData> GetSpecificOperationStatusAsync(
        string operationId,
        HttpRequestData req,
        PortalUserContext? viewer = null,
        bool portalView = false)
    {
        var operation = await _operationStorage.GetOperationAsync(operationId);
        if (operation != null)
        {
            if (portalView &&
                viewer != null &&
                !viewer.IsAdmin &&
                !string.Equals(operation.RequestedByUserId, viewer.UserId, StringComparison.OrdinalIgnoreCase))
            {
                return await _responseService.CreateForbiddenResponseAsync(req, "You are not allowed to view this operation.");
            }

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
                queue = new
                {
                    scope = operation.QueueScope,
                    position = queuePosition,
                    leaseAcquiredTime = operation.LeaseAcquiredTime,
                    leaseHeartbeatTime = operation.LeaseHeartbeatTime
                },

                // Enhanced progress tracking
                progress = new
                {
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
                performanceWarnings = ParsePerformanceWarnings(operation.Result),
                requestedBy = new
                {
                    userId = operation.RequestedByUserId,
                    displayName = operation.RequestedByDisplayName,
                    email = operation.RequestedByEmail
                },
                requestSource = operation.RequestSource,
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

    public virtual async Task<HttpResponseData> GetGeneralStatusAsync(
        HttpRequestData req,
        PortalUserContext? viewer = null,
        bool portalView = false)
    {
        var recentOperations = await _operationStorage.GetRecentOperationsAsync(portalView ? 50 : 10);
        if (portalView && viewer != null && !viewer.IsAdmin)
        {
            recentOperations = recentOperations
                .Where(op => string.Equals(op.RequestedByUserId, viewer.UserId, StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        var operationCounts = await _operationStorage.GetOperationCountsAsync();
        var visibleCounts = portalView
            ? (
                queued: recentOperations.Count(op => string.Equals(op.Status, OperationStatusEnum.Queued, StringComparison.OrdinalIgnoreCase)),
                running: recentOperations.Count(op => string.Equals(op.Status, OperationStatusEnum.Running, StringComparison.OrdinalIgnoreCase)),
                completed: recentOperations.Count(op => string.Equals(op.Status, OperationStatusEnum.Completed, StringComparison.OrdinalIgnoreCase)),
                failed: recentOperations.Count(op => string.Equals(op.Status, OperationStatusEnum.Failed, StringComparison.OrdinalIgnoreCase)),
                total: recentOperations.Count)
            : operationCounts;

        var recentOperationsData = recentOperations.Select(op =>
        {
            _progressTracking.UpdateProgress(op); // Update progress for each operation
            var referenceStart = string.Equals(op.Status, OperationStatusEnum.Queued, StringComparison.OrdinalIgnoreCase)
                ? op.EnqueuedTime
                : op.StartTime;
            return new
            {
                operationId = op.OperationId,
                status = op.Status,
                enqueuedTime = op.EnqueuedTime,
                startTime = op.StartTime,
                tablesCount = op.TablesCount,
                elapsedMinutes = op.EndTime.HasValue
                    ? Math.Round((op.EndTime.Value - referenceStart).TotalMinutes, 2)
                    : Math.Round((DateTime.UtcNow - referenceStart).TotalMinutes, 2),
                queue = new
                {
                    scope = op.QueueScope,
                    leaseAcquiredTime = op.LeaseAcquiredTime
                },
                progress = new
                {
                    percentage = op.ProgressPercentage,
                    completed = op.TablesCompleted,
                    failed = op.TablesFailed,
                    inProgress = op.TablesInProgress
                },
                requestedBy = new
                {
                    userId = op.RequestedByUserId,
                    displayName = op.RequestedByDisplayName,
                    email = op.RequestedByEmail
                },
                requestSource = op.RequestSource
            };
        }).ToList();

        var statusData = new
        {
            timestamp = DateTime.UtcNow,
            status = "Function is running",
            totalOperations = visibleCounts.total,
            queuedOperations = visibleCounts.queued,
            runningOperations = visibleCounts.running,
            completedOperations = visibleCounts.completed,
            failedOperations = visibleCounts.failed,
            viewer,
            recentOperations = recentOperationsData,
            endpoints = new
            {
                test_token = "/api/DHRefreshAAS_TestToken",
                test_connection = "/api/DHRefreshAAS_TestConnection",
                refresh = "/api/DHRefreshAAS_HttpStart",
                status = "/api/DHRefreshAAS_Status",
                specific_status = "/api/DHRefreshAAS_Status?operationId=YOUR_OPERATION_ID",
                format_slow_tables_html = "/api/DHRefreshAAS_FormatSlowTablesHtml",
                portal_models = "/api/DHRefreshAAS_PortalModels",
                portal_tables = "/api/DHRefreshAAS_PortalTables?databaseName=YOUR_DATABASE",
                portal_partitions = "/api/DHRefreshAAS_PortalPartitions?databaseName=YOUR_DATABASE&tableName=YOUR_TABLE",
                portal_submit_refresh = "/api/DHRefreshAAS_PortalSubmitRefresh",
                portal_status = "/api/DHRefreshAAS_PortalStatus"
            }
        };

        return await _responseService.CreateStatusResponseAsync(req, statusData);
    }

    public object[]? ParseTopSlowTables(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson)) return null;
        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            if (!doc.RootElement.TryGetProperty("topSlowTables", out var slowTables))
                return null;

            var rows = new List<(double SortKey, object Row)>();
            foreach (var item in slowTables.EnumerateArray())
            {
                var secs = item.TryGetProperty("processingTimeSeconds", out var t) ? t.GetDouble() : 0;
                var row = new
                {
                    databaseName = item.TryGetProperty("databaseName", out var dn) ? dn.GetString() ?? "" : "",
                    tableName = item.TryGetProperty("tableName", out var tn) ? tn.GetString() ?? "" : "",
                    partitionName = item.TryGetProperty("partitionName", out var p) ? p.GetString() ?? "" : "",
                    processingTimeSeconds = Math.Round(secs, 1),
                    rowCount = item.TryGetProperty("rowCount", out var r) && r.ValueKind != JsonValueKind.Null ? r.GetInt64() : (long?)null,
                    severity = item.TryGetProperty("severity", out var s) ? s.GetString() : null
                };
                rows.Add((secs, row));
            }

            var ordered = rows.OrderByDescending(x => x.SortKey).Select(x => x.Row).ToArray();
            return ordered.Length > 0 ? ordered : null;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse topSlowTables from operation result JSON");
            return null;
        }
    }

    public object[]? ParsePerformanceWarnings(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson)) return null;
        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            if (!doc.RootElement.TryGetProperty("performanceWarnings", out var arr))
                return null;

            var rows = new List<(double SortKey, object Row)>();
            foreach (var item in arr.EnumerateArray())
            {
                var secs = item.TryGetProperty("processingTimeSeconds", out var t) ? t.GetDouble() : 0;
                var row = new
                {
                    databaseName = item.TryGetProperty("databaseName", out var dn) ? dn.GetString() ?? "" : "",
                    tableName = item.TryGetProperty("tableName", out var tn) ? tn.GetString() ?? "" : "",
                    partitionName = item.TryGetProperty("partitionName", out var p) ? p.GetString() ?? "" : "",
                    processingTimeSeconds = Math.Round(secs, 1),
                    severity = item.TryGetProperty("severity", out var s) ? s.GetString() ?? "" : ""
                };
                rows.Add((secs, row));
            }

            var ordered = rows.OrderByDescending(x => x.SortKey).Select(x => x.Row).ToArray();
            return ordered.Length > 0 ? ordered : null;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse performanceWarnings from operation result JSON");
            return null;
        }
    }
}
