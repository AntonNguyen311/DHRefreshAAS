using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;

namespace DHRefreshAAS.Services;

/// <summary>
/// Table entity for storing operation status in Azure Table Storage
/// </summary>
public class OperationEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "operations";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }
    
    // Operation data
    public string OperationId { get; set; } = "";
    public string Status { get; set; } = "";
    public DateTime EnqueuedTime { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string? Result { get; set; }
    public string? ErrorMessage { get; set; }
    public int TablesCount { get; set; }
    public double EstimatedDurationMinutes { get; set; }
    public string QueueScope { get; set; } = "";
    public string? RequestPayloadJson { get; set; }
    public string? LeaseOwner { get; set; }
    public DateTime? LeaseAcquiredTime { get; set; }
    public DateTime? LeaseHeartbeatTime { get; set; }
    
    // Progress tracking
    public int TablesCompleted { get; set; } = 0;
    public int TablesFailed { get; set; } = 0;
    public int TablesInProgress { get; set; } = 0;
    public double ProgressPercentage { get; set; } = 0.0;
    public string CompletedTablesJson { get; set; } = "[]";
    public string FailedTablesJson { get; set; } = "[]";
    public string InProgressTablesJson { get; set; } = "[]";
    public string CurrentPhase { get; set; } = "Initializing";
    public int? LastBatchIndex { get; set; }
    public string LastBatchTablesJson { get; set; } = "[]";
    public string? LastBatchError { get; set; }
    public string? LastBatchFailureCategory { get; set; }
    public string? LastBatchFailureSource { get; set; }
    public string? RequestedByUserId { get; set; }
    public string? RequestedByDisplayName { get; set; }
    public string? RequestedByEmail { get; set; }
    public string? RequestSource { get; set; }
    
    public OperationEntity() { }
    
    public OperationEntity(OperationStatus operation)
    {
        PartitionKey = "operations";
        RowKey = operation.OperationId;
        OperationId = operation.OperationId;
        Status = operation.Status;
        EnqueuedTime = operation.EnqueuedTime;
        StartTime = operation.StartTime;
        EndTime = operation.EndTime;
        Result = operation.Result;
        ErrorMessage = operation.ErrorMessage;
        TablesCount = operation.TablesCount;
        EstimatedDurationMinutes = operation.EstimatedDurationMinutes;
        QueueScope = operation.QueueScope;
        RequestPayloadJson = operation.RequestPayloadJson;
        LeaseOwner = operation.LeaseOwner;
        LeaseAcquiredTime = operation.LeaseAcquiredTime;
        LeaseHeartbeatTime = operation.LeaseHeartbeatTime;
        TablesCompleted = operation.TablesCompleted;
        TablesFailed = operation.TablesFailed;
        TablesInProgress = operation.TablesInProgress;
        ProgressPercentage = operation.ProgressPercentage;
        CompletedTablesJson = JsonSerializer.Serialize(operation.CompletedTables);
        FailedTablesJson = JsonSerializer.Serialize(operation.FailedTables);
        InProgressTablesJson = JsonSerializer.Serialize(operation.InProgressTables);
        CurrentPhase = operation.CurrentPhase;
        LastBatchIndex = operation.LastBatchIndex;
        LastBatchTablesJson = JsonSerializer.Serialize(operation.LastBatchTables);
        LastBatchError = operation.LastBatchError;
        LastBatchFailureCategory = operation.LastBatchFailureCategory;
        LastBatchFailureSource = operation.LastBatchFailureSource;
        RequestedByUserId = operation.RequestedByUserId;
        RequestedByDisplayName = operation.RequestedByDisplayName;
        RequestedByEmail = operation.RequestedByEmail;
        RequestSource = operation.RequestSource;
    }
    
    public OperationStatus ToOperationStatus()
    {
        return new OperationStatus
        {
            OperationId = OperationId,
            Status = Status,
            EnqueuedTime = EnqueuedTime,
            StartTime = StartTime,
            EndTime = EndTime,
            Result = Result,
            ErrorMessage = ErrorMessage,
            TablesCount = TablesCount,
            EstimatedDurationMinutes = EstimatedDurationMinutes,
            QueueScope = QueueScope,
            RequestPayloadJson = RequestPayloadJson,
            LeaseOwner = LeaseOwner,
            LeaseAcquiredTime = LeaseAcquiredTime,
            LeaseHeartbeatTime = LeaseHeartbeatTime,
            TablesCompleted = TablesCompleted,
            TablesFailed = TablesFailed,
            TablesInProgress = TablesInProgress,
            ProgressPercentage = ProgressPercentage,
            CompletedTables = JsonSerializer.Deserialize<List<string>>(CompletedTablesJson) ?? new List<string>(),
            FailedTables = JsonSerializer.Deserialize<List<string>>(FailedTablesJson) ?? new List<string>(),
            InProgressTables = JsonSerializer.Deserialize<List<string>>(InProgressTablesJson) ?? new List<string>(),
            CurrentPhase = CurrentPhase,
            LastBatchIndex = LastBatchIndex,
            LastBatchTables = JsonSerializer.Deserialize<List<string>>(LastBatchTablesJson) ?? new List<string>(),
            LastBatchError = LastBatchError,
            LastBatchFailureCategory = LastBatchFailureCategory,
            LastBatchFailureSource = LastBatchFailureSource,
            RequestedByUserId = RequestedByUserId,
            RequestedByDisplayName = RequestedByDisplayName,
            RequestedByEmail = RequestedByEmail,
            RequestSource = RequestSource
        };
    }
}

internal class QueueLeaseEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "queueleases";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public string QueueScope { get; set; } = "";
    public string LeaseOwner { get; set; } = "";
    public string? ActiveOperationId { get; set; }
    public DateTime LeaseAcquiredTime { get; set; }
    public DateTime LeaseHeartbeatTime { get; set; }
}

/// <summary>
/// Service for persistent operation storage using Azure Table Storage
/// </summary>
public class OperationStorageService
{
    private readonly TableClient _tableClient;
    private readonly ILogger<OperationStorageService> _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private volatile bool _tableInitialized;
    private const string TableName = "OperationStatus";
    private const string OperationPartitionKey = "operations";
    private const string QueueLeasePartitionKey = "queueleases";
    
    public OperationStorageService(ILogger<OperationStorageService> logger)
    {
        _logger = logger;
        
        // Get connection string from environment variables
        var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage") 
                             ?? Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING")
                             ?? "UseDevelopmentStorage=true"; // Fallback to local storage emulator
        
        _tableClient = new TableClient(connectionString, TableName);
    }

    public virtual Task InitializeAsync(CancellationToken cancellationToken = default) =>
        EnsureTableInitializedAsync(cancellationToken);

    private async Task EnsureTableInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (_tableInitialized)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_tableInitialized)
            {
                return;
            }

            await _tableClient.CreateIfNotExistsAsync(cancellationToken);
            _tableInitialized = true;
            _logger.LogInformation("Operation storage table initialized: {TableName}", TableName);
        }
        finally
        {
            _initLock.Release();
        }
    }
    
    /// <summary>
    /// Store or update an operation
    /// </summary>
    public virtual async Task<bool> UpsertOperationAsync(OperationStatus operation)
    {
        try
        {
            await EnsureTableInitializedAsync();
            var entity = new OperationEntity(operation);
            await _tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace);
            _logger.LogDebug("Operation {OperationId} stored successfully", operation.OperationId);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store operation {OperationId}: {ErrorMessage}", operation.OperationId, ex.Message);
            return false;
        }
    }
    
    /// <summary>
    /// Retrieve an operation by ID
    /// </summary>
    public virtual async Task<OperationStatus?> GetOperationAsync(string operationId)
    {
        try
        {
            await EnsureTableInitializedAsync();
            var response = await _tableClient.GetEntityAsync<OperationEntity>("operations", operationId);
            return response.Value.ToOperationStatus();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            _logger.LogDebug("Operation {OperationId} not found", operationId);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve operation {OperationId}: {ErrorMessage}", operationId, ex.Message);
            return null;
        }
    }
    
    /// <summary>
    /// Get recent operations (for general status endpoint)
    /// </summary>
    public virtual async Task<List<OperationStatus>> GetRecentOperationsAsync(int count = 10)
    {
        try
        {
            await EnsureTableInitializedAsync();
            var operations = new List<OperationStatus>();
            var windowStart = DateTimeOffset.UtcNow.AddDays(-7);
            var filter = $"PartitionKey eq 'operations' and Timestamp ge datetime'{windowStart.UtcDateTime:yyyy-MM-ddTHH:mm:ssZ}'";

            await foreach (var entity in _tableClient.QueryAsync<OperationEntity>(
                filter: filter,
                maxPerPage: count))
            {
                operations.Add(entity.ToOperationStatus());
            }
            
            return operations.OrderByDescending(op => op.StartTime).Take(count).ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve recent operations: {ErrorMessage}", ex.Message);
            return new List<OperationStatus>();
        }
    }
    
    /// <summary>
    /// Get all operations with "running" status
    /// </summary>
    public virtual async Task<List<OperationStatus>> GetRunningOperationsAsync()
    {
        try
        {
            await EnsureTableInitializedAsync();
            var operations = new List<OperationStatus>();

            await foreach (var entity in _tableClient.QueryAsync<OperationEntity>(
                filter: $"PartitionKey eq '{OperationPartitionKey}' and Status eq '{OperationStatusEnum.Running}'"))
            {
                operations.Add(entity.ToOperationStatus());
            }

            return operations;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve running operations: {ErrorMessage}", ex.Message);
            return new List<OperationStatus>();
        }
    }

    /// <summary>
    /// Get the count of currently running operations (across all queue scopes)
    /// </summary>
    public virtual async Task<int> GetRunningOperationCountAsync()
    {
        try
        {
            await EnsureTableInitializedAsync();
            var count = 0;
            await foreach (var _ in _tableClient.QueryAsync<OperationEntity>(
                filter: $"PartitionKey eq '{OperationPartitionKey}' and Status eq '{OperationStatusEnum.Running}'",
                select: new[] { "PartitionKey" }))
            {
                count++;
            }
            return count;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to count running operations: {ErrorMessage}", ex.Message);
            return 0;
        }
    }

    public virtual async Task<List<OperationStatus>> GetQueuedOperationsAsync(string queueScope)
    {
        try
        {
            await EnsureTableInitializedAsync();
            var operations = new List<OperationStatus>();

            await foreach (var entity in _tableClient.QueryAsync<OperationEntity>(
                filter: $"PartitionKey eq '{OperationPartitionKey}' and Status eq '{OperationStatusEnum.Queued}' and QueueScope eq '{queueScope}'"))
            {
                operations.Add(entity.ToOperationStatus());
            }

            return operations
                .OrderBy(op => op.EnqueuedTime)
                .ThenBy(op => op.OperationId, StringComparer.Ordinal)
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve queued operations for scope {QueueScope}: {ErrorMessage}", queueScope, ex.Message);
            return new List<OperationStatus>();
        }
    }

    public virtual async Task<int?> GetQueuePositionAsync(string operationId)
    {
        var operation = await GetOperationAsync(operationId);
        if (operation == null || !string.Equals(operation.Status, OperationStatusEnum.Queued, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var queuedOperations = await GetQueuedOperationsAsync(operation.QueueScope);
        var position = queuedOperations.FindIndex(op => string.Equals(op.OperationId, operationId, StringComparison.Ordinal));
        return position >= 0 ? position + 1 : null;
    }

    public virtual async Task<bool> TryAcquireQueueLeaseAsync(string queueScope, string leaseOwner, TimeSpan staleAfter, CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureTableInitializedAsync(cancellationToken);
            var now = DateTime.UtcNow;
            var entity = new QueueLeaseEntity
            {
                RowKey = queueScope,
                QueueScope = queueScope,
                LeaseOwner = leaseOwner,
                LeaseAcquiredTime = now,
                LeaseHeartbeatTime = now
            };

            await _tableClient.AddEntityAsync(entity, cancellationToken);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            try
            {
                var existing = await _tableClient.GetEntityAsync<QueueLeaseEntity>(QueueLeasePartitionKey, queueScope, cancellationToken: cancellationToken);
                var lease = existing.Value;
                if (!IsLeaseStale(lease, staleAfter))
                {
                    return false;
                }

                lease.LeaseOwner = leaseOwner;
                lease.LeaseAcquiredTime = DateTime.UtcNow;
                lease.LeaseHeartbeatTime = lease.LeaseAcquiredTime;
                lease.ActiveOperationId = null;

                await _tableClient.UpdateEntityAsync(lease, lease.ETag, TableUpdateMode.Replace, cancellationToken);
                return true;
            }
            catch (RequestFailedException updateEx) when (updateEx.Status == 404 || updateEx.Status == 412)
            {
                return false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to acquire queue lease for scope {QueueScope}: {ErrorMessage}", queueScope, ex.Message);
        }

        return false;
    }

    public virtual async Task<string?> TryPromoteNextQueuedOperationAsync(string queueScope, string leaseOwner, CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureTableInitializedAsync(cancellationToken);
            var leaseResponse = await _tableClient.GetEntityAsync<QueueLeaseEntity>(QueueLeasePartitionKey, queueScope, cancellationToken: cancellationToken);
            var lease = leaseResponse.Value;
            if (!string.Equals(lease.LeaseOwner, leaseOwner, StringComparison.Ordinal))
            {
                return null;
            }

            var candidates = new List<OperationEntity>();
            await foreach (var entity in _tableClient.QueryAsync<OperationEntity>(
                filter: $"PartitionKey eq '{OperationPartitionKey}' and Status eq '{OperationStatusEnum.Queued}' and QueueScope eq '{queueScope}'",
                cancellationToken: cancellationToken))
            {
                candidates.Add(entity);
            }

            var next = candidates
                .OrderBy(op => op.EnqueuedTime)
                .ThenBy(op => op.OperationId, StringComparer.Ordinal)
                .FirstOrDefault();

            if (next == null)
            {
                return null;
            }

            var now = DateTime.UtcNow;
            next.Status = OperationStatusEnum.Running;
            next.StartTime = now;
            next.EndTime = null;
            next.ErrorMessage = null;
            next.Result = null;
            next.LeaseOwner = leaseOwner;
            next.LeaseAcquiredTime = now;
            next.LeaseHeartbeatTime = now;
            next.CurrentPhase = OperationPhaseEnum.Initializing;
            next.TablesCompleted = 0;
            next.TablesFailed = 0;
            next.TablesInProgress = 0;
            next.ProgressPercentage = 0;
            next.CompletedTablesJson = "[]";
            next.FailedTablesJson = "[]";
            next.InProgressTablesJson = "[]";
            next.LastBatchIndex = null;
            next.LastBatchTablesJson = "[]";
            next.LastBatchError = null;
            next.LastBatchFailureCategory = null;
            next.LastBatchFailureSource = null;

            await _tableClient.UpdateEntityAsync(next, next.ETag, TableUpdateMode.Replace, cancellationToken);

            lease.ActiveOperationId = next.OperationId;
            lease.LeaseHeartbeatTime = now;
            await _tableClient.UpdateEntityAsync(lease, lease.ETag, TableUpdateMode.Replace, cancellationToken);

            return next.OperationId;
        }
        catch (RequestFailedException ex) when (ex.Status == 404 || ex.Status == 412)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to promote next queued operation for scope {QueueScope}: {ErrorMessage}", queueScope, ex.Message);
            return null;
        }
    }

    public virtual async Task<bool> RenewQueueLeaseAsync(string queueScope, string leaseOwner, string operationId, CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureTableInitializedAsync(cancellationToken);
            var leaseResponse = await _tableClient.GetEntityAsync<QueueLeaseEntity>(QueueLeasePartitionKey, queueScope, cancellationToken: cancellationToken);
            var lease = leaseResponse.Value;
            if (!string.Equals(lease.LeaseOwner, leaseOwner, StringComparison.Ordinal))
            {
                return false;
            }

            var now = DateTime.UtcNow;
            lease.ActiveOperationId = operationId;
            lease.LeaseHeartbeatTime = now;
            await _tableClient.UpdateEntityAsync(lease, lease.ETag, TableUpdateMode.Replace, cancellationToken);

            var operationResponse = await _tableClient.GetEntityAsync<OperationEntity>(OperationPartitionKey, operationId, cancellationToken: cancellationToken);
            var operation = operationResponse.Value;
            operation.LeaseHeartbeatTime = now;
            await _tableClient.UpdateEntityAsync(operation, operation.ETag, TableUpdateMode.Replace, cancellationToken);

            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 404 || ex.Status == 412)
        {
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to renew queue lease for operation {OperationId}: {ErrorMessage}", operationId, ex.Message);
            return false;
        }
    }

    public virtual async Task<bool> ReleaseQueueLeaseAsync(string queueScope, string leaseOwner, CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureTableInitializedAsync(cancellationToken);
            var leaseResponse = await _tableClient.GetEntityAsync<QueueLeaseEntity>(QueueLeasePartitionKey, queueScope, cancellationToken: cancellationToken);
            var lease = leaseResponse.Value;
            if (!string.Equals(lease.LeaseOwner, leaseOwner, StringComparison.Ordinal))
            {
                return false;
            }

            await _tableClient.DeleteEntityAsync(lease.PartitionKey, lease.RowKey, lease.ETag, cancellationToken);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 404 || ex.Status == 412)
        {
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to release queue lease for scope {QueueScope}: {ErrorMessage}", queueScope, ex.Message);
            return false;
        }
    }

    public virtual async Task<bool> ReleaseQueueLeaseForOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        var operation = await GetOperationAsync(operationId);
        if (operation == null || string.IsNullOrWhiteSpace(operation.QueueScope) || string.IsNullOrWhiteSpace(operation.LeaseOwner))
        {
            return false;
        }

        return await ReleaseQueueLeaseAsync(operation.QueueScope, operation.LeaseOwner, cancellationToken);
    }

    /// <summary>
    /// Mark an operation as failed with a given error message
    /// </summary>
    public virtual async Task<bool> MarkOperationAsFailedAsync(string operationId, string errorMessage)
    {
        try
        {
            var operation = await GetOperationAsync(operationId);
            if (operation == null) return false;

            operation.Status = OperationStatusEnum.Failed;
            operation.EndTime = DateTime.UtcNow;
            operation.ErrorMessage = errorMessage;
            operation.CurrentPhase = OperationPhaseEnum.Failed;
            return await UpsertOperationAsync(operation);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to mark operation {OperationId} as failed: {ErrorMessage}", operationId, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Get operation counts by status
    /// </summary>
    public virtual async Task<(int queued, int running, int completed, int failed, int total)> GetOperationCountsAsync()
    {
        try
        {
            await EnsureTableInitializedAsync();
            var queued = 0;
            var running = 0;
            var completed = 0;
            var failed = 0;
            var total = 0;
            
            await foreach (var entity in _tableClient.QueryAsync<OperationEntity>(
                filter: $"PartitionKey eq '{OperationPartitionKey}'",
                select: new[] { "Status" }))
            {
                total++;
                switch (entity.Status.ToLowerInvariant())
                {
                    case OperationStatusEnum.Queued:
                        queued++;
                        break;
                    case "running":
                        running++;
                        break;
                    case "completed":
                        completed++;
                        break;
                    case "failed":
                        failed++;
                        break;
                }
            }
            
            return (queued, running, completed, failed, total);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get operation counts: {ErrorMessage}", ex.Message);
            return (0, 0, 0, 0, 0);
        }
    }

    public virtual async Task<List<OperationStatus>> GetStaleRunningOperationsAsync(TimeSpan staleAfter, CancellationToken cancellationToken = default)
    {
        var runningOperations = await GetRunningOperationsAsync();
        var cutoff = DateTime.UtcNow - staleAfter;
        return runningOperations
            .Where(op => (op.LeaseHeartbeatTime ?? op.StartTime) < cutoff)
            .ToList();
    }

    private static bool IsLeaseStale(QueueLeaseEntity lease, TimeSpan staleAfter)
    {
        var lastHeartbeat = lease.LeaseHeartbeatTime == default ? lease.LeaseAcquiredTime : lease.LeaseHeartbeatTime;
        return lastHeartbeat < DateTime.UtcNow - staleAfter;
    }
}

