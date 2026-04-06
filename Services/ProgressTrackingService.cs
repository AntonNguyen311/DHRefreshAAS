using System.Collections.Concurrent;
using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS.Services;

/// <summary>
/// Service for tracking operation progress.
/// All mutating methods are synchronized per-operation to protect List&lt;string&gt; properties
/// on OperationStatus from concurrent modification by parallel Task.Run callbacks.
/// </summary>
public class ProgressTrackingService : IProgressTrackingService
{
    private readonly ILogger<ProgressTrackingService> _logger;
    private readonly ConcurrentDictionary<string, object> _locks = new();

    public ProgressTrackingService(ILogger<ProgressTrackingService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Updates progress based on current state
    /// </summary>
    public virtual void UpdateProgress(OperationStatus operation)
    {
        var lockObj = _locks.GetOrAdd(operation.OperationId, _ => new object());
        lock (lockObj)
        {
            UpdateProgressInternal(operation);
        }
    }

    /// <summary>
    /// Marks a table as completed
    /// </summary>
    public virtual void CompleteTable(OperationStatus operation, string tableName)
    {
        var lockObj = _locks.GetOrAdd(operation.OperationId, _ => new object());
        lock (lockObj)
        {
            operation.TablesCompleted++;
            operation.CompletedTables.Add(tableName);
            operation.InProgressTables.Remove(tableName);

            _logger.LogInformation("Table {TableName} completed for operation {OperationId}", tableName, operation.OperationId);
            UpdateProgressInternal(operation);
        }
    }

    /// <summary>
    /// Marks a table as failed
    /// </summary>
    public virtual void FailTable(OperationStatus operation, string tableName, string errorMessage)
    {
        var lockObj = _locks.GetOrAdd(operation.OperationId, _ => new object());
        lock (lockObj)
        {
            operation.TablesFailed++;
            operation.FailedTables.Add($"{tableName}: {errorMessage}");
            operation.InProgressTables.Remove(tableName);

            _logger.LogWarning("Table {TableName} failed for operation {OperationId}: {ErrorMessage}", tableName, operation.OperationId, errorMessage);
            UpdateProgressInternal(operation);
        }
    }

    /// <summary>
    /// Marks that SaveChanges phase has started
    /// </summary>
    public virtual void StartSaveChanges(OperationStatus operation)
    {
        var lockObj = _locks.GetOrAdd(operation.OperationId, _ => new object());
        lock (lockObj)
        {
            operation.CurrentPhase = OperationPhaseEnum.SavingChanges;
            _logger.LogInformation("Operation {OperationId} entering SaveChanges phase", operation.OperationId);
            UpdateProgressInternal(operation);
        }
    }

    /// <summary>
    /// Initializes progress tracking for a new operation
    /// </summary>
    public virtual void InitializeProgress(OperationStatus operation)
    {
        var lockObj = _locks.GetOrAdd(operation.OperationId, _ => new object());
        lock (lockObj)
        {
            operation.CurrentPhase = OperationPhaseEnum.ProcessingTables;

            if (operation.RefreshObjects != null)
            {
                foreach (var refreshObj in operation.RefreshObjects)
                {
                    var tableName = refreshObj.Table ?? "Unknown Table";
                    operation.InProgressTables.Add(tableName);
                }
            }

            UpdateProgressInternal(operation);
            _logger.LogInformation("Progress tracking initialized for operation {OperationId} with {TablesCount} tables",
                operation.OperationId, operation.TablesCount);
        }
    }

    /// <summary>
    /// Removes the per-operation lock entry. Call when an operation reaches a terminal state.
    /// </summary>
    public void RemoveOperation(string operationId)
    {
        _locks.TryRemove(operationId, out _);
    }

    /// <summary>
    /// Checks if all tables are processed (completed or failed)
    /// </summary>
    public bool IsAllTablesProcessed(OperationStatus operation) =>
        (operation.TablesCompleted + operation.TablesFailed) >= operation.TablesCount;

    /// <summary>
    /// Checks if operation should be completed
    /// </summary>
    public bool ShouldBeCompleted(OperationStatus operation) =>
        IsAllTablesProcessed(operation) && operation.Status == OperationStatusEnum.Running;

    private void UpdateProgressInternal(OperationStatus operation)
    {
        var totalProcessed = operation.TablesCompleted + operation.TablesFailed;

        if (operation.TablesCount > 0)
        {
            if (operation.Status == OperationStatusEnum.Running && totalProcessed >= operation.TablesCount)
            {
                operation.ProgressPercentage = 95.0;
                operation.CurrentPhase = OperationPhaseEnum.SavingChanges;
            }
            else
            {
                operation.ProgressPercentage = Math.Round((double)totalProcessed / operation.TablesCount * 100, 1);
            }
        }

        operation.TablesInProgress = operation.TablesCount - totalProcessed;

        var processedTableNames = new HashSet<string>(operation.CompletedTables.Concat(operation.FailedTables));
        operation.InProgressTables = operation.InProgressTables.Where(table => !processedTableNames.Contains(table)).ToList();

        _logger.LogDebug("Progress updated for operation {OperationId}: {Progress}% ({Completed}/{Total} completed, {Failed} failed, {InProgress} in progress)",
            operation.OperationId, operation.ProgressPercentage, operation.TablesCompleted, operation.TablesCount,
            operation.TablesFailed, operation.TablesInProgress);
    }
}
