using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS.Services;

/// <summary>
/// Service for tracking operation progress
/// </summary>
public class ProgressTrackingService
{
    private readonly ILogger<ProgressTrackingService> _logger;

    public ProgressTrackingService(ILogger<ProgressTrackingService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Updates progress based on current state
    /// </summary>
    public virtual void UpdateProgress(OperationStatus operation)
    {
        var totalProcessed = operation.TablesCompleted + operation.TablesFailed;
        
        if (operation.TablesCount > 0)
        {
            if (operation.Status == OperationStatusEnum.Running && totalProcessed >= operation.TablesCount)
            {
                // All tables processed but SaveChanges still running
                operation.ProgressPercentage = 95.0;
                operation.CurrentPhase = OperationPhaseEnum.SavingChanges;
            }
            else
            {
                operation.ProgressPercentage = Math.Round((double)totalProcessed / operation.TablesCount * 100, 1);
            }
        }
        
        operation.TablesInProgress = operation.TablesCount - totalProcessed;
        
        // Update InProgressTables list to reflect actual remaining tables
        var processedTableNames = new HashSet<string>(operation.CompletedTables.Concat(operation.FailedTables));
        operation.InProgressTables = operation.InProgressTables.Where(table => !processedTableNames.Contains(table)).ToList();
        
        _logger.LogDebug("Progress updated for operation {OperationId}: {Progress}% ({Completed}/{Total} completed, {Failed} failed, {InProgress} in progress)", 
            operation.OperationId, operation.ProgressPercentage, operation.TablesCompleted, operation.TablesCount, 
            operation.TablesFailed, operation.TablesInProgress);
    }

    /// <summary>
    /// Marks a table as completed
    /// </summary>
    public virtual void CompleteTable(OperationStatus operation, string tableName)
    {
        operation.TablesCompleted++;
        operation.CompletedTables.Add(tableName);
        operation.InProgressTables.Remove(tableName);
        
        _logger.LogInformation("Table {TableName} completed for operation {OperationId}", tableName, operation.OperationId);
        UpdateProgress(operation);
    }

    /// <summary>
    /// Marks a table as failed
    /// </summary>
    public virtual void FailTable(OperationStatus operation, string tableName, string errorMessage)
    {
        operation.TablesFailed++;
        operation.FailedTables.Add($"{tableName}: {errorMessage}");
        operation.InProgressTables.Remove(tableName);
        
        _logger.LogWarning("Table {TableName} failed for operation {OperationId}: {ErrorMessage}", tableName, operation.OperationId, errorMessage);
        UpdateProgress(operation);
    }

    /// <summary>
    /// Marks that SaveChanges phase has started
    /// </summary>
    public virtual void StartSaveChanges(OperationStatus operation)
    {
        operation.CurrentPhase = OperationPhaseEnum.SavingChanges;
        _logger.LogInformation("Operation {OperationId} entering SaveChanges phase", operation.OperationId);
        UpdateProgress(operation);
    }

    /// <summary>
    /// Initializes progress tracking for a new operation
    /// </summary>
    public virtual void InitializeProgress(OperationStatus operation)
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
        
        UpdateProgress(operation);
        _logger.LogInformation("Progress tracking initialized for operation {OperationId} with {TablesCount} tables", 
            operation.OperationId, operation.TablesCount);
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
}
