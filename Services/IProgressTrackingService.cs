using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IProgressTrackingService
{
    void UpdateProgress(OperationStatus operation);
    void CompleteTable(OperationStatus operation, string tableName);
    void FailTable(OperationStatus operation, string tableName, string errorMessage);
    void StartSaveChanges(OperationStatus operation);
    void InitializeProgress(OperationStatus operation);
    void RemoveOperation(string operationId);
    bool IsAllTablesProcessed(OperationStatus operation);
    bool ShouldBeCompleted(OperationStatus operation);
}
