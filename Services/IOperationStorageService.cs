using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface IOperationStorageService
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
    Task<bool> UpsertOperationAsync(OperationStatus operation);
    Task<OperationStatus?> GetOperationAsync(string operationId);
    Task<List<OperationStatus>> GetRecentOperationsAsync(int count = 10);
    Task<List<OperationStatus>> GetRunningOperationsAsync();
    Task<int> GetRunningOperationCountAsync();
    Task<List<OperationStatus>> GetQueuedOperationsAsync(string queueScope);
    Task<int?> GetQueuePositionAsync(string operationId);
    Task<bool> TryAcquireQueueLeaseAsync(string queueScope, string leaseOwner, TimeSpan staleAfter, CancellationToken cancellationToken = default);
    Task<string?> TryPromoteNextQueuedOperationAsync(string queueScope, string leaseOwner, CancellationToken cancellationToken = default);
    Task<bool> RenewQueueLeaseAsync(string queueScope, string leaseOwner, string operationId, CancellationToken cancellationToken = default);
    Task<bool> ReleaseQueueLeaseAsync(string queueScope, string leaseOwner, CancellationToken cancellationToken = default);
    Task<bool> ReleaseQueueLeaseForOperationAsync(string operationId, CancellationToken cancellationToken = default);
    Task<bool> MarkOperationAsFailedAsync(string operationId, string errorMessage);
    Task<(int queued, int running, int completed, int failed, int total)> GetOperationCountsAsync();
    Task<List<OperationStatus>> GetStaleRunningOperationsAsync(TimeSpan staleAfter, CancellationToken cancellationToken = default);
    Task<int> DeleteExpiredOperationsAsync(int retentionDays, CancellationToken cancellationToken = default);
}
