using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public interface ISelfServiceMetadataService
{
    Task<IReadOnlyList<SelfServiceModelSummary>> GetAllowedModelsAsync(CancellationToken cancellationToken);
    Task<IReadOnlyList<SelfServiceTableSummary>> GetAllowedTablesAsync(string databaseName, CancellationToken cancellationToken);
    Task<SelfServicePartitionListResponse?> GetAllowedPartitionsAsync(string databaseName, string tableName, CancellationToken cancellationToken);
    Task<SelfServiceRefreshValidationResult> ValidateRefreshRequestAsync(PostData requestData, CancellationToken cancellationToken);
}
