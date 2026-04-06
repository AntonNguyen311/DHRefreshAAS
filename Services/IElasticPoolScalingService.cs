namespace DHRefreshAAS.Services;

public interface IElasticPoolScalingService
{
    Task<bool> ScaleUpAsync(CancellationToken cancellationToken = default);
    Task<bool> ScaleDownAsync(CancellationToken cancellationToken = default);
}
