namespace DHRefreshAAS.Services;

public interface IAasScalingService
{
    Task<bool> ScaleUpAsync(CancellationToken cancellationToken = default);
    Task<bool> ScaleDownAsync(CancellationToken cancellationToken = default);
}
