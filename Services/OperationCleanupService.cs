using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS.Services;

/// <summary>
/// Hosted service that cleans up zombie operations on startup and
/// marks in-flight operations as failed during graceful shutdown.
/// </summary>
public class OperationCleanupService : IHostedService
{
    private readonly OperationStorageService _operationStorage;
    private readonly ConfigurationService _config;
    private readonly ILogger<OperationCleanupService> _logger;
    private readonly ConcurrentDictionary<string, byte> _inFlightOperations = new();

    public OperationCleanupService(
        OperationStorageService operationStorage,
        ConfigurationService config,
        ILogger<OperationCleanupService> logger)
    {
        _operationStorage = operationStorage;
        _config = config;
        _logger = logger;
    }

    public void TrackOperation(string operationId) => _inFlightOperations.TryAdd(operationId, 0);
    public void UntrackOperation(string operationId) => _inFlightOperations.TryRemove(operationId, out _);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await CleanupZombieOperationsAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Zombie cleanup on startup failed: {ErrorMessage}", ex.Message);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var operationIds = _inFlightOperations.Keys.ToList();
        if (operationIds.Count == 0) return;

        _logger.LogWarning("Application shutting down with {Count} in-flight operations, marking as failed", operationIds.Count);

        foreach (var operationId in operationIds)
        {
            try
            {
                await _operationStorage.MarkOperationAsFailedAsync(
                    operationId, "Operation terminated: application shutting down");
                await _operationStorage.ReleaseQueueLeaseForOperationAsync(operationId);
                _logger.LogInformation("Marked in-flight operation {OperationId} as failed (shutdown)", operationId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to mark operation {OperationId} during shutdown", operationId);
            }
        }
    }

    private async Task CleanupZombieOperationsAsync(CancellationToken cancellationToken)
    {
        var staleAfter = TimeSpan.FromMinutes(_config.ZombieTimeoutMinutes);
        var timeoutMinutes = _config.ZombieTimeoutMinutes;
        var runningOps = await _operationStorage.GetRunningOperationsAsync();

        if (runningOps.Count == 0) return;

        var zombies = await _operationStorage.GetStaleRunningOperationsAsync(staleAfter, cancellationToken);

        if (zombies.Count == 0)
        {
            _logger.LogInformation("Found {Count} running operations, none older than {Timeout} minutes", runningOps.Count, timeoutMinutes);
            return;
        }

        _logger.LogWarning("Found {ZombieCount} zombie operations (running > {Timeout} min), cleaning up", zombies.Count, timeoutMinutes);

        foreach (var zombie in zombies)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var age = (DateTime.UtcNow - zombie.StartTime).TotalMinutes;
            await _operationStorage.MarkOperationAsFailedAsync(
                zombie.OperationId,
                $"Operation terminated: marked as zombie during application startup (running for {age:F0} min, exceeded {timeoutMinutes} min timeout)");
            await _operationStorage.ReleaseQueueLeaseForOperationAsync(zombie.OperationId, cancellationToken);
            _logger.LogInformation("Cleaned up zombie operation {OperationId} (age: {Age:F0} min)", zombie.OperationId, age);
        }

        _logger.LogInformation("Zombie cleanup complete: {CleanedCount} operations marked as failed", zombies.Count);
    }
}
