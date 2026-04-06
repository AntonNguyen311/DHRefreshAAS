using System.Collections.Concurrent;
using DHRefreshAAS.Services;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS;

/// <summary>
/// Manages per-database semaphores to prevent concurrent SaveChanges
/// on the same AAS database, which causes lock contention and hangs.
/// </summary>
public class RefreshConcurrencyService : IRefreshConcurrencyService
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _dbSemaphores = new(StringComparer.OrdinalIgnoreCase);
    private readonly ILogger<RefreshConcurrencyService> _logger;

    public RefreshConcurrencyService(ILogger<RefreshConcurrencyService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Get or create a semaphore for the given database.
    /// Only 1 SaveChanges can run at a time per database.
    /// Different databases get different semaphores and can run in parallel.
    /// </summary>
    public SemaphoreSlim GetDatabaseSemaphore(string databaseName)
    {
        return _dbSemaphores.GetOrAdd(databaseName, name =>
        {
            _logger.LogInformation("Creating concurrency semaphore for database '{Database}'", name);
            return new SemaphoreSlim(1, 1);
        });
    }

    /// <summary>
    /// Removes and disposes the semaphore for a database when no operations remain for it.
    /// </summary>
    public void ReleaseDatabaseSemaphore(string databaseName)
    {
        if (_dbSemaphores.TryRemove(databaseName, out var semaphore))
        {
            semaphore.Dispose();
            _logger.LogInformation("Released concurrency semaphore for database '{Database}'", databaseName);
        }
    }
}
