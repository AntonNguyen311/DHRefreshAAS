namespace DHRefreshAAS.Services;

public interface IRefreshConcurrencyService
{
    SemaphoreSlim GetDatabaseSemaphore(string databaseName);
    void ReleaseDatabaseSemaphore(string databaseName);
}
