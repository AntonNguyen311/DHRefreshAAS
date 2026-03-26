using Microsoft.AnalysisServices.Tabular;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;

namespace DHRefreshAAS;

/// <summary>
/// Main service for AAS refresh operations in the isolated worker model
/// </summary>
public class AasRefreshService
{
    private readonly ConfigurationService _config;
    private readonly ConnectionService _connectionService;
    private readonly ILogger<AasRefreshService> _logger;

    public AasRefreshService(
        ConfigurationService config,
        ConnectionService connectionService,
        ILogger<AasRefreshService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(connectionService);
        ArgumentNullException.ThrowIfNull(logger);
        _config = config;
        _connectionService = connectionService;
        _logger = logger;
    }

    /// <summary>
    /// Execute refresh operations with retry logic and circuit breaker pattern
    /// </summary>
    public async Task<ActivityResponse> ExecuteRefreshWithRetryAsync(
        EnhancedPostData requestData,
        CancellationToken cancellationToken = default,
        Action<string, bool, string>? progressCallback = null,
        Action? saveChangesCallback = null)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Enhanced AAS refresh started with stability improvements.");

        var response = new ActivityResponse
        {
            IsSuccess = false,
            Message = "",
            StackTrace = "",
            RefreshResults = new List<RefreshResult>(),
            StartTime = startTime
        };

        // Create cancellation token with timeout
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(requestData.OperationTimeoutMinutes));
        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            // Start heartbeat task for long-running operations
            using var heartbeatCts = new CancellationTokenSource();
            var heartbeatTask = StartHeartbeatAsync(heartbeatCts.Token);

            // Execute the main refresh logic with retry
            await ExecuteRefreshOperationsWithRetryAsync(requestData, response, combinedCts.Token, progressCallback, saveChangesCallback);

            // Stop heartbeat
            heartbeatCts.Cancel();
            await heartbeatTask;

            response.EndTime = DateTime.UtcNow;
            response.ExecutionTimeSeconds = (response.EndTime - response.StartTime).TotalSeconds;

            _logger.LogInformation("Enhanced AAS refresh completed in {ExecutionTime:F2} seconds", response.ExecutionTimeSeconds);
        }
        catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
        {
            var message = $"Operation timed out after {requestData.OperationTimeoutMinutes} minutes";
            _logger.LogError(message);
            response.Message = message;
            response.IsSuccess = false;
            response.EndTime = DateTime.UtcNow;
            response.ExecutionTimeSeconds = (response.EndTime - response.StartTime).TotalSeconds;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error in enhanced AAS refresh operation");
            response.Message = ex.Message;
            response.StackTrace = ex.StackTrace ?? "";
            response.IsSuccess = false;
            response.EndTime = DateTime.UtcNow;
            response.ExecutionTimeSeconds = (response.EndTime - response.StartTime).TotalSeconds;
        }

        return response;
    }

    /// <summary>
    /// Execute refresh operations with retry logic (simplified version)
    /// </summary>
    private async Task ExecuteRefreshOperationsWithRetryAsync(
        EnhancedPostData requestData,
        ActivityResponse response,
        CancellationToken cancellationToken,
        Action<string, bool, string>? progressCallback = null,
        Action? saveChangesCallback = null)
    {
        Server? asSrv = null;

        for (int attempt = 1; attempt <= requestData.MaxRetryAttempts; attempt++)
        {
            try
            {
                _logger.LogInformation("Refresh attempt {Attempt} of {MaxAttempts}", attempt, requestData.MaxRetryAttempts);

                var connectSec = _config.GetConnectTimeoutSeconds(requestData.ConnectionTimeoutMinutes);
                var commandSec = _config.GetCommandTimeoutSeconds(
                    requestData.OperationTimeoutMinutes,
                    _config.SaveChangesTimeoutMinutes);

                asSrv = await _connectionService.CreateServerConnectionAsync(cancellationToken, connectSec, commandSec);

                if (asSrv?.Connected == true)
                {
                    await ExecuteRefreshOperationsAsync(
                        asSrv,
                        requestData,
                        response,
                        cancellationToken,
                        progressCallback,
                        saveChangesCallback);

                    response.IsSuccess = !response.RefreshResults.Exists(r => !r.IsSuccess);
                    if (response.IsSuccess)
                    {
                        response.Message = "Successfully refreshed all specified tables/partitions.";
                    }
                    else if (string.IsNullOrWhiteSpace(response.Message))
                    {
                        response.Message = "Some table/partition refreshes failed. See RefreshResults.";
                    }

                    _logger.LogInformation("Refresh completed successfully on attempt {Attempt}", attempt);
                    break;
                }
            }
            catch (Exception ex) when (attempt < requestData.MaxRetryAttempts)
            {
                var delay = TimeSpan.FromSeconds(requestData.BaseDelaySeconds * Math.Pow(2, attempt - 1));

                _logger.LogWarning(ex, "Refresh attempt {Attempt} failed. Retrying in {DelaySeconds} seconds. Error: {ErrorMessage}",
                    attempt, delay.TotalSeconds, ex.Message);

                // Clean up current connection
                await _connectionService.SafeDisconnectAsync(asSrv);
                asSrv = null;

                // Wait before retry with exponential backoff
                await Task.Delay(delay, cancellationToken);
            }
            catch (Exception ex) when (attempt >= requestData.MaxRetryAttempts)
            {
                _logger.LogError(ex, "All {MaxAttempts} refresh attempts failed. Final error: {ErrorMessage}",
                    requestData.MaxRetryAttempts, ex.Message);
                response.Message = $"All retry attempts failed. Final error: {ex.Message}";
                response.StackTrace = ex.StackTrace ?? "";
                response.IsSuccess = false;
                break;
            }
        }

        // Always clean up connection
        await _connectionService.SafeDisconnectAsync(asSrv);
    }

    /// <summary>
    /// Execute refresh operations with simplified error handling and progress tracking
    /// </summary>
    private async Task ExecuteRefreshOperationsAsync(
        Server asSrv,
        EnhancedPostData enhancedRequest,
        ActivityResponse response,
        CancellationToken cancellationToken,
        Action<string, bool, string>? progressCallback = null,
        Action? saveChangesCallback = null)
    {
        var requestData = enhancedRequest.OriginalRequest;

        // Check database name
        if (string.IsNullOrEmpty(requestData?.DatabaseName))
        {
            var msg = "Database name is missing, cannot proceed with refresh.";
            _logger.LogWarning(msg);
            response.Message = msg;
            return;
        }

        // Retrieve the database
        _logger.LogInformation("Attempting to retrieve database '{DatabaseName}'.", requestData.DatabaseName);
        Database? db = asSrv.Databases.GetByName(requestData.DatabaseName);

        if (db == null)
        {
            var msg = $"Database '{requestData.DatabaseName}' does not exist on the server.";
            _logger.LogWarning(msg);
            response.Message = msg;
            return;
        }

        // Log database information
        if (_config.EnableDetailedLogging)
        {
            _logger.LogInformation("Database found - Name: {DbName}, Compatibility Level: {CompatibilityLevel}, Last Update: {LastUpdate}",
                db.Name, db.CompatibilityLevel, db.LastUpdate);
        }

        // Refresh tables/partitions
        Model model = db.Model;
        if (requestData.RefreshObjects == null || requestData.RefreshObjects.Length == 0)
        {
            _logger.LogInformation("No refresh objects specified. Nothing to refresh.");
            return;
        }

        // Process refresh objects (simplified approach)
        foreach (var refreshObj in requestData.RefreshObjects)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var result = new RefreshResult
            {
                TableName = refreshObj.Table ?? "",
                PartitionName = refreshObj.Partition ?? "",
                IsSuccess = false,
                ErrorMessage = "",
                StackTrace = "",
                AttemptCount = 1,
                ExecutionTimeSeconds = 0
            };

            var startTime = DateTime.UtcNow;

            try
            {
                if (string.IsNullOrEmpty(refreshObj.Table))
                {
                    result.ErrorMessage = "Table name is required";
                    result.IsSuccess = false;
                }
                else
                {
                    // Find the table
                    var table = model.Tables.Find(refreshObj.Table);
                    if (table == null)
                    {
                        result.ErrorMessage = $"Table '{refreshObj.Table}' does not exist";
                        result.IsSuccess = false;
                    }
                    else
                    {
                        // Refresh table or partition
                        if (string.IsNullOrEmpty(refreshObj.Partition))
                        {
                            // Refresh entire table
                            _logger.LogInformation("Refreshing entire table '{TableName}'", refreshObj.Table);
                            await Task.Run(() =>
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                table.RequestRefresh(RefreshType.Full);
                            }, cancellationToken);
                            result.IsSuccess = true;
                        }
                        else
                        {
                            // Refresh specific partition
                            if (table.Partitions.ContainsName(refreshObj.Partition))
                            {
                                _logger.LogInformation("Refreshing partition '{PartitionName}' in table '{TableName}'",
                                    refreshObj.Partition, refreshObj.Table);
                                await Task.Run(() =>
                                {
                                    cancellationToken.ThrowIfCancellationRequested();
                                    table.Partitions[refreshObj.Partition].RequestRefresh(RefreshType.Full);
                                }, cancellationToken);
                                result.IsSuccess = true;
                            }
                            else
                            {
                                result.ErrorMessage = $"Partition '{refreshObj.Partition}' not found in table '{refreshObj.Table}'";
                                result.IsSuccess = false;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error refreshing table '{TableName}', partition '{PartitionName}': {ErrorMessage}",
                    refreshObj.Table, refreshObj.Partition, ex.Message);
                result.IsSuccess = false;
                result.ErrorMessage = ex.Message;
                result.StackTrace = ex.StackTrace ?? "";
            }

            result.ExecutionTimeSeconds = (DateTime.UtcNow - startTime).TotalSeconds;
            response.RefreshResults.Add(result);
            
            // Report progress if callback is provided
            progressCallback?.Invoke(
                result.TableName, 
                result.IsSuccess, 
                result.IsSuccess ? "" : result.ErrorMessage);
        }

        var effectiveSaveTimeoutMinutes = Math.Max(_config.SaveChangesTimeoutMinutes, enhancedRequest.OperationTimeoutMinutes);

        // Save all changes at once with timeout and retry logic for deadlocks
        CancellationTokenSource? saveTimeoutCts = null;
        int retryCount = 0;
        const int maxRetries = 3;
        const int baseDelayMs = 1000;
        var saveChangesCompleted = false;
        string? saveFailureMessage = null;

        while (retryCount <= maxRetries)
        {
            try
            {
                _logger.LogInformation(
                    "Saving model changes with timeout of {TimeoutMinutes} minutes... (Attempt {Attempt}/{MaxAttempts})",
                    effectiveSaveTimeoutMinutes,
                    retryCount + 1,
                    maxRetries + 1);

                if (retryCount == 0)
                {
                    saveChangesCallback?.Invoke();
                }

                saveTimeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(effectiveSaveTimeoutMinutes));
                using var combinedSaveCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, saveTimeoutCts.Token);

                await Task.Run(() =>
                {
                    combinedSaveCts.Token.ThrowIfCancellationRequested();
                    model.SaveChanges();
                }, combinedSaveCts.Token);

                _logger.LogInformation("SaveChanges() completed successfully.");
                saveChangesCompleted = true;
                break;
            }
            catch (Exception ex) when (IsDeadlockException(ex) && retryCount < maxRetries)
            {
                retryCount++;
                var delayMs = baseDelayMs * (int)Math.Pow(2, retryCount - 1);
                _logger.LogWarning(
                    ex,
                    "Deadlock detected during SaveChanges. Retrying in {DelayMs}ms (Attempt {Attempt}/{MaxAttempts}). Error: {ErrorMessage}",
                    delayMs,
                    retryCount + 1,
                    maxRetries + 1,
                    ex.Message);

                await Task.Delay(delayMs, cancellationToken);
                saveTimeoutCts?.Dispose();
                saveTimeoutCts = null;
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    saveFailureMessage = "Operation canceled or timed out before SaveChanges finished.";
                    _logger.LogError(saveFailureMessage);
                }
                else if (saveTimeoutCts?.Token.IsCancellationRequested == true)
                {
                    saveFailureMessage = $"SaveChanges operation timed out after {effectiveSaveTimeoutMinutes} minutes.";
                    _logger.LogError(saveFailureMessage);
                }
                else
                {
                    saveFailureMessage = "SaveChanges was canceled.";
                    _logger.LogError(saveFailureMessage);
                }

                break;
            }
            catch (Exception ex)
            {
                if (IsDeadlockException(ex))
                {
                    saveFailureMessage = "SaveChanges failed after multiple retry attempts due to persistent deadlocks.";
                    _logger.LogError(ex, saveFailureMessage);
                }
                else
                {
                    saveFailureMessage = $"SaveChanges failed: {ex.Message}";
                    _logger.LogError(ex, "Error saving changes: {ErrorMessage}", ex.Message);
                }

                break;
            }
            finally
            {
                saveTimeoutCts?.Dispose();
                saveTimeoutCts = null;
            }
        }

        if (!saveChangesCompleted)
        {
            ApplySaveFailureToResponse(response, saveFailureMessage ?? "SaveChanges did not complete.");
        }
    }

    private static void ApplySaveFailureToResponse(ActivityResponse response, string errorMessage)
    {
        response.Message = errorMessage;
        foreach (var result in response.RefreshResults.Where(r => r.IsSuccess))
        {
            result.IsSuccess = false;
            result.ErrorMessage = errorMessage;
        }
    }

    /// <summary>
    /// Determines if an exception is caused by a deadlock condition
    /// </summary>
    private static bool IsDeadlockException(Exception ex)
    {
        var message = ex.Message?.ToLowerInvariant() ?? "";
        return message.Contains("deadlock") || 
               message.Contains("was deadlocked") ||
               message.Contains("deadlock condition was detected") ||
               (message.Contains("operation was canceled") && message.Contains("deadlock"));
    }

    /// <summary>
    /// Start heartbeat logging for long-running operations
    /// </summary>
    private async Task StartHeartbeatAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(_config.HeartbeatIntervalSeconds), cancellationToken);
                var memoryUsage = GC.GetTotalMemory(false) / 1024 / 1024;
                _logger.LogInformation("[HEARTBEAT] Operation still running at {CurrentTime:yyyy-MM-dd HH:mm:ss} UTC - Memory: {MemoryUsage} MB",
                    DateTime.UtcNow, memoryUsage);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            _logger.LogInformation("[HEARTBEAT] Stopped");
        }
    }
}