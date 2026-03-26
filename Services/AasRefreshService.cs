using Microsoft.AnalysisServices.Tabular;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using Polly;
using Polly.Retry;

namespace DHRefreshAAS;

/// <summary>
/// Main service for AAS refresh operations in the isolated worker model
/// </summary>
public class AasRefreshService
{
    private readonly ConfigurationService _config;
    private readonly ConnectionService _connectionService;
    private readonly AasScalingService _scalingService;
    private readonly ElasticPoolScalingService _elasticPoolScalingService;
    private readonly ILogger<AasRefreshService> _logger;

    public AasRefreshService(
        ConfigurationService config,
        ConnectionService connectionService,
        AasScalingService scalingService,
        ElasticPoolScalingService elasticPoolScalingService,
        ILogger<AasRefreshService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(connectionService);
        ArgumentNullException.ThrowIfNull(scalingService);
        ArgumentNullException.ThrowIfNull(elasticPoolScalingService);
        ArgumentNullException.ThrowIfNull(logger);
        _config = config;
        _connectionService = connectionService;
        _scalingService = scalingService;
        _elasticPoolScalingService = elasticPoolScalingService;
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
        var maxAttempts = Math.Max(1, requestData.MaxRetryAttempts);
        var connectSec = _config.GetConnectTimeoutSeconds(requestData.ConnectionTimeoutMinutes);
        var commandSec = _config.GetCommandTimeoutSeconds(
            requestData.OperationTimeoutMinutes,
            _config.SaveChangesTimeoutMinutes);

        var retryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = maxAttempts - 1,
            BackoffType = DelayBackoffType.Exponential,
            Delay = TimeSpan.FromSeconds(Math.Max(1, requestData.BaseDelaySeconds)),
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            OnRetry = async args =>
            {
                _logger.LogWarning(
                    args.Outcome.Exception,
                    "Refresh attempt {Attempt} failed. Retrying in {DelaySeconds} seconds. Error: {ErrorMessage}",
                    args.AttemptNumber + 1,
                    args.RetryDelay.TotalSeconds,
                    args.Outcome.Exception?.Message);
                await _connectionService.SafeDisconnectAsync(asSrv);
                asSrv = null;
            }
        };

        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(retryOptions)
            .Build();

        // Auto-scale Elastic Pool BEFORE AAS (SQL needs capacity first)
        var elasticPoolScaledUp = false;
        _logger.LogInformation("Elastic Pool auto-scaling enabled: {Enabled}", _config.EnableElasticPoolAutoScaling);
        if (_config.EnableElasticPoolAutoScaling)
        {
            try
            {
                elasticPoolScaledUp = await _elasticPoolScalingService.ScaleUpAsync(cancellationToken);
                _logger.LogInformation("Elastic Pool scale-up result: {ScaledUp}", elasticPoolScaledUp);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Elastic Pool auto-scale up failed, continuing with current DTU");
            }
        }

        // Auto-scale AAS BEFORE connecting (scaling restarts the server, which kills existing connections)
        var scaledUp = false;
        _logger.LogInformation("AAS auto-scaling enabled: {Enabled}", _config.EnableAasAutoScaling);
        if (_config.EnableAasAutoScaling)
        {
            try
            {
                scaledUp = await _scalingService.ScaleUpAsync(cancellationToken);
                _logger.LogInformation("AAS scale-up result: {ScaledUp}", scaledUp);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "AAS auto-scale up failed, continuing with current SKU");
            }
        }

        try
        {
            await pipeline.ExecuteAsync(async token =>
            {
                _logger.LogInformation("Refresh attempt started.");
                asSrv = await _connectionService.CreateServerConnectionAsync(token, connectSec, commandSec);
                if (asSrv?.Connected != true)
                {
                    throw new InvalidOperationException("Failed to connect to AAS server");
                }

                await ExecuteRefreshOperationsAsync(
                    asSrv,
                    requestData,
                    response,
                    token,
                    progressCallback,
                    saveChangesCallback);
            }, cancellationToken);

            response.IsSuccess = !response.RefreshResults.Exists(r => !r.IsSuccess);
            if (response.IsSuccess)
            {
                response.Message = "Successfully refreshed all specified tables/partitions.";
            }
            else if (string.IsNullOrWhiteSpace(response.Message))
            {
                response.Message = "Some table/partition refreshes failed. See RefreshResults.";
            }

            // Build top slow tables list (top 10 by processing time)
            response.TopSlowTables = response.RefreshResults
                .Where(r => r.ProcessingTimeSeconds.HasValue && r.ProcessingTimeSeconds > 0)
                .OrderByDescending(r => r.ProcessingTimeSeconds)
                .Take(10)
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "All {MaxAttempts} refresh attempts failed. Final error: {ErrorMessage}",
                maxAttempts, ex.Message);
            response.Message = $"All retry attempts failed. Final error: {ex.Message}";
            response.StackTrace = ex.StackTrace ?? "";
            response.IsSuccess = false;
        }
        finally
        {
            await _connectionService.SafeDisconnectAsync(asSrv);

            // ALWAYS scale down - even if refresh fails, crashes, or times out
            if (scaledUp)
            {
                try
                {
                    await _scalingService.ScaleDownAsync(CancellationToken.None);
                }
                catch (Exception scaleEx)
                {
                    _logger.LogCritical(scaleEx,
                        "CRITICAL: Failed to scale AAS back to {OriginalSku}! Manual intervention required to avoid cost overrun.",
                        _config.AasOriginalSku);
                }
            }

            if (elasticPoolScaledUp)
            {
                try
                {
                    await _elasticPoolScalingService.ScaleDownAsync(CancellationToken.None);
                }
                catch (Exception scaleEx)
                {
                    _logger.LogCritical(scaleEx,
                        "CRITICAL: Failed to scale Elastic Pool back to {OriginalDtu} DTU! Manual intervention required to avoid cost overrun.",
                        _config.ElasticPoolOriginalDtu);
                }
            }
        }
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

        // Validate and collect refresh objects
        var validObjects = new List<(RefreshObject refreshObj, RefreshResult result)>();
        foreach (var refreshObj in requestData.RefreshObjects)
        {
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

            if (string.IsNullOrEmpty(refreshObj.Table))
            {
                result.ErrorMessage = "Table name is required";
                response.RefreshResults.Add(result);
                progressCallback?.Invoke(result.TableName, false, result.ErrorMessage);
                continue;
            }

            var table = model.Tables.Find(refreshObj.Table);
            if (table == null)
            {
                result.ErrorMessage = $"Table '{refreshObj.Table}' does not exist";
                response.RefreshResults.Add(result);
                progressCallback?.Invoke(result.TableName, false, result.ErrorMessage);
                continue;
            }

            if (!string.IsNullOrEmpty(refreshObj.Partition) && !table.Partitions.ContainsName(refreshObj.Partition))
            {
                result.ErrorMessage = $"Partition '{refreshObj.Partition}' not found in table '{refreshObj.Table}'";
                response.RefreshResults.Add(result);
                progressCallback?.Invoke(result.TableName, false, result.ErrorMessage);
                continue;
            }

            validObjects.Add((refreshObj, result));
        }

        if (validObjects.Count == 0)
        {
            _logger.LogInformation("No valid refresh objects found after validation.");
            return;
        }

        // Process in batches: RequestRefresh + SaveChanges per batch
        var batchSize = Math.Max(1, _config.SaveChangesBatchSize);
        var maxParallelism = Math.Max(1, _config.SaveChangesMaxParallelism);
        var effectiveSaveTimeoutMinutes = Math.Max(_config.SaveChangesTimeoutMinutes, enhancedRequest.OperationTimeoutMinutes);
        var batches = validObjects.Chunk(batchSize).ToList();

        _logger.LogInformation(
            "Processing {TotalObjects} refresh objects in {BatchCount} batches (batch size: {BatchSize}, max parallelism: {MaxParallelism})",
            validObjects.Count, batches.Count, batchSize, maxParallelism);

        var batchIndex = 0;
        foreach (var batch in batches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            batchIndex++;

            _logger.LogInformation("Starting batch {BatchIndex}/{BatchCount} with {BatchItemCount} objects",
                batchIndex, batches.Count, batch.Length);

            // RequestRefresh for all objects in this batch
            foreach (var (refreshObj, result) in batch)
            {
                var startTime = DateTime.UtcNow;
                try
                {
                    var table = model.Tables.Find(refreshObj.Table)!;
                    if (string.IsNullOrEmpty(refreshObj.Partition))
                    {
                        _logger.LogInformation("Requesting refresh for table '{TableName}'", refreshObj.Table);
                        table.RequestRefresh(RefreshType.DataOnly);
                    }
                    else
                    {
                        _logger.LogInformation("Requesting refresh for partition '{PartitionName}' in table '{TableName}'",
                            refreshObj.Partition, refreshObj.Table);
                        table.Partitions[refreshObj.Partition].RequestRefresh(RefreshType.DataOnly);
                    }
                    result.IsSuccess = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error requesting refresh for '{TableName}': {ErrorMessage}",
                        refreshObj.Table, ex.Message);
                    result.IsSuccess = false;
                    result.ErrorMessage = ex.Message;
                    result.StackTrace = ex.StackTrace ?? "";
                }
                result.ExecutionTimeSeconds = (DateTime.UtcNow - startTime).TotalSeconds;
            }

            // SaveChanges for this batch
            var batchHasValidRequests = batch.Any(b => b.result.IsSuccess);
            if (!batchHasValidRequests)
            {
                _logger.LogWarning("Batch {BatchIndex} has no valid refresh requests, skipping SaveChanges", batchIndex);
                foreach (var (_, result) in batch)
                {
                    response.RefreshResults.Add(result);
                    progressCallback?.Invoke(result.TableName, result.IsSuccess, result.ErrorMessage);
                }
                continue;
            }

            saveChangesCallback?.Invoke();
            var saveChangesStartTime = DateTime.UtcNow;
            var saveSuccess = await ExecuteBatchSaveChangesAsync(
                model, maxParallelism, effectiveSaveTimeoutMinutes, batchIndex, batches.Count, cancellationToken);

            // Query row counts after SaveChanges
            Dictionary<string, long>? rowCounts = null;
            if (saveSuccess)
            {
                _logger.LogInformation("Batch {BatchIndex}/{BatchCount} SaveChanges completed successfully", batchIndex, batches.Count);
                var tableNames = batch
                    .Where(b => b.result.IsSuccess)
                    .Select(b => b.refreshObj.Table!)
                    .Distinct()
                    .ToList();
                rowCounts = await QueryTableRowCountsAsync(requestData.DatabaseName, tableNames);
            }
            else
            {
                _logger.LogError("Batch {BatchIndex}/{BatchCount} SaveChanges failed", batchIndex, batches.Count);
            }

            // Update results for this batch
            foreach (var (refreshObj, result) in batch)
            {
                if (!saveSuccess && result.IsSuccess)
                {
                    result.IsSuccess = false;
                    result.ErrorMessage = $"SaveChanges failed for batch {batchIndex}";
                }

                // Add row count and partition info
                if (saveSuccess && result.IsSuccess && rowCounts != null)
                {
                    var key = string.IsNullOrEmpty(refreshObj.Partition) ? refreshObj.Table! : $"{refreshObj.Table}|{refreshObj.Partition}";
                    if (rowCounts.TryGetValue(refreshObj.Table!, out var count))
                    {
                        result.RowCount = count;
                    }
                    // Get RefreshedTime from TOM and calculate processing time
                    var tbl = model.Tables.Find(refreshObj.Table);
                    if (tbl != null)
                    {
                        var partition = string.IsNullOrEmpty(refreshObj.Partition)
                            ? tbl.Partitions.FirstOrDefault()
                            : (tbl.Partitions.ContainsName(refreshObj.Partition) ? tbl.Partitions[refreshObj.Partition] : null);
                        result.RefreshedTime = partition?.RefreshedTime;
                        if (result.RefreshedTime.HasValue)
                        {
                            result.ProcessingTimeSeconds = (result.RefreshedTime.Value - saveChangesStartTime).TotalSeconds;
                            if (result.ProcessingTimeSeconds < 0) result.ProcessingTimeSeconds = 0;
                        }
                    }

                    _logger.LogInformation(
                        "Table '{TableName}' partition '{PartitionName}' refreshed — RowCount: {RowCount:N0}, ProcessingTime: {ProcessingTime:F1}s, RefreshedTime: {RefreshedTime}",
                        result.TableName, result.PartitionName, result.RowCount ?? -1, result.ProcessingTimeSeconds ?? 0, result.RefreshedTime);
                }

                response.RefreshResults.Add(result);
                progressCallback?.Invoke(result.TableName, result.IsSuccess, result.ErrorMessage);
            }
        }
    }

    private Task<Dictionary<string, long>> QueryTableRowCountsAsync(string databaseName, List<string> tableNames)
    {
        // Row count querying requires ADOMD which has package compatibility issues.
        // For now, log table names only. Row counts will be added when ADOMD version is resolved.
        var results = new Dictionary<string, long>();
        foreach (var tableName in tableNames)
        {
            _logger.LogInformation("Table '{TableName}' in database '{DatabaseName}' refreshed successfully", tableName, databaseName);
        }
        return Task.FromResult(results);
    }

    private async Task<bool> ExecuteBatchSaveChangesAsync(
        Model model,
        int maxParallelism,
        int timeoutMinutes,
        int batchIndex,
        int totalBatches,
        CancellationToken cancellationToken)
    {
        const int maxRetries = 3;

        var saveRetryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = maxRetries,
            BackoffType = DelayBackoffType.Exponential,
            Delay = TimeSpan.FromSeconds(1),
            ShouldHandle = new PredicateBuilder().Handle<Exception>(IsDeadlockException),
            OnRetry = args =>
            {
                _logger.LogInformation(
                    "Deadlock in batch {BatchIndex} SaveChanges. Retrying in {DelaySeconds}s (Attempt {Attempt}/{MaxAttempts})",
                    batchIndex, args.RetryDelay.TotalSeconds, args.AttemptNumber + 2, maxRetries + 1);
                return ValueTask.CompletedTask;
            }
        };

        var savePipeline = new ResiliencePipelineBuilder()
            .AddRetry(saveRetryOptions)
            .Build();

        using var saveTimeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(timeoutMinutes));
        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, saveTimeoutCts.Token);

        try
        {
            _logger.LogInformation(
                "Batch {BatchIndex}/{TotalBatches} SaveChanges starting (timeout: {Timeout}min, parallelism: {Parallelism})",
                batchIndex, totalBatches, timeoutMinutes, maxParallelism);

            await savePipeline.ExecuteAsync(async token =>
            {
                await Task.Run(() =>
                {
                    token.ThrowIfCancellationRequested();
                    model.SaveChanges(new SaveOptions { MaxParallelism = maxParallelism });
                }, token);
            }, combinedCts.Token);

            return true;
        }
        catch (OperationCanceledException) when (saveTimeoutCts.Token.IsCancellationRequested)
        {
            _logger.LogError("Batch {BatchIndex} SaveChanges timed out after {Timeout} minutes", batchIndex, timeoutMinutes);
            return false;
        }
        catch (OperationCanceledException)
        {
            _logger.LogError("Batch {BatchIndex} SaveChanges was canceled", batchIndex);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Batch {BatchIndex} SaveChanges failed: {ErrorMessage}", batchIndex, ex.Message);
            return false;
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