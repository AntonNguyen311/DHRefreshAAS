using Microsoft.AnalysisServices.Tabular;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;
using Polly;
using Polly.Retry;

namespace DHRefreshAAS;

/// <summary>
/// Main service for AAS refresh operations in the isolated worker model
/// </summary>
public class AasRefreshService : IAasRefreshService
{
    private readonly IConfigurationService _config;
    private readonly IConnectionService _connectionService;
    private readonly AasScalingService _scalingService;
    private readonly ElasticPoolScalingService _elasticPoolScalingService;
    private readonly RefreshConcurrencyService _concurrencyService;
    private readonly IOperationStorageService _operationStorage;
    private readonly RowCountQueryService _rowCountQueryService;
    private readonly SlowTableMetricsService _slowTableMetricsService;
    private readonly ILogger<AasRefreshService> _logger;

    public AasRefreshService(
        IConfigurationService config,
        IConnectionService connectionService,
        AasScalingService scalingService,
        ElasticPoolScalingService elasticPoolScalingService,
        RefreshConcurrencyService concurrencyService,
        IOperationStorageService operationStorage,
        RowCountQueryService rowCountQueryService,
        SlowTableMetricsService slowTableMetricsService,
        ILogger<AasRefreshService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(connectionService);
        ArgumentNullException.ThrowIfNull(scalingService);
        ArgumentNullException.ThrowIfNull(elasticPoolScalingService);
        ArgumentNullException.ThrowIfNull(concurrencyService);
        ArgumentNullException.ThrowIfNull(operationStorage);
        ArgumentNullException.ThrowIfNull(rowCountQueryService);
        ArgumentNullException.ThrowIfNull(slowTableMetricsService);
        ArgumentNullException.ThrowIfNull(logger);
        _config = config;
        _connectionService = connectionService;
        _scalingService = scalingService;
        _elasticPoolScalingService = elasticPoolScalingService;
        _concurrencyService = concurrencyService;
        _operationStorage = operationStorage;
        _rowCountQueryService = rowCountQueryService;
        _slowTableMetricsService = slowTableMetricsService;
        _logger = logger;
    }

    /// <summary>
    /// Execute refresh operations with retry logic and circuit breaker pattern
    /// </summary>
    public async Task<ActivityResponse> ExecuteRefreshWithRetryAsync(
        EnhancedPostData requestData,
        CancellationToken cancellationToken = default,
        Action<string, bool, string>? progressCallback = null,
        Action<SaveChangesDiagnostic>? saveChangesCallback = null)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Enhanced AAS refresh started with stability improvements.");

        var response = new ActivityResponse
        {
            IsSuccess = false,
            Message = "",
            StackTrace = "",
            RefreshResults = new List<RefreshResult>(),
            StartTime = startTime,
            ExecutionSettings = new RefreshExecutionSettings
            {
                OperationTimeoutMinutes = requestData.OperationTimeoutMinutes,
                SaveChangesTimeoutMinutes = Math.Max(_config.SaveChangesTimeoutMinutes, requestData.OperationTimeoutMinutes),
                SaveChangesBatchSize = Math.Max(1, _config.SaveChangesBatchSize),
                SaveChangesMaxParallelism = Math.Max(1, _config.SaveChangesMaxParallelism),
                MaxRetryAttempts = Math.Max(1, requestData.MaxRetryAttempts)
            }
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
            response.Message = response.LastBatchDiagnostic == null
                ? message
                : $"{message}. Last observed batch: {SaveChangesFailureAnalyzer.BuildBatchFailureMessage(response.LastBatchDiagnostic)}";
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
        Action<SaveChangesDiagnostic>? saveChangesCallback = null)
    {
        Server? asSrv = null;
        var maxAttempts = Math.Max(1, requestData.MaxRetryAttempts);
        var connectSec = _config.GetConnectTimeoutSeconds(requestData.ConnectionTimeoutMinutes);
        var commandSec = _config.GetCommandTimeoutSeconds(
            requestData.OperationTimeoutMinutes,
            _config.SaveChangesTimeoutMinutes);

        var pipelineBuilder = new ResiliencePipelineBuilder();
        if (maxAttempts > 1)
        {
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

            pipelineBuilder.AddRetry(retryOptions);
        }

        var pipeline = pipelineBuilder.Build();

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
                // Probe server readiness before attempting the real connection
                // (covers the gap between ARM provisioning state=Succeeded and AAS engine accepting connections)
                await _connectionService.WaitForServerReadyAsync(token, maxRetries: 18, delaySeconds: 10);

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

            // Build top slow tables list (top 10 by processing time)
            response.TopSlowTables = response.RefreshResults
                .Where(r => r.ProcessingTimeSeconds.HasValue && r.ProcessingTimeSeconds > 0)
                .OrderByDescending(r => r.ProcessingTimeSeconds)
                .Take(10)
                .ToList();

            _slowTableMetricsService.ApplySlowTableMetrics(response, requestData);
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
            // BUT only if no other operations are still running (parallel per-database queues)
            if (scaledUp)
            {
                try
                {
                    var otherRunning = await _operationStorage.GetRunningOperationCountAsync();
                    if (otherRunning <= 0)
                    {
                        await _scalingService.ScaleDownAsync(CancellationToken.None);
                    }
                    else
                    {
                        _logger.LogInformation(
                            "Skipping inline AAS scale-down: {RunningCount} other operation(s) still running. Logic App Scale_Down will handle it.",
                            otherRunning);
                    }
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
                    var otherRunning = await _operationStorage.GetRunningOperationCountAsync();
                    if (otherRunning <= 0)
                    {
                        await _elasticPoolScalingService.ScaleDownAsync(CancellationToken.None);
                    }
                    else
                    {
                        _logger.LogInformation(
                            "Skipping inline Elastic Pool scale-down: {RunningCount} other operation(s) still running.",
                            otherRunning);
                    }
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
        Action<SaveChangesDiagnostic>? saveChangesCallback = null)
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

        // Get per-database semaphore to prevent concurrent SaveChanges on the same AAS database
        var dbSemaphore = _concurrencyService.GetDatabaseSemaphore(requestData.DatabaseName);

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
                    var tomRefreshType = refreshObj.IsFullRefresh ? RefreshType.Full : RefreshType.DataOnly;
                    if (string.IsNullOrEmpty(refreshObj.Partition))
                    {
                        _logger.LogInformation("Requesting {RefreshType} refresh for table '{TableName}'",
                            tomRefreshType, refreshObj.Table);
                        table.RequestRefresh(tomRefreshType);
                    }
                    else
                    {
                        _logger.LogInformation("Requesting {RefreshType} refresh for partition '{PartitionName}' in table '{TableName}'",
                            tomRefreshType, refreshObj.Partition, refreshObj.Table);
                        table.Partitions[refreshObj.Partition].RequestRefresh(tomRefreshType);
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

            var saveTargets = batch
                .Where(b => b.result.IsSuccess)
                .Select(b => string.IsNullOrEmpty(b.refreshObj.Partition)
                    ? b.refreshObj.Table!
                    : $"{b.refreshObj.Table}::{b.refreshObj.Partition}")
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            var batchDiagnostic = CreateBatchDiagnostic(
                saveTargets,
                batchIndex,
                batches.Count,
                effectiveSaveTimeoutMinutes,
                maxParallelism);

            // Acquire per-database lock to prevent concurrent SaveChanges on the same AAS database
            _logger.LogInformation("Waiting for database lock on '{Database}' for batch {BatchIndex}...",
                requestData.DatabaseName, batchIndex);
            await dbSemaphore.WaitAsync(cancellationToken);
            SaveChangesDiagnostic saveChangesDiagnostic;
            var saveChangesStartTime = DateTime.UtcNow;
            try
            {
                _logger.LogInformation("Acquired database lock on '{Database}' for batch {BatchIndex}",
                    requestData.DatabaseName, batchIndex);
                response.LastBatchDiagnostic = CloneDiagnostic(batchDiagnostic);
                saveChangesCallback?.Invoke(CloneDiagnostic(batchDiagnostic));
                saveChangesDiagnostic = await ExecuteBatchSaveChangesAsync(model, batchDiagnostic, cancellationToken);
                response.LastBatchDiagnostic = CloneDiagnostic(saveChangesDiagnostic);
            }
            finally
            {
                dbSemaphore.Release();
                _logger.LogInformation("Released database lock on '{Database}' for batch {BatchIndex}",
                    requestData.DatabaseName, batchIndex);
            }
            var saveSuccess = saveChangesDiagnostic.IsSuccess;

            // Query row counts after SaveChanges
            Dictionary<string, long>? rowCounts = null;
            if (saveSuccess)
            {
                _logger.LogInformation("Batch {BatchIndex}/{BatchCount} SaveChanges completed successfully", batchIndex, batches.Count);
                var refreshedObjects = batch
                    .Where(b => b.result.IsSuccess)
                    .ToList();
                rowCounts = await _rowCountQueryService.QueryTableRowCountsAsync(requestData.DatabaseName, refreshedObjects, cancellationToken);
            }
            else
            {
                response.LastBatchDiagnostic = saveChangesDiagnostic;
                _logger.LogError(
                    "Batch {BatchIndex}/{BatchCount} SaveChanges failed. Category: {FailureCategory}, Source: {FailureSource}, Error: {ErrorMessage}",
                    batchIndex,
                    batches.Count,
                    saveChangesDiagnostic.FailureCategory,
                    saveChangesDiagnostic.FailureSource,
                    saveChangesDiagnostic.ErrorMessage);
            }

            // Update results for this batch
            foreach (var (refreshObj, result) in batch)
            {
                if (!saveSuccess && result.IsSuccess)
                {
                    result.IsSuccess = false;
                    result.ErrorMessage = SaveChangesFailureAnalyzer.BuildBatchFailureMessage(saveChangesDiagnostic);
                }

                // Add row count and partition info
                if (saveSuccess && result.IsSuccess && rowCounts != null)
                {
                    var count = RowCountQueryService.ResolveRowCount(rowCounts, refreshObj.Table!, refreshObj.Partition);
                    if (count.HasValue)
                    {
                        result.RowCount = count.Value;
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

        // Recalculate the model if any DataOnly tables were refreshed successfully
        // (Full tables already include calculate, so no extra step needed for those)
        var anySuccess = response.RefreshResults.Any(r => r.IsSuccess);
        var hasDataOnlyTables = validObjects.Any(v => !v.refreshObj.IsFullRefresh &&
            response.RefreshResults.Any(r => r.IsSuccess && r.TableName == v.refreshObj.Table));
        var calculateSuccess = !hasDataOnlyTables; // true if no DataOnly tables (nothing to calculate)
        if (hasDataOnlyTables)
        {
            _logger.LogInformation("DataOnly batches complete. Starting model Calculate for database '{Database}'...",
                requestData.DatabaseName);
            await dbSemaphore.WaitAsync(cancellationToken);
            try
            {
                model.RequestRefresh(RefreshType.Calculate);
                var calculateDiagnostic = CreateBatchDiagnostic(
                    new[] { "Model Calculate" },
                    batches.Count + 1,
                    batches.Count + 1,
                    effectiveSaveTimeoutMinutes,
                    maxParallelism);
                response.LastBatchDiagnostic = CloneDiagnostic(calculateDiagnostic);
                saveChangesCallback?.Invoke(CloneDiagnostic(calculateDiagnostic));
                calculateDiagnostic = await ExecuteBatchSaveChangesAsync(model, calculateDiagnostic, cancellationToken);
                response.LastBatchDiagnostic = CloneDiagnostic(calculateDiagnostic);
                calculateSuccess = calculateDiagnostic.IsSuccess;
                if (calculateSuccess)
                {
                    _logger.LogInformation("Model Calculate completed for database '{Database}'", requestData.DatabaseName);
                }
                else
                {
                    response.LastBatchDiagnostic = calculateDiagnostic;
                    _logger.LogError("Model Calculate SaveChanges failed for database '{Database}'", requestData.DatabaseName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Model Calculate failed for database '{Database}': {Error}",
                    requestData.DatabaseName, ex.Message);
            }
            finally
            {
                dbSemaphore.Release();
            }
        }
        else if (anySuccess)
        {
            _logger.LogInformation("All tables used Full refresh — skipping separate Calculate step for database '{Database}'",
                requestData.DatabaseName);
        }

        // Set final response status
        var allDataSuccess = !response.RefreshResults.Exists(r => !r.IsSuccess);
        if (allDataSuccess && calculateSuccess)
        {
            response.IsSuccess = true;
            response.Message = "Successfully refreshed all specified tables/partitions.";
        }
        else if (allDataSuccess && !calculateSuccess)
        {
            response.IsSuccess = false;
            response.Message = response.LastBatchDiagnostic == null
                ? "Data refresh succeeded but Calculate failed. Model may show 'needs to be recalculated'."
                : $"Data refresh succeeded but Calculate failed. {SaveChangesFailureAnalyzer.BuildBatchFailureMessage(response.LastBatchDiagnostic)}";
        }
        else if (!allDataSuccess)
        {
            response.IsSuccess = false;
            if (string.IsNullOrWhiteSpace(response.Message))
            {
                response.Message = response.LastBatchDiagnostic == null
                    ? "Some table/partition refreshes failed. See RefreshResults."
                    : $"Some table/partition refreshes failed. {SaveChangesFailureAnalyzer.BuildBatchFailureMessage(response.LastBatchDiagnostic)}";
            }
        }
    }

    private async Task<SaveChangesDiagnostic> ExecuteBatchSaveChangesAsync(
        Model model,
        SaveChangesDiagnostic diagnostic,
        CancellationToken cancellationToken)
    {
        const int maxRetries = 3;

        var saveRetryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = maxRetries,
            BackoffType = DelayBackoffType.Exponential,
            Delay = TimeSpan.FromSeconds(1),
            ShouldHandle = new PredicateBuilder().Handle<Exception>(SaveChangesFailureAnalyzer.IsDeadlockException),
            OnRetry = args =>
            {
                _logger.LogInformation(
                    "Deadlock in batch {BatchIndex} SaveChanges. Retrying in {DelaySeconds}s (Attempt {Attempt}/{MaxAttempts})",
                    diagnostic.BatchIndex, args.RetryDelay.TotalSeconds, args.AttemptNumber + 2, maxRetries + 1);
                return ValueTask.CompletedTask;
            }
        };

        var savePipeline = new ResiliencePipelineBuilder()
            .AddRetry(saveRetryOptions)
            .Build();

        using var saveTimeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(diagnostic.SaveChangesTimeoutMinutes));
        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, saveTimeoutCts.Token);

        try
        {
            _logger.LogInformation(
                "Batch {BatchIndex}/{TotalBatches} SaveChanges starting (timeout: {Timeout}min, parallelism: {Parallelism}). Targets: {Tables}",
                diagnostic.BatchIndex,
                diagnostic.TotalBatches,
                diagnostic.SaveChangesTimeoutMinutes,
                diagnostic.MaxParallelism,
                string.Join(", ", diagnostic.Tables));

            await savePipeline.ExecuteAsync(async token =>
            {
                await Task.Run(() =>
                {
                    token.ThrowIfCancellationRequested();
                    model.SaveChanges(new SaveOptions { MaxParallelism = diagnostic.MaxParallelism });
                }, token);
            }, combinedCts.Token);

            diagnostic.IsSuccess = true;
            return diagnostic;
        }
        catch (OperationCanceledException ex) when (saveTimeoutCts.Token.IsCancellationRequested)
        {
            SaveChangesFailureAnalyzer.PopulateFailureDiagnostic(
                diagnostic,
                ex,
                $"SaveChanges timed out after {diagnostic.SaveChangesTimeoutMinutes} minutes. The underlying server-side command may still be running.",
                timedOut: true,
                canceled: false);
            _logger.LogError(
                ex,
                "Batch {BatchIndex} SaveChanges timed out after {Timeout} minutes. Targets: {Tables}",
                diagnostic.BatchIndex,
                diagnostic.SaveChangesTimeoutMinutes,
                string.Join(", ", diagnostic.Tables));
            return diagnostic;
        }
        catch (OperationCanceledException ex)
        {
            SaveChangesFailureAnalyzer.PopulateFailureDiagnostic(
                diagnostic,
                ex,
                "SaveChanges was canceled by the host or shutdown token before completion.",
                timedOut: false,
                canceled: true);
            _logger.LogError(
                ex,
                "Batch {BatchIndex} SaveChanges was canceled. Targets: {Tables}",
                diagnostic.BatchIndex,
                string.Join(", ", diagnostic.Tables));
            return diagnostic;
        }
        catch (Exception ex)
        {
            SaveChangesFailureAnalyzer.PopulateFailureDiagnostic(
                diagnostic,
                ex,
                ex.Message,
                timedOut: false,
                canceled: false);
            _logger.LogError(
                ex,
                "Batch {BatchIndex} SaveChanges failed. Category: {FailureCategory}, Source: {FailureSource}, Targets: {Tables}, Error: {ErrorMessage}",
                diagnostic.BatchIndex,
                diagnostic.FailureCategory,
                diagnostic.FailureSource,
                string.Join(", ", diagnostic.Tables),
                diagnostic.ErrorMessage);
            return diagnostic;
        }
    }

    private static SaveChangesDiagnostic CreateBatchDiagnostic(
        IEnumerable<string> tables,
        int batchIndex,
        int totalBatches,
        int timeoutMinutes,
        int maxParallelism)
    {
        return new SaveChangesDiagnostic
        {
            BatchIndex = batchIndex,
            TotalBatches = totalBatches,
            Tables = tables.Where(t => !string.IsNullOrWhiteSpace(t)).Distinct(StringComparer.OrdinalIgnoreCase).ToList(),
            SaveChangesTimeoutMinutes = timeoutMinutes,
            MaxParallelism = maxParallelism
        };
    }

    private static SaveChangesDiagnostic CloneDiagnostic(SaveChangesDiagnostic diagnostic)
    {
        return new SaveChangesDiagnostic
        {
            BatchIndex = diagnostic.BatchIndex,
            TotalBatches = diagnostic.TotalBatches,
            Tables = diagnostic.Tables.ToList(),
            SaveChangesTimeoutMinutes = diagnostic.SaveChangesTimeoutMinutes,
            MaxParallelism = diagnostic.MaxParallelism,
            IsSuccess = diagnostic.IsSuccess,
            ErrorMessage = diagnostic.ErrorMessage,
            ExceptionType = diagnostic.ExceptionType,
            FailureCategory = diagnostic.FailureCategory,
            FailureSource = diagnostic.FailureSource,
            MatchedSignals = diagnostic.MatchedSignals.ToList()
        };
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