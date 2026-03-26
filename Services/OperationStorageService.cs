using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

/// <summary>
/// Table entity for storing operation status in Azure Table Storage
/// </summary>
public class OperationEntity : ITableEntity
{
    public string PartitionKey { get; set; } = "operations";
    public string RowKey { get; set; } = "";
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }
    
    // Operation data
    public string OperationId { get; set; } = "";
    public string Status { get; set; } = "";
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string? Result { get; set; }
    public string? ErrorMessage { get; set; }
    public int TablesCount { get; set; }
    public double EstimatedDurationMinutes { get; set; }
    
    // Progress tracking
    public int TablesCompleted { get; set; } = 0;
    public int TablesFailed { get; set; } = 0;
    public int TablesInProgress { get; set; } = 0;
    public double ProgressPercentage { get; set; } = 0.0;
    public string CompletedTablesJson { get; set; } = "[]";
    public string FailedTablesJson { get; set; } = "[]";
    public string InProgressTablesJson { get; set; } = "[]";
    public string CurrentPhase { get; set; } = "Initializing";
    
    public OperationEntity() { }
    
    public OperationEntity(OperationStatus operation)
    {
        PartitionKey = "operations";
        RowKey = operation.OperationId;
        OperationId = operation.OperationId;
        Status = operation.Status;
        StartTime = operation.StartTime;
        EndTime = operation.EndTime;
        Result = operation.Result;
        ErrorMessage = operation.ErrorMessage;
        TablesCount = operation.TablesCount;
        EstimatedDurationMinutes = operation.EstimatedDurationMinutes;
        TablesCompleted = operation.TablesCompleted;
        TablesFailed = operation.TablesFailed;
        TablesInProgress = operation.TablesInProgress;
        ProgressPercentage = operation.ProgressPercentage;
        CompletedTablesJson = JsonSerializer.Serialize(operation.CompletedTables);
        FailedTablesJson = JsonSerializer.Serialize(operation.FailedTables);
        InProgressTablesJson = JsonSerializer.Serialize(operation.InProgressTables);
        CurrentPhase = operation.CurrentPhase;
    }
    
    public OperationStatus ToOperationStatus()
    {
        return new OperationStatus
        {
            OperationId = OperationId,
            Status = Status,
            StartTime = StartTime,
            EndTime = EndTime,
            Result = Result,
            ErrorMessage = ErrorMessage,
            TablesCount = TablesCount,
            EstimatedDurationMinutes = EstimatedDurationMinutes,
            TablesCompleted = TablesCompleted,
            TablesFailed = TablesFailed,
            TablesInProgress = TablesInProgress,
            ProgressPercentage = ProgressPercentage,
            CompletedTables = JsonSerializer.Deserialize<List<string>>(CompletedTablesJson) ?? new List<string>(),
            FailedTables = JsonSerializer.Deserialize<List<string>>(FailedTablesJson) ?? new List<string>(),
            InProgressTables = JsonSerializer.Deserialize<List<string>>(InProgressTablesJson) ?? new List<string>(),
            CurrentPhase = CurrentPhase
        };
    }
}

/// <summary>
/// Service for persistent operation storage using Azure Table Storage
/// </summary>
public class OperationStorageService
{
    private readonly TableClient _tableClient;
    private readonly ILogger<OperationStorageService> _logger;
    private const string TableName = "OperationStatus";
    
    public OperationStorageService(ILogger<OperationStorageService> logger)
    {
        _logger = logger;
        
        // Get connection string from environment variables
        var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage") 
                             ?? Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING")
                             ?? "UseDevelopmentStorage=true"; // Fallback to local storage emulator
        
        _tableClient = new TableClient(connectionString, TableName);
        
        // Create table if it doesn't exist
        _ = Task.Run(async () =>
        {
            try
            {
                await _tableClient.CreateIfNotExistsAsync();
                _logger.LogInformation("Operation storage table initialized: {TableName}", TableName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize operation storage table: {TableName}", TableName);
            }
        });
    }
    
    /// <summary>
    /// Store or update an operation
    /// </summary>
    public virtual async Task<bool> UpsertOperationAsync(OperationStatus operation)
    {
        try
        {
            var entity = new OperationEntity(operation);
            await _tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace);
            _logger.LogDebug("Operation {OperationId} stored successfully", operation.OperationId);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store operation {OperationId}: {ErrorMessage}", operation.OperationId, ex.Message);
            return false;
        }
    }
    
    /// <summary>
    /// Retrieve an operation by ID
    /// </summary>
    public virtual async Task<OperationStatus?> GetOperationAsync(string operationId)
    {
        try
        {
            var response = await _tableClient.GetEntityAsync<OperationEntity>("operations", operationId);
            return response.Value.ToOperationStatus();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            _logger.LogDebug("Operation {OperationId} not found", operationId);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve operation {OperationId}: {ErrorMessage}", operationId, ex.Message);
            return null;
        }
    }
    
    /// <summary>
    /// Get recent operations (for general status endpoint)
    /// </summary>
    public virtual async Task<List<OperationStatus>> GetRecentOperationsAsync(int count = 10)
    {
        try
        {
            var operations = new List<OperationStatus>();
            
            await foreach (var entity in _tableClient.QueryAsync<OperationEntity>(
                filter: $"PartitionKey eq 'operations'",
                maxPerPage: count))
            {
                operations.Add(entity.ToOperationStatus());
            }
            
            return operations.OrderByDescending(op => op.StartTime).Take(count).ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve recent operations: {ErrorMessage}", ex.Message);
            return new List<OperationStatus>();
        }
    }
    
    /// <summary>
    /// Get operation counts by status
    /// </summary>
    public virtual async Task<(int running, int completed, int failed, int total)> GetOperationCountsAsync()
    {
        try
        {
            var running = 0;
            var completed = 0;
            var failed = 0;
            var total = 0;
            
            await foreach (var entity in _tableClient.QueryAsync<OperationEntity>(
                filter: $"PartitionKey eq 'operations'",
                select: new[] { "Status" }))
            {
                total++;
                switch (entity.Status.ToLowerInvariant())
                {
                    case "running":
                        running++;
                        break;
                    case "completed":
                        completed++;
                        break;
                    case "failed":
                        failed++;
                        break;
                }
            }
            
            return (running, completed, failed, total);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get operation counts: {ErrorMessage}", ex.Message);
            return (0, 0, 0, 0);
        }
    }
}

