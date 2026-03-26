using Xunit;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;
using Microsoft.Extensions.Logging.Abstractions;

namespace DHRefreshAAS.Tests;

public class ProgressTrackingServiceTests
{
    private static ProgressTrackingService CreateService() =>
        new(NullLogger<ProgressTrackingService>.Instance);

    private static OperationStatus CreateOperation(int tableCount, params string[] tableNames)
    {
        var refresh = tableNames.Select(t => new RefreshObject { Table = t }).ToArray();
        return new OperationStatus
        {
            OperationId = "test-op",
            Status = OperationStatusEnum.Running,
            TablesCount = tableCount,
            RefreshObjects = refresh
        };
    }

    [Fact]
    public void InitializeProgress_ShouldSetPhaseAndSeedInProgressTables()
    {
        var service = CreateService();
        var op = CreateOperation(2, "T1", "T2");

        service.InitializeProgress(op);

        Assert.Equal(OperationPhaseEnum.ProcessingTables, op.CurrentPhase);
        Assert.Equal(2, op.InProgressTables.Count);
        Assert.Contains("T1", op.InProgressTables);
        Assert.Contains("T2", op.InProgressTables);
        Assert.True(op.ProgressPercentage >= 0);
    }

    [Fact]
    public void CompleteTable_ShouldIncrementCompletedAndAdjustProgress()
    {
        var service = CreateService();
        var op = CreateOperation(2, "T1", "T2");
        service.InitializeProgress(op);

        service.CompleteTable(op, "T1");

        Assert.Equal(1, op.TablesCompleted);
        Assert.Contains("T1", op.CompletedTables);
        Assert.DoesNotContain("T1", op.InProgressTables);
        Assert.Equal(50.0, op.ProgressPercentage);
    }

    [Fact]
    public void FailTable_ShouldIncrementFailedAndRecordMessage()
    {
        var service = CreateService();
        var op = CreateOperation(1, "BadTable");
        service.InitializeProgress(op);

        service.FailTable(op, "BadTable", "timeout");

        Assert.Equal(1, op.TablesFailed);
        Assert.Contains("BadTable: timeout", op.FailedTables);
    }

    [Fact]
    public void UpdateProgress_WhenAllTablesProcessedWhileRunning_ShouldSetSavingChangesPhase()
    {
        var service = CreateService();
        var op = CreateOperation(1, "Only");
        service.InitializeProgress(op);
        service.CompleteTable(op, "Only");

        Assert.Equal(OperationPhaseEnum.SavingChanges, op.CurrentPhase);
        Assert.Equal(95.0, op.ProgressPercentage);
    }

    [Fact]
    public void ShouldBeCompleted_IsTrueWhenAllTablesDoneAndStillRunning()
    {
        var service = CreateService();
        var op = CreateOperation(1, "Only");
        service.InitializeProgress(op);
        service.CompleteTable(op, "Only");

        Assert.True(service.IsAllTablesProcessed(op));
        Assert.True(service.ShouldBeCompleted(op));
    }
}
