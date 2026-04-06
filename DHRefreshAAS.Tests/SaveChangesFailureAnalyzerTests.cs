using Xunit;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Tests;

public class SaveChangesFailureAnalyzerTests
{
    [Fact]
    public void AnalyzeSaveChangesFailure_Timeout_ReturnsTimeoutCategory()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(null, "some error", timedOut: true, canceled: false);
        Assert.Equal("Timeout", category);
        Assert.Equal("Unknown", source);
        Assert.Contains("save-changes-timeout", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_Canceled_ReturnsCanceledCategory()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(null, "some error", timedOut: false, canceled: true);
        Assert.Equal("Canceled", category);
        Assert.Equal("Unknown", source);
        Assert.Contains("operation-canceled", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_Deadlock_ReturnsDeadlockCategory()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(
            new Exception("A deadlock occurred"), "A deadlock occurred", timedOut: false, canceled: false);
        Assert.Equal("Deadlock", category);
        Assert.Equal("Unknown", source);
        Assert.Contains("deadlock", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_XmlaRequest_ReturnsServiceRestartOrNodeMove()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(
            new Exception("long running xmla request was interrupted"), null, timedOut: false, canceled: false);
        Assert.Equal("ServiceRestartOrNodeMove", category);
        Assert.Equal("AAS", source);
        Assert.Contains("xmla-request-interrupted", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_ServerRestart_ReturnsServiceRestartOrNodeMove()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(
            new Exception("server restart detected"), null, timedOut: false, canceled: false);
        Assert.Equal("ServiceRestartOrNodeMove", category);
        Assert.Equal("AAS", source);
        Assert.Contains("server-restart", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_OutOfMemory_ReturnsCapacityOrMemory()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(
            new Exception("out of memory error"), null, timedOut: false, canceled: false);
        Assert.Equal("CapacityOrMemory", category);
        Assert.Equal("AAS", source);
        Assert.Contains("out-of-memory", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_SqlTimeout_ReturnsDataSourceOrConnectivity()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(
            new Exception("timeout expired"), null, timedOut: false, canceled: false);
        Assert.Equal("DataSourceOrConnectivity", category);
        Assert.Equal("AzureSQLOrDataSource", source);
        Assert.Contains("sql-timeout", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_LoginFailed_ReturnsDataSourceOrConnectivity()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(
            new Exception("login failed for user"), null, timedOut: false, canceled: false);
        Assert.Equal("DataSourceOrConnectivity", category);
        Assert.Equal("AzureSQLOrDataSource", source);
        Assert.Contains("sql-login-failed", signals);
    }

    [Fact]
    public void AnalyzeSaveChangesFailure_UnknownError_ReturnsUnknown()
    {
        var (category, source, signals) = SaveChangesFailureAnalyzer.AnalyzeSaveChangesFailure(
            new Exception("something weird happened"), null, timedOut: false, canceled: false);
        Assert.Equal("Unknown", category);
        Assert.Equal("Unknown", source);
        Assert.Empty(signals);
    }

    [Fact]
    public void FlattenExceptionMessages_SingleException_ReturnsTrimmedMessage()
    {
        var ex = new Exception("  Root error  ");
        var result = SaveChangesFailureAnalyzer.FlattenExceptionMessages(ex);
        Assert.Equal("Root error", result);
    }

    [Fact]
    public void FlattenExceptionMessages_NestedException_ReturnsChain()
    {
        var inner = new Exception("Inner error");
        var outer = new Exception("Outer error", inner);
        var result = SaveChangesFailureAnalyzer.FlattenExceptionMessages(outer);
        Assert.Equal("Outer error --> Inner error", result);
    }

    [Fact]
    public void FlattenExceptionMessages_DuplicateMessages_Deduplicates()
    {
        var inner = new Exception("Same message");
        var outer = new Exception("Same message", inner);
        var result = SaveChangesFailureAnalyzer.FlattenExceptionMessages(outer);
        Assert.Equal("Same message", result);
    }

    [Theory]
    [InlineData("deadlock detected")]
    [InlineData("was deadlocked on lock resources")]
    [InlineData("deadlock condition was detected")]
    public void IsDeadlockException_DeadlockMessages_ReturnsTrue(string message)
    {
        Assert.True(SaveChangesFailureAnalyzer.IsDeadlockException(new Exception(message)));
    }

    [Fact]
    public void IsDeadlockException_NonDeadlock_ReturnsFalse()
    {
        Assert.False(SaveChangesFailureAnalyzer.IsDeadlockException(new Exception("timeout expired")));
    }

    [Fact]
    public void PopulateFailureDiagnostic_SetsAllFields()
    {
        var diagnostic = new SaveChangesDiagnostic();
        var exception = new Exception("deadlock occurred");

        SaveChangesFailureAnalyzer.PopulateFailureDiagnostic(diagnostic, exception, "fallback", timedOut: false, canceled: false);

        Assert.False(diagnostic.IsSuccess);
        Assert.Equal("Exception", diagnostic.ExceptionType);
        Assert.Equal("deadlock occurred", diagnostic.ErrorMessage);
        Assert.Equal("Deadlock", diagnostic.FailureCategory);
        Assert.Equal("Unknown", diagnostic.FailureSource);
    }

    [Fact]
    public void PopulateFailureDiagnostic_NullException_UsesFallbackMessage()
    {
        var diagnostic = new SaveChangesDiagnostic();

        SaveChangesFailureAnalyzer.PopulateFailureDiagnostic(diagnostic, null, "fallback msg", timedOut: true, canceled: false);

        Assert.Equal("fallback msg", diagnostic.ErrorMessage);
        Assert.Equal("Timeout", diagnostic.FailureCategory);
    }

    [Fact]
    public void BuildBatchFailureMessage_FormatsCorrectly()
    {
        var diagnostic = new SaveChangesDiagnostic
        {
            BatchIndex = 2,
            TotalBatches = 3,
            FailureSource = "AAS",
            FailureCategory = "Timeout",
            ErrorMessage = "Timed out"
        };

        var result = SaveChangesFailureAnalyzer.BuildBatchFailureMessage(diagnostic);

        Assert.Equal("SaveChanges failed for batch 2/3 [AAS/Timeout]: Timed out", result);
    }

    [Fact]
    public void BuildBatchFailureMessage_NullFields_UsesDefaults()
    {
        var diagnostic = new SaveChangesDiagnostic
        {
            BatchIndex = 1,
            TotalBatches = 1
        };

        var result = SaveChangesFailureAnalyzer.BuildBatchFailureMessage(diagnostic);

        Assert.Contains("UnknownSource", result);
        Assert.Contains("UnknownCategory", result);
        Assert.Contains("No inner SaveChanges error details", result);
    }
}
