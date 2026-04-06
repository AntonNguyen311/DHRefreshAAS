using Xunit;

namespace DHRefreshAAS.Tests;

public class RowCountQueryServiceTests
{
    [Fact]
    public void ResolveRowCount_PartitionKey_ReturnsPartitionCount()
    {
        var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Sales"] = 100,
            ["Sales|P202603"] = 25
        };

        var result = RowCountQueryService.ResolveRowCount(rowCounts, "Sales", "P202603");

        Assert.Equal(25L, result);
    }

    [Fact]
    public void ResolveRowCount_NoPartitionKey_ReturnsTableCount()
    {
        var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Sales"] = 100,
            ["Sales|P202603"] = 25
        };

        var result = RowCountQueryService.ResolveRowCount(rowCounts, "Sales", null);

        Assert.Equal(100L, result);
    }

    [Fact]
    public void ResolveRowCount_MissingPartition_FallsBackToTableCount()
    {
        var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Sales"] = 100
        };

        var result = RowCountQueryService.ResolveRowCount(rowCounts, "Sales", "NonExistent");

        Assert.Equal(100L, result);
    }

    [Fact]
    public void ResolveRowCount_MissingTable_ReturnsNull()
    {
        var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Sales"] = 100
        };

        var result = RowCountQueryService.ResolveRowCount(rowCounts, "Orders", null);

        Assert.Null(result);
    }

    [Fact]
    public void BuildRowCountKey_TableOnly_ReturnsTableName()
    {
        var result = RowCountQueryService.BuildRowCountKey("Sales", null);
        Assert.Equal("Sales", result);
    }

    [Fact]
    public void BuildRowCountKey_WithPartition_ReturnsPipeDelimited()
    {
        var result = RowCountQueryService.BuildRowCountKey("Sales", "P202603");
        Assert.Equal("Sales|P202603", result);
    }

    [Fact]
    public void BuildRowCountKey_WhitespacePartition_ReturnsTableOnly()
    {
        var result = RowCountQueryService.BuildRowCountKey("Sales", "  ");
        Assert.Equal("Sales", result);
    }

    [Fact]
    public void BuildRowCountKey_TrimsWhitespace()
    {
        var result = RowCountQueryService.BuildRowCountKey("  Sales  ", "  P1  ");
        Assert.Equal("Sales|P1", result);
    }

    [Fact]
    public void SetMaximumRowCount_NewKey_SetsValue()
    {
        var results = new Dictionary<string, long>();
        RowCountQueryService.SetMaximumRowCount(results, "Sales", 100);
        Assert.Equal(100L, results["Sales"]);
    }

    [Fact]
    public void SetMaximumRowCount_LargerValue_Updates()
    {
        var results = new Dictionary<string, long> { ["Sales"] = 50 };
        RowCountQueryService.SetMaximumRowCount(results, "Sales", 100);
        Assert.Equal(100L, results["Sales"]);
    }

    [Fact]
    public void SetMaximumRowCount_SmallerValue_DoesNotUpdate()
    {
        var results = new Dictionary<string, long> { ["Sales"] = 100 };
        RowCountQueryService.SetMaximumRowCount(results, "Sales", 50);
        Assert.Equal(100L, results["Sales"]);
    }
}
