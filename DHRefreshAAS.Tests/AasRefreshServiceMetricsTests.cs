using Xunit;

namespace DHRefreshAAS.Tests;

public class AasRefreshServiceMetricsTests
{
    [Theory]
    [InlineData(null, 120, 300, "normal")]
    [InlineData(0d, 120, 300, "normal")]
    [InlineData(33.5d, 120, 300, "normal")]
    [InlineData(120d, 120, 300, "warning")]
    [InlineData(300d, 120, 300, "critical")]
    public void ClassifySlowTableSeverity_ReturnsExpectedBand(double? seconds, int warnSec, int critSec, string expected)
    {
        var result = SlowTableMetricsService.ClassifySlowTableSeverity(seconds, warnSec, critSec);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ResolveRowCount_PrefersPartitionSpecificCount()
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
    public void ResolveRowCount_FallsBackToTableCount_WhenPartitionSpecificMissing()
    {
        var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Sales"] = 100
        };

        var result = RowCountQueryService.ResolveRowCount(rowCounts, "Sales", "P202603");

        Assert.Equal(100L, result);
    }
}
