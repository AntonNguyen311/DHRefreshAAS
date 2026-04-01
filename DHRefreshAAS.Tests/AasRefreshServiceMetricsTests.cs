using System.Reflection;
using DHRefreshAAS.Models;
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
        var method = typeof(AasRefreshService).GetMethod("ClassifySlowTableSeverity", BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(method);

        var result = method!.Invoke(null, new object?[] { seconds, warnSec, critSec });

        Assert.Equal(expected, Assert.IsType<string>(result));
    }

    [Fact]
    public void ResolveRowCount_PrefersPartitionSpecificCount()
    {
        var method = typeof(AasRefreshService).GetMethod("ResolveRowCount", BindingFlags.NonPublic | BindingFlags.Static);
        var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Sales"] = 100,
            ["Sales|P202603"] = 25
        };

        Assert.NotNull(method);

        var result = method!.Invoke(null, new object?[] { rowCounts, "Sales", "P202603" });

        Assert.Equal(25L, Assert.IsType<long>(result));
    }

    [Fact]
    public void ResolveRowCount_FallsBackToTableCount_WhenPartitionSpecificMissing()
    {
        var method = typeof(AasRefreshService).GetMethod("ResolveRowCount", BindingFlags.NonPublic | BindingFlags.Static);
        var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Sales"] = 100
        };

        Assert.NotNull(method);

        var result = method!.Invoke(null, new object?[] { rowCounts, "Sales", "P202603" });

        Assert.Equal(100L, Assert.IsType<long>(result));
    }
}
