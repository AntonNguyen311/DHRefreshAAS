using Xunit;

namespace DHRefreshAAS.Tests;

public class SlowTableMetricsServiceTests
{
    [Theory]
    [InlineData(null, 120, 300, "normal")]
    [InlineData(0d, 120, 300, "normal")]
    [InlineData(-5d, 120, 300, "normal")]
    [InlineData(33.5d, 120, 300, "normal")]
    [InlineData(119.9d, 120, 300, "normal")]
    [InlineData(120d, 120, 300, "warning")]
    [InlineData(200d, 120, 300, "warning")]
    [InlineData(299.9d, 120, 300, "warning")]
    [InlineData(300d, 120, 300, "critical")]
    [InlineData(500d, 120, 300, "critical")]
    public void ClassifySlowTableSeverity_ReturnsExpectedBand(double? seconds, int warnSec, int critSec, string expected)
    {
        var result = SlowTableMetricsService.ClassifySlowTableSeverity(seconds, warnSec, critSec);
        Assert.Equal(expected, result);
    }
}
