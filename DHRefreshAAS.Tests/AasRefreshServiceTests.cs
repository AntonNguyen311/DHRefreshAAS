using Xunit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Threading;
using System.Threading.Tasks;
using DHRefreshAAS;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Tests;

/// <summary>
/// Unit tests that do not mock MSOLAP <see cref="Microsoft.AnalysisServices.Tabular.Server"/> (Connected and many APIs are not virtual).
/// </summary>
public class AasRefreshServiceTests
{
    private readonly Mock<IConfiguration> _mockConfiguration;
    private readonly Mock<ILogger<ConfigurationService>> _mockConfigLogger;
    private readonly ConfigurationService _configService;
    private readonly Mock<ILogger<ConnectionService>> _mockConnectionLogger;
    private readonly Mock<ConnectionService> _mockConnectionService;
    private readonly Mock<ILogger<AasScalingService>> _mockScalingLogger;
    private readonly Mock<AasScalingService> _mockScalingService;
    private readonly Mock<ILogger<ElasticPoolScalingService>> _mockElasticPoolScalingLogger;
    private readonly Mock<ElasticPoolScalingService> _mockElasticPoolScalingService;
    private readonly Mock<ILogger<AasRefreshService>> _mockLogger;
    private readonly AasRefreshService _service;

    public AasRefreshServiceTests()
    {
        _mockConfiguration = new Mock<IConfiguration>();
        _mockConfigLogger = new Mock<ILogger<ConfigurationService>>();
        _configService = new ConfigurationService(_mockConfiguration.Object, _mockConfigLogger.Object);
        _mockConnectionLogger = new Mock<ILogger<ConnectionService>>();
        _mockConnectionService = new Mock<ConnectionService>(_configService, _mockConnectionLogger.Object);
        _mockScalingLogger = new Mock<ILogger<AasScalingService>>();
        _mockScalingService = new Mock<AasScalingService>(_configService, _mockScalingLogger.Object);
        _mockElasticPoolScalingLogger = new Mock<ILogger<ElasticPoolScalingService>>();
        _mockElasticPoolScalingService = new Mock<ElasticPoolScalingService>(_configService, _mockElasticPoolScalingLogger.Object);
        _mockLogger = new Mock<ILogger<AasRefreshService>>();
        _service = new AasRefreshService(_configService, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, new RefreshConcurrencyService(Mock.Of<ILogger<RefreshConcurrencyService>>()), _mockLogger.Object);
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesService()
    {
        var service = new AasRefreshService(_configService, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, new RefreshConcurrencyService(Mock.Of<ILogger<RefreshConcurrencyService>>()), _mockLogger.Object);
        Assert.NotNull(service);
    }

    [Fact]
    public void Constructor_NullConfig_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AasRefreshService(null!, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, new RefreshConcurrencyService(Mock.Of<ILogger<RefreshConcurrencyService>>()), _mockLogger.Object));
    }

    [Fact]
    public void Constructor_NullConnectionService_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AasRefreshService(_configService, null!, _mockScalingService.Object, _mockElasticPoolScalingService.Object, new RefreshConcurrencyService(Mock.Of<ILogger<RefreshConcurrencyService>>()), _mockLogger.Object));
    }

    [Fact]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AasRefreshService(_configService, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, new RefreshConcurrencyService(Mock.Of<ILogger<RefreshConcurrencyService>>()), null!));
    }

    [Fact]
    public async Task ExecuteRefreshWithRetryAsync_AllRetriesExhausted_ReturnsFailure()
    {
        var requestData = CreateValidEnhancedPostData();
        requestData.MaxRetryAttempts = 2;

        _mockConnectionService
            .Setup(x => x.CreateServerConnectionAsync(It.IsAny<CancellationToken>(), It.IsAny<int>(), It.IsAny<int>()))
            .ThrowsAsync(new Exception("Persistent connection failure"));

        var result = await _service.ExecuteRefreshWithRetryAsync(requestData, CancellationToken.None);

        Assert.NotNull(result);
        Assert.False(result.IsSuccess);
        Assert.Contains("All retry attempts failed", result.Message);
        Assert.Contains("Persistent connection failure", result.Message);
    }

    private static EnhancedPostData CreateValidEnhancedPostData()
    {
        var postData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[]
            {
                new RefreshObject { Table = "Table1" },
                new RefreshObject { Table = "Table2" }
            }
        };

        return new EnhancedPostData
        {
            OriginalRequest = postData,
            MaxRetryAttempts = 3,
            BaseDelaySeconds = 2,
            ConnectionTimeoutMinutes = 5,
            OperationTimeoutMinutes = 10
        };
    }
}
