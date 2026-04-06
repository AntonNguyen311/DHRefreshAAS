using Xunit;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Threading;
using System.Threading.Tasks;
using DHRefreshAAS;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;

namespace DHRefreshAAS.Tests;

/// <summary>
/// Unit tests that do not mock MSOLAP <see cref="Microsoft.AnalysisServices.Tabular.Server"/> (Connected and many APIs are not virtual).
/// </summary>
public class AasRefreshServiceTests
{
    private readonly Mock<IConfigurationService> _mockConfig;
    private readonly Mock<IConnectionService> _mockConnectionService;
    private readonly Mock<IAasScalingService> _mockScalingService;
    private readonly Mock<IElasticPoolScalingService> _mockElasticPoolScalingService;
    private readonly Mock<ILogger<AasRefreshService>> _mockLogger;
    private readonly Mock<IOperationStorageService> _mockOperationStorage;
    private readonly Mock<RowCountQueryService> _mockRowCountQueryService;
    private readonly Mock<SlowTableMetricsService> _mockSlowTableMetricsService;
    private readonly AasRefreshService _service;

    public AasRefreshServiceTests()
    {
        _mockConfig = new Mock<IConfigurationService>();
        _mockConfig.Setup(x => x.HeartbeatIntervalSeconds).Returns(3600);
        _mockConnectionService = new Mock<IConnectionService>();
        _mockScalingService = new Mock<IAasScalingService>();
        _mockElasticPoolScalingService = new Mock<IElasticPoolScalingService>();
        _mockLogger = new Mock<ILogger<AasRefreshService>>();
        _mockOperationStorage = new Mock<IOperationStorageService>();
        _mockRowCountQueryService = new Mock<RowCountQueryService>(_mockConnectionService.Object, Mock.Of<ILogger<RowCountQueryService>>());
        _mockSlowTableMetricsService = new Mock<SlowTableMetricsService>(_mockConfig.Object);
        _service = new AasRefreshService(_mockConfig.Object, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, Mock.Of<IRefreshConcurrencyService>(), _mockOperationStorage.Object, _mockRowCountQueryService.Object, _mockSlowTableMetricsService.Object, _mockLogger.Object);
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesService()
    {
        var service = new AasRefreshService(_mockConfig.Object, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, Mock.Of<IRefreshConcurrencyService>(), _mockOperationStorage.Object, _mockRowCountQueryService.Object, _mockSlowTableMetricsService.Object, _mockLogger.Object);
        Assert.NotNull(service);
    }

    [Fact]
    public void Constructor_NullConfig_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AasRefreshService(null!, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, Mock.Of<IRefreshConcurrencyService>(), _mockOperationStorage.Object, _mockRowCountQueryService.Object, _mockSlowTableMetricsService.Object, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_NullConnectionService_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AasRefreshService(_mockConfig.Object, null!, _mockScalingService.Object, _mockElasticPoolScalingService.Object, Mock.Of<IRefreshConcurrencyService>(), _mockOperationStorage.Object, _mockRowCountQueryService.Object, _mockSlowTableMetricsService.Object, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new AasRefreshService(_mockConfig.Object, _mockConnectionService.Object, _mockScalingService.Object, _mockElasticPoolScalingService.Object, Mock.Of<IRefreshConcurrencyService>(), _mockOperationStorage.Object, _mockRowCountQueryService.Object, _mockSlowTableMetricsService.Object, null!));
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

    [Fact]
    public async Task ExecuteRefreshWithRetryAsync_SingleRetryAttempt_ReturnsFailureWithoutPolicyValidationError()
    {
        var requestData = CreateValidEnhancedPostData();
        requestData.MaxRetryAttempts = 1;

        _mockConnectionService
            .Setup(x => x.CreateServerConnectionAsync(It.IsAny<CancellationToken>(), It.IsAny<int>(), It.IsAny<int>()))
            .ThrowsAsync(new Exception("Single-attempt connection failure"));

        var result = await _service.ExecuteRefreshWithRetryAsync(requestData, CancellationToken.None);

        Assert.NotNull(result);
        Assert.False(result.IsSuccess);
        Assert.Contains("All retry attempts failed", result.Message);
        Assert.Contains("Single-attempt connection failure", result.Message);
        Assert.DoesNotContain("Attempt count must be greater than zero", result.Message, StringComparison.OrdinalIgnoreCase);
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
