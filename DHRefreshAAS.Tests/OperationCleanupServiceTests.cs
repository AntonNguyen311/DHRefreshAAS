using DHRefreshAAS.Enums;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DHRefreshAAS.Tests;

public class OperationCleanupServiceTests
{
    private readonly Mock<OperationStorageService> _mockOperationStorage;
    private readonly Mock<ConfigurationService> _mockConfig;
    private readonly OperationCleanupService _service;

    public OperationCleanupServiceTests()
    {
        _mockOperationStorage = new Mock<OperationStorageService>(Mock.Of<ILogger<OperationStorageService>>());
        _mockConfig = new Mock<ConfigurationService>(Mock.Of<IConfiguration>(), Mock.Of<ILogger<ConfigurationService>>());
        _mockConfig.SetupGet(x => x.ZombieTimeoutMinutes).Returns(30);
        _service = new OperationCleanupService(_mockOperationStorage.Object, _mockConfig.Object, Mock.Of<ILogger<OperationCleanupService>>());
    }

    [Fact]
    public async Task StopAsync_ReleasesQueueLeaseForTrackedOperations()
    {
        _mockOperationStorage
            .Setup(x => x.MarkOperationAsFailedAsync("op-1", It.IsAny<string>()))
            .ReturnsAsync(true);
        _mockOperationStorage
            .Setup(x => x.ReleaseQueueLeaseForOperationAsync("op-1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _service.TrackOperation("op-1");

        await _service.StopAsync(CancellationToken.None);

        _mockOperationStorage.Verify(x => x.MarkOperationAsFailedAsync("op-1", It.IsAny<string>()), Times.Once);
        _mockOperationStorage.Verify(x => x.ReleaseQueueLeaseForOperationAsync("op-1", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAsync_CleansUpStaleRunningOperationsAndReleasesLeases()
    {
        var zombie = new OperationStatus
        {
            OperationId = "op-zombie",
            Status = OperationStatusEnum.Running,
            StartTime = DateTime.UtcNow.AddHours(-2),
            QueueScope = "aas:vnaassasdpp01",
            LeaseOwner = "lease-owner",
            LeaseHeartbeatTime = DateTime.UtcNow.AddHours(-2)
        };

        _mockOperationStorage
            .Setup(x => x.GetRunningOperationsAsync())
            .ReturnsAsync(new List<OperationStatus> { zombie });
        _mockOperationStorage
            .Setup(x => x.GetStaleRunningOperationsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<OperationStatus> { zombie });
        _mockOperationStorage
            .Setup(x => x.MarkOperationAsFailedAsync("op-zombie", It.IsAny<string>()))
            .ReturnsAsync(true);
        _mockOperationStorage
            .Setup(x => x.ReleaseQueueLeaseForOperationAsync("op-zombie", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        await _service.StartAsync(CancellationToken.None);

        _mockOperationStorage.Verify(x => x.GetStaleRunningOperationsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Once);
        _mockOperationStorage.Verify(x => x.MarkOperationAsFailedAsync("op-zombie", It.IsAny<string>()), Times.Once);
        _mockOperationStorage.Verify(x => x.ReleaseQueueLeaseForOperationAsync("op-zombie", It.IsAny<CancellationToken>()), Times.Once);
    }
}
