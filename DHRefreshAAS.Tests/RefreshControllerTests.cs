using Xunit;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using DHRefreshAAS.Controllers;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;

namespace DHRefreshAAS.Tests;

public class RefreshControllerTests
{
    private readonly Mock<IConfigurationService> _mockConfig;
    private readonly Mock<IConnectionService> _mockConnectionService;
    private readonly Mock<AasScalingService> _mockScalingService;
    private readonly Mock<ElasticPoolScalingService> _mockElasticPoolScalingService;
    private readonly Mock<IOperationStorageService> _mockOperationStorage;
    private readonly Mock<RequestProcessingService> _mockRequestProcessing;
    private readonly Mock<ResponseService> _mockResponseService;
    private readonly Mock<ErrorHandlingService> _mockErrorHandling;
    private readonly Mock<QueueExecutionService> _mockQueueExecution;
    private readonly Mock<StatusResponseBuilder> _mockStatusResponseBuilder;
    private readonly Mock<ILogger<RefreshController>> _mockLogger;
    private readonly RefreshController _controller;

    public RefreshControllerTests()
    {
        _mockConfig = new Mock<IConfigurationService>();
        _mockConnectionService = new Mock<IConnectionService>();
        _mockScalingService = new Mock<AasScalingService>(_mockConfig.Object, Mock.Of<ILogger<AasScalingService>>());
        _mockElasticPoolScalingService = new Mock<ElasticPoolScalingService>(_mockConfig.Object, Mock.Of<ILogger<ElasticPoolScalingService>>());
        _mockOperationStorage = new Mock<IOperationStorageService>();
        _mockRequestProcessing = new Mock<RequestProcessingService>(Mock.Of<ILogger<RequestProcessingService>>());
        _mockResponseService = new Mock<ResponseService>();
        _mockErrorHandling = new Mock<ErrorHandlingService>(Mock.Of<ILogger<ErrorHandlingService>>());

        _mockQueueExecution = new Mock<QueueExecutionService>(
            _mockConfig.Object, Mock.Of<IAasRefreshService>(), _mockOperationStorage.Object,
            new Mock<ProgressTrackingService>(Mock.Of<ILogger<ProgressTrackingService>>()).Object,
            new Mock<OperationCleanupService>(_mockOperationStorage.Object, _mockConfig.Object, Mock.Of<ILogger<OperationCleanupService>>()).Object,
            Mock.Of<Microsoft.Extensions.Hosting.IHostApplicationLifetime>(),
            Mock.Of<ILogger<QueueExecutionService>>());
        _mockStatusResponseBuilder = new Mock<StatusResponseBuilder>(
            _mockOperationStorage.Object,
            new Mock<ProgressTrackingService>(Mock.Of<ILogger<ProgressTrackingService>>()).Object,
            _mockResponseService.Object);
        _mockLogger = new Mock<ILogger<RefreshController>>();

        _controller = new RefreshController(
            _mockConfig.Object,
            _mockConnectionService.Object,
            _mockScalingService.Object,
            _mockElasticPoolScalingService.Object,
            _mockOperationStorage.Object,
            _mockRequestProcessing.Object,
            _mockResponseService.Object,
            _mockErrorHandling.Object,
            _mockQueueExecution.Object,
            _mockStatusResponseBuilder.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task HttpStart_ValidRequest_DelegatesToQueueExecution()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var requestData = new PostData
        {
            DatabaseName = "Db",
            RefreshObjects = new[] { new RefreshObject { Table = "TestTable" } },
            OperationTimeoutMinutes = 30
        };
        var enhancedRequestData = new EnhancedPostData
        {
            OriginalRequest = requestData,
            MaxRetryAttempts = 3,
            BaseDelaySeconds = 2,
            ConnectionTimeoutMinutes = 10,
            OperationTimeoutMinutes = 30
        };

        _mockRequestProcessing
            .Setup(x => x.ParseAndValidateRequestAsync(It.IsAny<HttpRequestData>()))
            .ReturnsAsync(requestData);
        _mockRequestProcessing
            .Setup(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object))
            .Returns(enhancedRequestData);
        _mockRequestProcessing
            .Setup(x => x.EstimateOperationDuration(requestData))
            .Returns(15);

        var queueResult = new QueueOperationResult
        {
            OperationId = "op-123",
            EstimatedDurationMinutes = 15,
            StartedImmediately = true,
            Status = OperationStatusEnum.Running,
            Message = "Started",
            QueuePosition = null,
            QueueScope = "aas:server:db"
        };
        _mockQueueExecution
            .Setup(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 15, null, "api"))
            .ReturnsAsync(queueResult);

        var mockAcceptedResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(
                It.IsAny<HttpRequestData>(), "op-123", 15,
                OperationStatusEnum.Running, "Started", null, "aas:server:db"))
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockQueueExecution.Verify(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 15, null, "api"), Times.Once);
    }

    [Fact]
    public async Task HttpStart_QueuedBehind_ReturnsQueuedStatus()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var requestData = new PostData
        {
            DatabaseName = "Db",
            RefreshObjects = new[] { new RefreshObject { Table = "TestTable" } }
        };
        var enhancedRequestData = new EnhancedPostData { OriginalRequest = requestData };

        _mockRequestProcessing
            .Setup(x => x.ParseAndValidateRequestAsync(It.IsAny<HttpRequestData>()))
            .ReturnsAsync(requestData);
        _mockRequestProcessing
            .Setup(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object))
            .Returns(enhancedRequestData);
        _mockRequestProcessing
            .Setup(x => x.EstimateOperationDuration(requestData))
            .Returns(15);

        string? capturedStatus = null;
        _mockQueueExecution
            .Setup(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 15, null, "api"))
            .ReturnsAsync(new QueueOperationResult
            {
                OperationId = "op-456",
                EstimatedDurationMinutes = 15,
                StartedImmediately = false,
                Status = OperationStatusEnum.Queued,
                Message = "Queued",
                QueuePosition = 2,
                QueueScope = "aas:server:db"
            });

        var mockAcceptedResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(
                It.IsAny<HttpRequestData>(), It.IsAny<string>(), 15,
                It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<int?>(), It.IsAny<string?>()))
            .Callback<HttpRequestData, string, int, string, string?, int?, string?>((_, _, _, status, _, _, _) => capturedStatus = status)
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        Assert.Equal(OperationStatusEnum.Queued, capturedStatus);
    }

    [Fact]
    public async Task HttpStart_InvalidRequest_ReturnsValidationError()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

        _mockRequestProcessing
            .Setup(x => x.ParseAndValidateRequestAsync(It.IsAny<HttpRequestData>()))
            .ReturnsAsync((PostData)null!);

        var mockErrorResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.BadRequest);
        _mockErrorHandling
            .Setup(x => x.CreateValidationErrorResponseAsync(It.IsAny<HttpRequestData>(), "Invalid request data"))
            .ReturnsAsync(mockErrorResponse);

        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.BadRequest, result.StatusCode);
    }

    [Fact]
    public async Task GetStatus_SpecificOperationId_DelegatesToStatusResponseBuilder()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var operationId = "test-operation-id";

        mockRequest.Setup(x => x.Url).Returns(new Uri($"http://localhost/api/status?operationId={operationId}"));

        var mockStatusResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockStatusResponseBuilder
            .Setup(x => x.GetSpecificOperationStatusAsync(operationId, It.IsAny<HttpRequestData>(), null, false))
            .ReturnsAsync(mockStatusResponse);

        var result = await _controller.GetStatus(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockStatusResponseBuilder.Verify(
            x => x.GetSpecificOperationStatusAsync(operationId, It.IsAny<HttpRequestData>(), null, false), Times.Once);
    }

    [Fact]
    public async Task GetStatus_NoOperationId_DelegatesToGeneralStatus()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

        mockRequest.Setup(x => x.Url).Returns(new Uri("http://localhost/api/status"));

        var mockStatusResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockStatusResponseBuilder
            .Setup(x => x.GetGeneralStatusAsync(It.IsAny<HttpRequestData>(), null, false))
            .ReturnsAsync(mockStatusResponse);

        var result = await _controller.GetStatus(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockStatusResponseBuilder.Verify(
            x => x.GetGeneralStatusAsync(It.IsAny<HttpRequestData>(), null, false), Times.Once);
    }
}
