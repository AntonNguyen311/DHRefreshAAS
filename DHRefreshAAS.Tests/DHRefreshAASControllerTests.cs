using Xunit;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using System.Text.Json;
using DHRefreshAAS;
using DHRefreshAAS.Controllers;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;

namespace DHRefreshAAS.Tests;

public class DHRefreshAASControllerTests
{
    private readonly Mock<ConfigurationService> _mockConfig;
    private readonly Mock<ConnectionService> _mockConnectionService;
    private readonly Mock<AasRefreshService> _mockAasRefreshService;
    private readonly Mock<AasScalingService> _mockScalingService;
    private readonly Mock<ElasticPoolScalingService> _mockElasticPoolScalingService;
    private readonly Mock<OperationStorageService> _mockOperationStorage;
    private readonly Mock<ProgressTrackingService> _mockProgressTracking;
    private readonly Mock<ErrorHandlingService> _mockErrorHandling;
    private readonly Mock<RequestProcessingService> _mockRequestProcessing;
    private readonly Mock<ResponseService> _mockResponseService;
    private readonly Mock<OperationCleanupService> _mockCleanupService;
    private readonly Mock<IHostApplicationLifetime> _mockHostLifetime;
    private readonly Mock<ILogger<DHRefreshAASController>> _mockLogger;
    private readonly DHRefreshAASController _controller;

    public DHRefreshAASControllerTests()
    {
        _mockConfig = new Mock<ConfigurationService>(Mock.Of<IConfiguration>(), Mock.Of<ILogger<ConfigurationService>>());
        _mockConnectionService = new Mock<ConnectionService>(_mockConfig.Object, Mock.Of<ILogger<ConnectionService>>());
        _mockAasRefreshService = new Mock<AasRefreshService>(_mockConfig.Object, _mockConnectionService.Object, new Mock<AasScalingService>(_mockConfig.Object, Mock.Of<ILogger<AasScalingService>>()).Object, new Mock<ElasticPoolScalingService>(_mockConfig.Object, Mock.Of<ILogger<ElasticPoolScalingService>>()).Object, new RefreshConcurrencyService(Mock.Of<ILogger<RefreshConcurrencyService>>()), Mock.Of<ILogger<AasRefreshService>>());
        _mockScalingService = new Mock<AasScalingService>(_mockConfig.Object, Mock.Of<ILogger<AasScalingService>>());
        _mockElasticPoolScalingService = new Mock<ElasticPoolScalingService>(_mockConfig.Object, Mock.Of<ILogger<ElasticPoolScalingService>>());
        _mockOperationStorage = new Mock<OperationStorageService>(Mock.Of<ILogger<OperationStorageService>>());
        _mockProgressTracking = new Mock<ProgressTrackingService>(Mock.Of<ILogger<ProgressTrackingService>>());
        _mockErrorHandling = new Mock<ErrorHandlingService>(Mock.Of<ILogger<ErrorHandlingService>>());
        _mockRequestProcessing = new Mock<RequestProcessingService>(Mock.Of<ILogger<RequestProcessingService>>());
        _mockResponseService = new Mock<ResponseService>();
        _mockCleanupService = new Mock<OperationCleanupService>(_mockOperationStorage.Object, _mockConfig.Object, Mock.Of<ILogger<OperationCleanupService>>());
        _mockHostLifetime = new Mock<IHostApplicationLifetime>();
        _mockHostLifetime.Setup(l => l.ApplicationStopping).Returns(CancellationToken.None);
        _mockLogger = new Mock<ILogger<DHRefreshAASController>>();

        _controller = new DHRefreshAASController(
            _mockConfig.Object,
            _mockConnectionService.Object,
            _mockAasRefreshService.Object,
            _mockScalingService.Object,
            _mockElasticPoolScalingService.Object,
            _mockOperationStorage.Object,
            _mockProgressTracking.Object,
            _mockErrorHandling.Object,
            _mockRequestProcessing.Object,
            _mockResponseService.Object,
            _mockCleanupService.Object,
            _mockHostLifetime.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task TestToken_ValidRequest_ReturnsSuccessResponse()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();
        var testResult = new TokenTestResult { IsSuccessful = true };

        _mockConnectionService
            .Setup(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(testResult);

        var mockResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(It.IsAny<HttpRequestData>(), testResult, HttpStatusCode.OK))
            .ReturnsAsync(mockResponse);

        // Act
        var result = await _controller.TestToken(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()), Times.Once);
        _mockResponseService.Verify(x => x.CreateSuccessResponseAsync(It.IsAny<HttpRequestData>(), testResult, HttpStatusCode.OK), Times.Once);
    }

    [Fact]
    public async Task TestToken_ExceptionThrown_ReturnsErrorResponse()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();
        var exception = new Exception("Connection failed");

        _mockConnectionService
            .Setup(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        var mockErrorResponse = CreateMockHttpResponse(HttpStatusCode.InternalServerError);
        _mockErrorHandling
            .Setup(x => x.HandleExceptionAsync(It.IsAny<HttpRequestData>(), exception, "token acquisition test"))
            .ReturnsAsync(mockErrorResponse);

        // Act
        var result = await _controller.TestToken(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.InternalServerError, result.StatusCode);
        _mockErrorHandling.Verify(x => x.HandleExceptionAsync(It.IsAny<HttpRequestData>(), exception, "token acquisition test"), Times.Once);
    }

    [Fact]
    public async Task TestConnection_ValidRequest_ReturnsSuccessResponse()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();
        var testResult = new ConnectionTestResult { IsSuccessful = true };

        _mockConnectionService
            .Setup(x => x.TestConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(testResult);

        var mockResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(It.IsAny<HttpRequestData>(), testResult, HttpStatusCode.OK))
            .ReturnsAsync(mockResponse);

        // Act
        var result = await _controller.TestConnection(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
        _mockResponseService.Verify(x => x.CreateSuccessResponseAsync(It.IsAny<HttpRequestData>(), testResult, HttpStatusCode.OK), Times.Once);
    }

    [Fact]
    public async Task HttpStart_ValidRequest_StartsAsyncOperation()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();
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

        var mockAcceptedResponse = CreateMockHttpResponse(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(It.IsAny<HttpRequestData>(), It.IsAny<string>(), 15))
            .ReturnsAsync(mockAcceptedResponse);

        // Act
        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockRequestProcessing.Verify(x => x.ParseAndValidateRequestAsync(It.IsAny<HttpRequestData>()), Times.Once);
        _mockOperationStorage.Verify(x => x.UpsertOperationAsync(It.IsAny<OperationStatus>()), Times.Once);
    }

    [Fact]
    public async Task HttpStart_InvalidRequest_ReturnsValidationError()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        _mockRequestProcessing
            .Setup(x => x.ParseAndValidateRequestAsync(It.IsAny<HttpRequestData>()))
            .ReturnsAsync((PostData)null!);

        var mockErrorResponse = CreateMockHttpResponse(HttpStatusCode.BadRequest);
        _mockErrorHandling
            .Setup(x => x.CreateValidationErrorResponseAsync(It.IsAny<HttpRequestData>(), "Invalid request data"))
            .ReturnsAsync(mockErrorResponse);

        // Act
        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, result.StatusCode);
        _mockErrorHandling.Verify(x => x.CreateValidationErrorResponseAsync(It.IsAny<HttpRequestData>(), "Invalid request data"), Times.Once);
    }

    [Fact]
    public async Task GetStatus_SpecificOperationId_ReturnsOperationStatus()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();
        var operationId = "test-operation-id";
        object? responsePayload = null;
        var operationStatus = new OperationStatus
        {
            OperationId = operationId,
            Status = OperationStatusEnum.Running,
            StartTime = DateTime.UtcNow,
            TablesCount = 5,
            TablesCompleted = 3,
            LastBatchIndex = 2,
            LastBatchTables = new List<string> { "TableA", "TableB" },
            LastBatchError = "Batch failed",
            LastBatchFailureCategory = "DataSourceOrConnectivity",
            LastBatchFailureSource = "AzureSQLOrDataSource"
        };

        // Mock query string parsing
        mockRequest.Setup(x => x.Url).Returns(new Uri($"http://localhost/api/status?operationId={operationId}"));

        _mockOperationStorage
            .Setup(x => x.GetOperationAsync(operationId))
            .ReturnsAsync(operationStatus);

        var mockStatusResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateStatusResponseAsync(It.IsAny<HttpRequestData>(), It.IsAny<object>()))
            .Callback<HttpRequestData, object>((_, payload) => responsePayload = payload)
            .ReturnsAsync(mockStatusResponse);

        // Act
        var result = await _controller.GetStatus(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockOperationStorage.Verify(x => x.GetOperationAsync(operationId), Times.Once);
        _mockProgressTracking.Verify(x => x.UpdateProgress(operationStatus), Times.Once);
        Assert.NotNull(responsePayload);

        var json = JsonSerializer.Serialize(responsePayload);
        using var doc = JsonDocument.Parse(json);
        var lastBatch = doc.RootElement.GetProperty("lastBatch");
        Assert.Equal(2, lastBatch.GetProperty("index").GetInt32());
        Assert.Equal(2, lastBatch.GetProperty("tables").GetArrayLength());
        Assert.Equal("TableA", lastBatch.GetProperty("tables")[0].GetString());
        Assert.Equal("TableB", lastBatch.GetProperty("tables")[1].GetString());
        Assert.Equal("Batch failed", lastBatch.GetProperty("error").GetString());
        Assert.Equal("DataSourceOrConnectivity", lastBatch.GetProperty("failureCategory").GetString());
        Assert.Equal("AzureSQLOrDataSource", lastBatch.GetProperty("failureSource").GetString());
    }

    [Fact]
    public async Task GetStatus_NoOperationId_ReturnsGeneralStatus()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        mockRequest.Setup(x => x.Url).Returns(new Uri("http://localhost/api/status"));

        var recentOperations = new List<OperationStatus>
        {
            new OperationStatus { OperationId = "op1", Status = OperationStatusEnum.Completed },
            new OperationStatus { OperationId = "op2", Status = OperationStatusEnum.Running }
        };

        var operationCounts = (total: 10, running: 3, completed: 6, failed: 1);

        _mockOperationStorage
            .Setup(x => x.GetRecentOperationsAsync(10))
            .ReturnsAsync(recentOperations);

        _mockOperationStorage
            .Setup(x => x.GetOperationCountsAsync())
            .ReturnsAsync(operationCounts);

        var mockStatusResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateStatusResponseAsync(It.IsAny<HttpRequestData>(), It.IsAny<object>()))
            .ReturnsAsync(mockStatusResponse);

        // Act
        var result = await _controller.GetStatus(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockOperationStorage.Verify(x => x.GetRecentOperationsAsync(10), Times.Once);
        _mockOperationStorage.Verify(x => x.GetOperationCountsAsync(), Times.Once);
    }

    [Fact]
    public async Task GetStatus_OperationNotFound_ReturnsNotFound()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();
        var operationId = "non-existent-operation";

        mockRequest.Setup(x => x.Url).Returns(new Uri($"http://localhost/api/status?operationId={operationId}"));

        _mockOperationStorage
            .Setup(x => x.GetOperationAsync(operationId))
            .ReturnsAsync((OperationStatus)null!);

        var mockNotFoundResponse = CreateMockHttpResponse(HttpStatusCode.NotFound);
        _mockResponseService
            .Setup(x => x.CreateNotFoundResponseAsync(It.IsAny<HttpRequestData>(), "Operation", operationId))
            .ReturnsAsync(mockNotFoundResponse);

        // Act
        var result = await _controller.GetStatus(mockRequest.Object, mockContext.Object);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, result.StatusCode);
        _mockResponseService.Verify(x => x.CreateNotFoundResponseAsync(It.IsAny<HttpRequestData>(), "Operation", operationId), Times.Once);
    }

    // Helper methods
    private Mock<HttpRequestData> CreateMockHttpRequest() => TestHttpHelpers.CreateHttpRequestMock();

    private Mock<FunctionContext> CreateMockFunctionContext() => TestHttpHelpers.CreateFunctionContextMock();

    private static HttpResponseData CreateMockHttpResponse(HttpStatusCode statusCode) =>
        TestHttpHelpers.CreateHttpResponseData(statusCode);
}
