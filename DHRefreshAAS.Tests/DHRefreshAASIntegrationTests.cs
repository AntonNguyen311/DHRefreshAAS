using Xunit;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using DHRefreshAAS;
using DHRefreshAAS.Controllers;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;

namespace DHRefreshAAS.Tests;

public class DHRefreshAASIntegrationTests
{
    private readonly Mock<ConfigurationService> _mockConfig;
    private readonly Mock<ConnectionService> _mockConnectionService;
    private readonly Mock<AasRefreshService> _mockAasRefreshService;
    private readonly Mock<OperationStorageService> _mockOperationStorage;
    private readonly Mock<ProgressTrackingService> _mockProgressTracking;
    private readonly Mock<ErrorHandlingService> _mockErrorHandling;
    private readonly Mock<RequestProcessingService> _mockRequestProcessing;
    private readonly Mock<ResponseService> _mockResponseService;
    private readonly Mock<IHostApplicationLifetime> _mockHostLifetime;
    private readonly Mock<ILogger<DHRefreshAASController>> _mockLogger;
    private readonly DHRefreshAASController _controller;

    public DHRefreshAASIntegrationTests()
    {
        _mockConfig = new Mock<ConfigurationService>(Mock.Of<IConfiguration>(), Mock.Of<ILogger<ConfigurationService>>());
        _mockConnectionService = new Mock<ConnectionService>(_mockConfig.Object, Mock.Of<ILogger<ConnectionService>>());
        _mockAasRefreshService = new Mock<AasRefreshService>(_mockConfig.Object, _mockConnectionService.Object, Mock.Of<ILogger<AasRefreshService>>());
        _mockOperationStorage = new Mock<OperationStorageService>(Mock.Of<ILogger<OperationStorageService>>());
        _mockProgressTracking = new Mock<ProgressTrackingService>(Mock.Of<ILogger<ProgressTrackingService>>());
        _mockErrorHandling = new Mock<ErrorHandlingService>(Mock.Of<ILogger<ErrorHandlingService>>());
        _mockRequestProcessing = new Mock<RequestProcessingService>(Mock.Of<ILogger<RequestProcessingService>>());
        _mockResponseService = new Mock<ResponseService>();
        _mockHostLifetime = new Mock<IHostApplicationLifetime>();
        _mockHostLifetime.Setup(l => l.ApplicationStopping).Returns(CancellationToken.None);
        _mockLogger = new Mock<ILogger<DHRefreshAASController>>();

        _controller = new DHRefreshAASController(
            _mockConfig.Object,
            _mockConnectionService.Object,
            _mockAasRefreshService.Object,
            _mockOperationStorage.Object,
            _mockProgressTracking.Object,
            _mockErrorHandling.Object,
            _mockRequestProcessing.Object,
            _mockResponseService.Object,
            _mockHostLifetime.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task EndToEnd_ValidRefreshRequest_CompleteFlow()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[]
            {
                new RefreshObject { Table = "Sales" },
                new RefreshObject { Table = "Inventory" }
            },
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
            .Setup(x => x.ParseAndValidateRequestAsync(mockRequest.Object))
            .ReturnsAsync(requestData);

        _mockRequestProcessing
            .Setup(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object))
            .Returns(enhancedRequestData);

        _mockRequestProcessing
            .Setup(x => x.EstimateOperationDuration(requestData))
            .Returns(20);

        _mockOperationStorage
            .Setup(x => x.UpsertOperationAsync(It.IsAny<OperationStatus>()))
            .ReturnsAsync(true);

        var mockAcceptedResponse = CreateMockHttpResponse(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(mockRequest.Object, It.IsAny<string>(), 20))
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockRequestProcessing.Verify(x => x.ParseAndValidateRequestAsync(mockRequest.Object), Times.Once);
        _mockRequestProcessing.Verify(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object), Times.Once);
        _mockRequestProcessing.Verify(x => x.EstimateOperationDuration(requestData), Times.Once);
        _mockOperationStorage.Verify(x => x.UpsertOperationAsync(It.IsAny<OperationStatus>()), Times.Once);
        _mockProgressTracking.Verify(x => x.InitializeProgress(It.IsAny<OperationStatus>()), Times.Once);
    }

    [Fact]
    public async Task StatusEndpoint_ExistingOperation_ReturnsDetailedStatus()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();
        var operationId = "existing-operation-id";

        mockRequest.Setup(x => x.Url).Returns(new Uri($"http://localhost/api/status?operationId={operationId}"));

        var operationStatus = new OperationStatus
        {
            OperationId = operationId,
            Status = OperationStatusEnum.Running,
            StartTime = DateTime.UtcNow.AddMinutes(-10),
            EndTime = null,
            TablesCount = 5,
            TablesCompleted = 3,
            TablesFailed = 1,
            ProgressPercentage = 80.0,
            CompletedTables = new List<string> { "Table1", "Table2", "Table3" },
            FailedTables = new List<string> { "Table4: Connection timeout" },
            InProgressTables = new List<string> { "Table5" },
            CurrentPhase = OperationPhaseEnum.ProcessingTables
        };

        _mockOperationStorage
            .Setup(x => x.GetOperationAsync(operationId))
            .ReturnsAsync(operationStatus);

        var mockStatusResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateStatusResponseAsync(mockRequest.Object, It.IsAny<object>()))
            .ReturnsAsync(mockStatusResponse);

        var result = await _controller.GetStatus(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockOperationStorage.Verify(x => x.GetOperationAsync(operationId), Times.Once);
        _mockProgressTracking.Verify(x => x.UpdateProgress(operationStatus), Times.Once);
        _mockResponseService.Verify(x => x.CreateStatusResponseAsync(mockRequest.Object, It.IsAny<object>()), Times.Once);
    }

    [Fact]
    public async Task StatusEndpoint_NoSpecificOperation_ReturnsGeneralStatus()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        mockRequest.Setup(x => x.Url).Returns(new Uri("http://localhost/api/status"));

        var recentOperations = new List<OperationStatus>
        {
            new OperationStatus
            {
                OperationId = "op1",
                Status = OperationStatusEnum.Completed,
                StartTime = DateTime.UtcNow.AddMinutes(-30),
                TablesCount = 3,
                TablesCompleted = 3,
                ProgressPercentage = 100.0
            },
            new OperationStatus
            {
                OperationId = "op2",
                Status = OperationStatusEnum.Running,
                StartTime = DateTime.UtcNow.AddMinutes(-5),
                TablesCount = 2,
                TablesCompleted = 1,
                ProgressPercentage = 50.0
            }
        };

        var operationCounts = (running: 3, completed: 10, failed: 2, total: 15);

        _mockOperationStorage
            .Setup(x => x.GetRecentOperationsAsync(10))
            .ReturnsAsync(recentOperations);

        _mockOperationStorage
            .Setup(x => x.GetOperationCountsAsync())
            .ReturnsAsync(operationCounts);

        var mockStatusResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateStatusResponseAsync(mockRequest.Object, It.IsAny<object>()))
            .ReturnsAsync(mockStatusResponse);

        var result = await _controller.GetStatus(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockOperationStorage.Verify(x => x.GetRecentOperationsAsync(10), Times.Once);
        _mockOperationStorage.Verify(x => x.GetOperationCountsAsync(), Times.Once);
        _mockResponseService.Verify(x => x.CreateStatusResponseAsync(mockRequest.Object, It.IsAny<object>()), Times.Once);
    }

    [Fact]
    public async Task TokenTestEndpoint_SuccessfulTokenAcquisition_ReturnsSuccess()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        var testResult = new TokenTestResult
        {
            IsSuccessful = true,
            AuthenticationMode = "ServicePrincipal",
            TokenAcquired = true,
            TokenLength = 1234
        };

        _mockConnectionService
            .Setup(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(testResult);

        var mockSuccessResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(mockRequest.Object, testResult, HttpStatusCode.OK))
            .ReturnsAsync(mockSuccessResponse);

        var result = await _controller.TestToken(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ConnectionTestEndpoint_SuccessfulConnection_ReturnsSuccess()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        var connectionTestResult = new ConnectionTestResult
        {
            IsSuccessful = true,
            ServerVersion = "16.0.123.45",
            ConnectionTimeMs = 150.5,
            DatabaseFound = true,
            TablesCount = 25
        };

        _mockConnectionService
            .Setup(x => x.TestConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(connectionTestResult);

        var mockSuccessResponse = CreateMockHttpResponse(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(mockRequest.Object, connectionTestResult, HttpStatusCode.OK))
            .ReturnsAsync(mockSuccessResponse);

        var result = await _controller.TestConnection(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ErrorHandling_ConfigurationFailure_ReturnsErrorResponse()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[] { new RefreshObject { Table = "Table1" } }
        };

        _mockRequestProcessing
            .Setup(x => x.ParseAndValidateRequestAsync(mockRequest.Object))
            .ReturnsAsync(requestData);

        _mockRequestProcessing
            .Setup(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object))
            .Throws(new InvalidOperationException("Configuration error"));

        var mockErrorResponse = CreateMockHttpResponse(HttpStatusCode.BadRequest);
        _mockErrorHandling
            .Setup(x => x.HandleExceptionAsync(mockRequest.Object, It.IsAny<Exception>(), "AAS refresh"))
            .ReturnsAsync(mockErrorResponse);

        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.BadRequest, result.StatusCode);
        _mockErrorHandling.Verify(x => x.HandleExceptionAsync(mockRequest.Object, It.IsAny<Exception>(), "AAS refresh"), Times.Once);
    }

    [Fact]
    public async Task ProgressTracking_UpdatesOperationStatusCorrectly()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[] { new RefreshObject { Table = "Table1" } }
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
            .Setup(x => x.ParseAndValidateRequestAsync(mockRequest.Object))
            .ReturnsAsync(requestData);

        _mockRequestProcessing
            .Setup(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object))
            .Returns(enhancedRequestData);

        _mockRequestProcessing
            .Setup(x => x.EstimateOperationDuration(requestData))
            .Returns(15);

        _mockOperationStorage
            .Setup(x => x.UpsertOperationAsync(It.IsAny<OperationStatus>()))
            .ReturnsAsync(true);

        var mockAcceptedResponse = CreateMockHttpResponse(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(mockRequest.Object, It.IsAny<string>(), 15))
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockProgressTracking.Verify(x => x.InitializeProgress(It.IsAny<OperationStatus>()), Times.Once);
        _mockOperationStorage.Verify(x => x.UpsertOperationAsync(It.IsAny<OperationStatus>()), Times.Once);
    }

    [Fact]
    public async Task RequestProcessing_ValidatesAndEnhancesRequestData()
    {
        var mockRequest = CreateMockHttpRequest();
        var mockContext = CreateMockFunctionContext();

        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[]
            {
                new RefreshObject { Table = "Table1" },
                new RefreshObject { Table = "Table2" }
            },
            MaxRetryAttempts = 5,
            BaseDelaySeconds = 10
        };

        var enhancedRequestData = new EnhancedPostData
        {
            OriginalRequest = requestData,
            MaxRetryAttempts = 5,
            BaseDelaySeconds = 10,
            ConnectionTimeoutMinutes = 10,
            OperationTimeoutMinutes = 30
        };

        _mockRequestProcessing
            .Setup(x => x.ParseAndValidateRequestAsync(mockRequest.Object))
            .ReturnsAsync(requestData);

        _mockRequestProcessing
            .Setup(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object))
            .Returns(enhancedRequestData);

        _mockRequestProcessing
            .Setup(x => x.EstimateOperationDuration(requestData))
            .Returns(25);

        _mockOperationStorage
            .Setup(x => x.UpsertOperationAsync(It.IsAny<OperationStatus>()))
            .ReturnsAsync(true);

        var mockAcceptedResponse = CreateMockHttpResponse(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(mockRequest.Object, It.IsAny<string>(), 25))
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _controller.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockRequestProcessing.Verify(x => x.ParseAndValidateRequestAsync(mockRequest.Object), Times.Once);
        _mockRequestProcessing.Verify(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object), Times.Once);
        _mockRequestProcessing.Verify(x => x.EstimateOperationDuration(requestData), Times.Once);
    }

    private static Mock<HttpRequestData> CreateMockHttpRequest() => TestHttpHelpers.CreateHttpRequestMock();

    private static Mock<FunctionContext> CreateMockFunctionContext() => TestHttpHelpers.CreateFunctionContextMock();

    private static HttpResponseData CreateMockHttpResponse(HttpStatusCode statusCode) =>
        TestHttpHelpers.CreateHttpResponseData(statusCode);
}
