using Xunit;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using DHRefreshAAS.Controllers;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;
using DHRefreshAAS.Enums;

namespace DHRefreshAAS.Tests;

/// <summary>
/// Integration-level tests verifying cross-service flows through the new controllers.
/// </summary>
public class DHRefreshAASIntegrationTests
{
    private readonly Mock<IConfigurationService> _mockConfig;
    private readonly Mock<IConnectionService> _mockConnectionService;
    private readonly Mock<IOperationStorageService> _mockOperationStorage;
    private readonly Mock<RequestProcessingService> _mockRequestProcessing;
    private readonly Mock<ResponseService> _mockResponseService;
    private readonly Mock<ErrorHandlingService> _mockErrorHandling;
    private readonly Mock<QueueExecutionService> _mockQueueExecution;
    private readonly Mock<StatusResponseBuilder> _mockStatusResponseBuilder;
    private readonly RefreshController _refreshController;
    private readonly DiagnosticsController _diagnosticsController;

    public DHRefreshAASIntegrationTests()
    {
        _mockConfig = new Mock<IConfigurationService>();
        _mockConnectionService = new Mock<IConnectionService>();
        _mockOperationStorage = new Mock<IOperationStorageService>();
        var mockScalingService = new Mock<AasScalingService>(_mockConfig.Object, Mock.Of<ILogger<AasScalingService>>());
        var mockElasticPoolScalingService = new Mock<ElasticPoolScalingService>(_mockConfig.Object, Mock.Of<ILogger<ElasticPoolScalingService>>());
        _mockRequestProcessing = new Mock<RequestProcessingService>(Mock.Of<ILogger<RequestProcessingService>>());
        _mockResponseService = new Mock<ResponseService>();
        _mockErrorHandling = new Mock<ErrorHandlingService>(Mock.Of<ILogger<ErrorHandlingService>>());

        _mockQueueExecution = new Mock<QueueExecutionService>(
            _mockConfig.Object, Mock.Of<IAasRefreshService>(), _mockOperationStorage.Object,
            new Mock<ProgressTrackingService>(Mock.Of<ILogger<ProgressTrackingService>>()).Object,
            new Mock<OperationCleanupService>(_mockOperationStorage.Object, _mockConfig.Object, Mock.Of<ILogger<OperationCleanupService>>()).Object,
            Mock.Of<IHostApplicationLifetime>(),
            Mock.Of<ILogger<QueueExecutionService>>());
        _mockStatusResponseBuilder = new Mock<StatusResponseBuilder>(
            _mockOperationStorage.Object,
            new Mock<ProgressTrackingService>(Mock.Of<ILogger<ProgressTrackingService>>()).Object,
            _mockResponseService.Object);

        _refreshController = new RefreshController(
            _mockConfig.Object,
            _mockConnectionService.Object,
            mockScalingService.Object,
            mockElasticPoolScalingService.Object,
            _mockOperationStorage.Object,
            _mockRequestProcessing.Object,
            _mockResponseService.Object,
            _mockErrorHandling.Object,
            _mockQueueExecution.Object,
            _mockStatusResponseBuilder.Object,
            Mock.Of<ILogger<RefreshController>>());

        _diagnosticsController = new DiagnosticsController(
            _mockConnectionService.Object,
            _mockResponseService.Object,
            _mockErrorHandling.Object,
            Mock.Of<ILogger<DiagnosticsController>>());
    }

    [Fact]
    public async Task EndToEnd_ValidRefreshRequest_CompleteFlow()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

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

        _mockQueueExecution
            .Setup(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 20, null, "api"))
            .ReturnsAsync(new QueueOperationResult
            {
                OperationId = "op-1",
                EstimatedDurationMinutes = 20,
                StartedImmediately = true,
                Status = OperationStatusEnum.Running,
                Message = "Started",
                QueueScope = "aas:server:testdb"
            });

        var mockAcceptedResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(
                mockRequest.Object, It.IsAny<string>(), 20,
                It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<int?>(), It.IsAny<string?>()))
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _refreshController.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockRequestProcessing.Verify(x => x.ParseAndValidateRequestAsync(mockRequest.Object), Times.Once);
        _mockRequestProcessing.Verify(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object), Times.Once);
        _mockQueueExecution.Verify(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 20, null, "api"), Times.Once);
    }

    [Fact]
    public async Task StatusEndpoint_ExistingOperation_DelegatesToBuilder()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var operationId = "existing-operation-id";

        mockRequest.Setup(x => x.Url).Returns(new Uri($"http://localhost/api/status?operationId={operationId}"));

        var mockStatusResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockStatusResponseBuilder
            .Setup(x => x.GetSpecificOperationStatusAsync(operationId, It.IsAny<HttpRequestData>(), null, false))
            .ReturnsAsync(mockStatusResponse);

        var result = await _refreshController.GetStatus(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockStatusResponseBuilder.Verify(
            x => x.GetSpecificOperationStatusAsync(operationId, It.IsAny<HttpRequestData>(), null, false), Times.Once);
    }

    [Fact]
    public async Task StatusEndpoint_NoSpecificOperation_ReturnsGeneralStatus()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

        mockRequest.Setup(x => x.Url).Returns(new Uri("http://localhost/api/status"));

        var mockStatusResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockStatusResponseBuilder
            .Setup(x => x.GetGeneralStatusAsync(It.IsAny<HttpRequestData>(), null, false))
            .ReturnsAsync(mockStatusResponse);

        var result = await _refreshController.GetStatus(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockStatusResponseBuilder.Verify(
            x => x.GetGeneralStatusAsync(It.IsAny<HttpRequestData>(), null, false), Times.Once);
    }

    [Fact]
    public async Task TokenTestEndpoint_SuccessfulTokenAcquisition_ReturnsSuccess()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

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

        var mockSuccessResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(mockRequest.Object, testResult, HttpStatusCode.OK))
            .ReturnsAsync(mockSuccessResponse);

        var result = await _diagnosticsController.TestToken(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ConnectionTestEndpoint_SuccessfulConnection_ReturnsSuccess()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

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

        var mockSuccessResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(mockRequest.Object, connectionTestResult, HttpStatusCode.OK))
            .ReturnsAsync(mockSuccessResponse);

        var result = await _diagnosticsController.TestConnection(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ErrorHandling_ConfigurationFailure_ReturnsErrorResponse()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

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

        var mockErrorResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.BadRequest);
        _mockErrorHandling
            .Setup(x => x.HandleExceptionAsync(mockRequest.Object, It.IsAny<Exception>(), "AAS refresh"))
            .ReturnsAsync(mockErrorResponse);

        var result = await _refreshController.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.BadRequest, result.StatusCode);
        _mockErrorHandling.Verify(x => x.HandleExceptionAsync(mockRequest.Object, It.IsAny<Exception>(), "AAS refresh"), Times.Once);
    }

    [Fact]
    public async Task ProgressTracking_HttpStartDelegatesToQueueExecution()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

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

        _mockQueueExecution
            .Setup(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 15, null, "api"))
            .ReturnsAsync(new QueueOperationResult
            {
                OperationId = "op-track",
                EstimatedDurationMinutes = 15,
                StartedImmediately = true,
                Status = OperationStatusEnum.Running,
                Message = "Started"
            });

        var mockAcceptedResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(
                mockRequest.Object, It.IsAny<string>(), 15,
                It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<int?>(), It.IsAny<string?>()))
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _refreshController.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockQueueExecution.Verify(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 15, null, "api"), Times.Once);
    }

    [Fact]
    public async Task RequestProcessing_ValidatesAndEnhancesRequestData()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

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

        _mockQueueExecution
            .Setup(x => x.StartAsyncOperationAsync(requestData, enhancedRequestData, 25, null, "api"))
            .ReturnsAsync(new QueueOperationResult
            {
                OperationId = "op-validate",
                EstimatedDurationMinutes = 25,
                StartedImmediately = true,
                Status = OperationStatusEnum.Running,
                Message = "Started"
            });

        var mockAcceptedResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.Accepted);
        _mockResponseService
            .Setup(x => x.CreateAcceptedResponseAsync(
                mockRequest.Object, It.IsAny<string>(), 25,
                It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<int?>(), It.IsAny<string?>()))
            .ReturnsAsync(mockAcceptedResponse);

        var result = await _refreshController.HttpStart(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        _mockRequestProcessing.Verify(x => x.ParseAndValidateRequestAsync(mockRequest.Object), Times.Once);
        _mockRequestProcessing.Verify(x => x.CreateEnhancedRequestData(requestData, _mockConfig.Object), Times.Once);
        _mockRequestProcessing.Verify(x => x.EstimateOperationDuration(requestData), Times.Once);
    }
}
