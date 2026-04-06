using Xunit;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using DHRefreshAAS.Controllers;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Tests;

public class DiagnosticsControllerTests
{
    private readonly Mock<IConnectionService> _mockConnectionService;
    private readonly Mock<IResponseService> _mockResponseService;
    private readonly Mock<IErrorHandlingService> _mockErrorHandling;
    private readonly Mock<ILogger<DiagnosticsController>> _mockLogger;
    private readonly DiagnosticsController _controller;

    public DiagnosticsControllerTests()
    {
        _mockConnectionService = new Mock<IConnectionService>();
        _mockResponseService = new Mock<IResponseService>();
        _mockErrorHandling = new Mock<IErrorHandlingService>();
        _mockLogger = new Mock<ILogger<DiagnosticsController>>();

        _controller = new DiagnosticsController(
            _mockConnectionService.Object,
            _mockResponseService.Object,
            _mockErrorHandling.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task TestToken_ValidRequest_ReturnsSuccessResponse()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var testResult = new TokenTestResult { IsSuccessful = true };

        _mockConnectionService
            .Setup(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(testResult);

        var mockResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(It.IsAny<HttpRequestData>(), testResult, HttpStatusCode.OK))
            .ReturnsAsync(mockResponse);

        var result = await _controller.TestToken(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TestToken_ExceptionThrown_ReturnsErrorResponse()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var exception = new Exception("Connection failed");

        _mockConnectionService
            .Setup(x => x.TestTokenAcquisitionAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        var mockErrorResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.InternalServerError);
        _mockErrorHandling
            .Setup(x => x.HandleExceptionAsync(It.IsAny<HttpRequestData>(), exception, "token acquisition test"))
            .ReturnsAsync(mockErrorResponse);

        var result = await _controller.TestToken(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.InternalServerError, result.StatusCode);
    }

    [Fact]
    public async Task TestConnection_ValidRequest_ReturnsSuccessResponse()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var testResult = new ConnectionTestResult { IsSuccessful = true };

        _mockConnectionService
            .Setup(x => x.TestConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(testResult);

        var mockResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockResponseService
            .Setup(x => x.CreateSuccessResponseAsync(It.IsAny<HttpRequestData>(), testResult, HttpStatusCode.OK))
            .ReturnsAsync(mockResponse);

        var result = await _controller.TestConnection(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockConnectionService.Verify(x => x.TestConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}
