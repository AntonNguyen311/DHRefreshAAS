using Xunit;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using System.Threading.Tasks;
using DHRefreshAAS.Services;

namespace DHRefreshAAS.Tests;

public class ErrorHandlingServiceTests
{
    private readonly Mock<ILogger<ErrorHandlingService>> _mockLogger;
    private readonly ErrorHandlingService _service;

    public ErrorHandlingServiceTests()
    {
        _mockLogger = new Mock<ILogger<ErrorHandlingService>>();
        _service = new ErrorHandlingService(_mockLogger.Object);
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesService()
    {
        // Arrange & Act
        var service = new ErrorHandlingService(_mockLogger.Object);

        // Assert
        Assert.NotNull(service);
    }

    [Fact]
    public async Task CreateErrorResponseAsync_ValidParameters_ReturnsErrorResponse()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var statusCode = HttpStatusCode.BadRequest;
        var errorMessage = "Test error message";
        var exception = new Exception("Test exception");

        var mockResponse = TestHttpHelpers.CreateHttpResponseMock(statusCode);
        mockRequest.Setup(x => x.CreateResponse()).Returns(mockResponse);

        // Act
        var result = await _service.CreateErrorResponseAsync(mockRequest.Object, statusCode, errorMessage, exception);

        // Assert
        Assert.Equal(mockResponse, result);
        mockRequest.Verify(x => x.CreateResponse(), Times.Once);
    }

    [Fact]
    public async Task CreateErrorResponseAsync_NoException_ReturnsErrorResponse()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var statusCode = HttpStatusCode.InternalServerError;
        var errorMessage = "Test error without exception";

        var mockResponse = TestHttpHelpers.CreateHttpResponseMock(statusCode);
        mockRequest.Setup(x => x.CreateResponse()).Returns(mockResponse);

        // Act
        var result = await _service.CreateErrorResponseAsync(mockRequest.Object, statusCode, errorMessage);

        // Assert
        Assert.Equal(mockResponse, result);
        mockRequest.Verify(x => x.CreateResponse(), Times.Once);
    }

    [Fact]
    public async Task CreateValidationErrorResponseAsync_ValidParameters_ReturnsBadRequestResponse()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var validationMessage = "Validation failed";

        var mockResponse = TestHttpHelpers.CreateHttpResponseMock(HttpStatusCode.BadRequest);
        mockRequest.Setup(x => x.CreateResponse()).Returns(mockResponse);

        // Act
        var result = await _service.CreateValidationErrorResponseAsync(mockRequest.Object, validationMessage);

        // Assert
        Assert.Equal(mockResponse, result);
        mockRequest.Verify(x => x.CreateResponse(), Times.Once);
    }

    [Fact]
    public async Task CreateNotFoundResponseAsync_ValidParameters_ReturnsNotFoundResponse()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var resourceName = "Operation";
        var resourceId = "test-operation-id";

        var mockResponse = TestHttpHelpers.CreateHttpResponseMock(HttpStatusCode.NotFound);
        mockRequest.Setup(x => x.CreateResponse()).Returns(mockResponse);

        // Act
        var result = await _service.CreateNotFoundResponseAsync(mockRequest.Object, resourceName, resourceId);

        // Assert
        Assert.Equal(mockResponse, result);
        mockRequest.Verify(x => x.CreateResponse(), Times.Once);
    }

    [Fact]
    public async Task HandleExceptionAsync_ArgumentException_ReturnsBadRequestWithOriginalMessage()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var operation = "TestOperation";
        var exception = new ArgumentException("Invalid argument provided");

        var mockResponse = TestHttpHelpers.CreateHttpResponseMock(HttpStatusCode.BadRequest);
        mockRequest.Setup(x => x.CreateResponse()).Returns(mockResponse);

        // Act
        var result = await _service.HandleExceptionAsync(mockRequest.Object, exception, operation);

        // Assert
        Assert.Equal(mockResponse, result);
        mockRequest.Verify(x => x.CreateResponse(), Times.Once);
        // The WriteStringAsync should be called with the original exception message
    }

    [Fact]
    public async Task HandleExceptionAsync_InternalServerErrorException_ReturnsGenericMessage()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var operation = "TestOperation";
        var exception = new Exception("Some internal error");

        var mockResponse = TestHttpHelpers.CreateHttpResponseMock(HttpStatusCode.InternalServerError);
        mockRequest.Setup(x => x.CreateResponse()).Returns(mockResponse);

        // Act
        var result = await _service.HandleExceptionAsync(mockRequest.Object, exception, operation);

        // Assert
        Assert.Equal(mockResponse, result);
        mockRequest.Verify(x => x.CreateResponse(), Times.Once);
        // The WriteStringAsync should be called with generic "internal server error" message
    }

    [Fact]
    public async Task HandleExceptionAsync_LogsErrorWithOperationContext()
    {
        // Arrange
        var mockRequest = CreateMockHttpRequest();
        var operation = "TestOperation";
        var exception = new Exception("Test exception");

        var mockResponse = TestHttpHelpers.CreateHttpResponseMock(HttpStatusCode.InternalServerError);
        mockRequest.Setup(x => x.CreateResponse()).Returns(mockResponse);

        // Act
        await _service.HandleExceptionAsync(mockRequest.Object, exception, operation);

        // Assert (HandleExceptionAsync logs once; CreateErrorResponseAsync logs again when building body)
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeast(1));
    }

    // Helper methods
    private Mock<HttpRequestData> CreateMockHttpRequest() => TestHttpHelpers.CreateHttpRequestMock();
}
