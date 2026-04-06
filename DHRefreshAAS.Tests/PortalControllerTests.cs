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

public class PortalControllerTests
{
    private readonly Mock<IPortalAuthService> _mockPortalAuth;
    private readonly Mock<ISelfServiceMetadataService> _mockMetadata;
    private readonly Mock<IRequestProcessingService> _mockRequestProcessing;
    private readonly Mock<IQueueExecutionService> _mockQueueExecution;
    private readonly Mock<IStatusResponseBuilder> _mockStatusResponseBuilder;
    private readonly Mock<IResponseService> _mockResponseService;
    private readonly Mock<IErrorHandlingService> _mockErrorHandling;
    private readonly PortalController _controller;

    public PortalControllerTests()
    {
        var mockConfig = new Mock<IConfigurationService>();

        _mockPortalAuth = new Mock<IPortalAuthService>();
        _mockMetadata = new Mock<ISelfServiceMetadataService>();
        _mockRequestProcessing = new Mock<IRequestProcessingService>();
        _mockQueueExecution = new Mock<IQueueExecutionService>();
        _mockStatusResponseBuilder = new Mock<IStatusResponseBuilder>();
        _mockResponseService = new Mock<IResponseService>();
        _mockErrorHandling = new Mock<IErrorHandlingService>();

        _controller = new PortalController(
            _mockPortalAuth.Object,
            _mockMetadata.Object,
            _mockRequestProcessing.Object,
            mockConfig.Object,
            _mockQueueExecution.Object,
            _mockStatusResponseBuilder.Object,
            _mockResponseService.Object,
            _mockErrorHandling.Object,
            Mock.Of<ILogger<PortalController>>());
    }

    [Fact]
    public async Task PortalListModels_NoAuth_ReturnsUnauthorized()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();

        _mockPortalAuth
            .Setup(x => x.GetPortalUser(It.IsAny<HttpRequestData>()))
            .Returns((PortalUserContext?)null);

        var mockResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.Unauthorized);
        _mockResponseService
            .Setup(x => x.CreateUnauthorizedResponseAsync(It.IsAny<HttpRequestData>(), It.IsAny<string>()))
            .ReturnsAsync(mockResponse);

        var result = await _controller.PortalListModels(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Unauthorized, result.StatusCode);
    }

    [Fact]
    public async Task PortalListModels_NoPermission_ReturnsForbidden()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var user = new PortalUserContext { UserId = "u1", DisplayName = "User" };

        _mockPortalAuth
            .Setup(x => x.GetPortalUser(It.IsAny<HttpRequestData>()))
            .Returns(user);
        _mockPortalAuth
            .Setup(x => x.CanReadMetadata(user))
            .Returns(false);

        var mockResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.Forbidden);
        _mockResponseService
            .Setup(x => x.CreateForbiddenResponseAsync(It.IsAny<HttpRequestData>(), It.IsAny<string>()))
            .ReturnsAsync(mockResponse);

        var result = await _controller.PortalListModels(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.Forbidden, result.StatusCode);
    }

    [Fact]
    public async Task PortalStatus_DelegatesToStatusResponseBuilder()
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var mockContext = TestHttpHelpers.CreateFunctionContextMock();
        var user = new PortalUserContext { UserId = "u1", DisplayName = "User" };

        mockRequest.Setup(x => x.Url).Returns(new Uri("http://localhost/api/portalstatus"));

        _mockPortalAuth
            .Setup(x => x.GetPortalUser(It.IsAny<HttpRequestData>()))
            .Returns(user);
        _mockPortalAuth
            .Setup(x => x.CanReadMetadata(user))
            .Returns(true);

        var mockResponse = TestHttpHelpers.CreateHttpResponseData(HttpStatusCode.OK);
        _mockStatusResponseBuilder
            .Setup(x => x.GetGeneralStatusAsync(It.IsAny<HttpRequestData>(), user, true))
            .ReturnsAsync(mockResponse);

        var result = await _controller.PortalStatus(mockRequest.Object, mockContext.Object);

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
        _mockStatusResponseBuilder.Verify(x => x.GetGeneralStatusAsync(It.IsAny<HttpRequestData>(), user, true), Times.Once);
    }
}
