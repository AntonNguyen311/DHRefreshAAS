using System.IO;
using System.Net;
using System.Threading;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace DHRefreshAAS.Tests;

/// <summary>
/// HttpRequestData / HttpResponseData require a <see cref="FunctionContext"/> for Moq.
/// </summary>
internal static class TestHttpHelpers
{
    private static readonly IServiceProvider SharedServiceProvider = BuildSharedServiceProvider();

    private static IServiceProvider BuildSharedServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services.BuildServiceProvider();
    }

    public static Mock<FunctionContext> CreateFunctionContextMock()
    {
        var mockContext = new Mock<FunctionContext>();
        mockContext.Setup(c => c.CancellationToken).Returns(CancellationToken.None);
        mockContext.Setup(c => c.InstanceServices).Returns(SharedServiceProvider);
        return mockContext;
    }

    public static Mock<HttpRequestData> CreateHttpRequestMock(Uri? url = null)
    {
        var mockContext = CreateFunctionContextMock();
        var mockRequest = new Mock<HttpRequestData>(mockContext.Object);
        mockRequest.Setup(x => x.Url).Returns(url ?? new Uri("http://localhost/api/test"));
        mockRequest.Setup(x => x.Body).Returns(new MemoryStream());
        return mockRequest;
    }

    /// <summary>
    /// Returns a Moq-backed <see cref="HttpResponseData"/> (SDK type is abstract).
    /// </summary>
    public static HttpResponseData CreateHttpResponseData(HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        var mockContext = CreateFunctionContextMock();
        var mockResponse = new Mock<HttpResponseData>(mockContext.Object);
        mockResponse.SetupProperty(r => r.StatusCode, statusCode);
        mockResponse.SetupProperty(r => r.Headers, new HttpHeadersCollection());
        mockResponse.SetupProperty(r => r.Body, new MemoryStream());
        return mockResponse.Object;
    }

    /// <summary>Backward-compatible name for callers that treated the result like a mock's <c>.Object</c>.</summary>
    public static HttpResponseData CreateHttpResponseMock(HttpStatusCode statusCode = HttpStatusCode.OK) =>
        CreateHttpResponseData(statusCode);
}
