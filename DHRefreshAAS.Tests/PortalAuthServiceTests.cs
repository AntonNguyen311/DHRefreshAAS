using System.Text;
using DHRefreshAAS.Models;
using DHRefreshAAS.Services;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DHRefreshAAS.Tests;

public class PortalAuthServiceTests
{
    [Fact]
    public void GetPortalUser_ParsesEasyAuthPrincipal()
    {
        var mockConfig = new Mock<IConfigurationService>();
        var service = new PortalAuthService(mockConfig.Object, Mock.Of<ILogger<PortalAuthService>>());
        var request = CreateRequest(new
        {
            auth_typ = "aad",
            claims = new[]
            {
                new { typ = "name", val = "Anton Tuan" },
                new { typ = "preferred_username", val = "anton.tuan@deheus.com" },
                new { typ = "http://schemas.microsoft.com/identity/claims/objectidentifier", val = "user-123" },
                new { typ = "roles", val = "Cube.Refresh" },
                new { typ = "groups", val = "group-a" }
            }
        });

        var user = service.GetPortalUser(request.Object);

        Assert.NotNull(user);
        Assert.Equal("user-123", user!.UserId);
        Assert.Equal("Anton Tuan", user.DisplayName);
        Assert.Equal("anton.tuan@deheus.com", user.Email);
        Assert.Contains("Cube.Refresh", user.Roles);
        Assert.Contains("group-a", user.GroupIds);
    }

    [Fact]
    public void CanSubmitRefresh_UsesConfiguredRoles()
    {
        var mockConfig = new Mock<IConfigurationService>();
        mockConfig.Setup(x => x.PortalRefreshRoles).Returns(new[] { "Cube.Refresh" });
        mockConfig.Setup(x => x.PortalRefreshGroups).Returns(Array.Empty<string>());
        mockConfig.Setup(x => x.PortalAdminRoles).Returns(Array.Empty<string>());
        mockConfig.Setup(x => x.PortalAdminGroups).Returns(Array.Empty<string>());

        var service = new PortalAuthService(mockConfig.Object, Mock.Of<ILogger<PortalAuthService>>());
        var allowed = new PortalUserContext { Roles = new List<string> { "Cube.Refresh" } };
        var blocked = new PortalUserContext { Roles = new List<string> { "Reader" } };

        Assert.True(service.CanSubmitRefresh(allowed));
        Assert.False(service.CanSubmitRefresh(blocked));
    }

    private static Mock<HttpRequestData> CreateRequest(object principalPayload)
    {
        var context = TestHttpHelpers.CreateFunctionContextMock();
        var request = new Mock<HttpRequestData>(context.Object);
        var principalJson = System.Text.Json.JsonSerializer.Serialize(principalPayload);
        var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(principalJson));
        request.Setup(x => x.Headers).Returns(new HttpHeadersCollection
        {
            { "X-MS-CLIENT-PRINCIPAL", encoded }
        });
        return request;
    }
}
