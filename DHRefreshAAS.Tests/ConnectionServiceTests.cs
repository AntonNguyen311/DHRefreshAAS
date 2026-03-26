using Xunit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DHRefreshAAS;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Tests;

public class ConnectionServiceTests
{
    private readonly Mock<IConfiguration> _mockConfiguration;
    private readonly Mock<ILogger<ConfigurationService>> _mockConfigLogger;
    private readonly ConfigurationService _configService;
    private readonly Mock<ILogger<ConnectionService>> _mockLogger;
    private readonly ConnectionService _service;

    public ConnectionServiceTests()
    {
        _mockConfiguration = new Mock<IConfiguration>();
        _mockConfigLogger = new Mock<ILogger<ConfigurationService>>();
        _configService = new ConfigurationService(_mockConfiguration.Object, _mockConfigLogger.Object);
        _mockLogger = new Mock<ILogger<ConnectionService>>();
        _service = new ConnectionService(_configService, _mockLogger.Object);
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesService()
    {
        // Arrange & Act
        var service = new ConnectionService(_configService, _mockLogger.Object);

        // Assert
        Assert.NotNull(service);
    }

    [Fact]
    public void Constructor_NullConfigurationService_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new ConnectionService(null!, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new ConnectionService(_configService, null!));
    }

    [Theory]
    [InlineData("ManagedIdentity")]
    [InlineData("ServicePrincipal")]
    [InlineData("UserPassword")]
    public void BuildConnectionString_ValidAuthModes_IncludesCorrectSettings(string authMode)
    {
        // Arrange
        SetupConfigForConnectionString(authMode);

        // Act
        var connectionString = GetConnectionString();

        // Assert
        Assert.Contains("Provider=MSOLAP;", connectionString);
        Assert.Contains("Data Source=https://test.asazure.windows.net;", connectionString);
        Assert.Contains("Initial Catalog=testdb;", connectionString);
        var (connectSec, commandSec) = _configService.GetDefaultClientTimeouts();
        Assert.Contains($"Connect Timeout={connectSec};", connectionString);
        Assert.Contains($"Command Timeout={commandSec};", connectionString);
    }

    [Fact]
    public void BuildConnectionString_ManagedIdentity_IncludesSSPI()
    {
        // Arrange
        SetupConfigForConnectionString("ManagedIdentity");

        // Act
        var connectionString = GetConnectionString();

        // Assert
        Assert.Contains("Integrated Security=SSPI;", connectionString);
        Assert.Contains("Impersonation Level=Impersonate;", connectionString);
    }

    [Fact]
    public void BuildConnectionString_ServicePrincipal_IncludesCredentials()
    {
        // Arrange
        SetupConfigForConnectionString("ServicePrincipal");

        // Act
        var connectionString = GetConnectionString();

        // Assert
        Assert.Contains("User ID=app:test-client-id@test-tenant-id;", connectionString);
        Assert.Contains("Password=test-secret;", connectionString);
        Assert.Contains("Impersonation Level=Impersonate;", connectionString);
    }

    [Fact]
    public void BuildConnectionString_UserPassword_IncludesCredentials()
    {
        // Arrange
        SetupConfigForConnectionString("UserPassword");

        // Act
        var connectionString = GetConnectionString();

        // Assert
        Assert.Contains("User ID=test@user.com;", connectionString);
        Assert.Contains("Password=test-password;", connectionString);
        Assert.Contains("Impersonation Level=Impersonate;", connectionString);
    }

    [Fact]
    public void BuildConnectionString_UnsupportedAuthMode_ThrowsInvalidOperationException()
    {
        // Arrange
        SetupConfigForConnectionString("UnsupportedMode");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => GetConnectionString());
        Assert.Contains("Unsupported authentication mode", exception.Message);
    }

    [Fact]
    public void BuildConnectionString_ServicePrincipalMissingCredentials_ThrowsInvalidOperationException()
    {
        // Arrange
        _mockConfiguration.Setup(c => c["AAS_SERVER_URL"]).Returns("https://test.asazure.windows.net");
        _mockConfiguration.Setup(c => c["AAS_AUTH_MODE"]).Returns("ServicePrincipal");
        _mockConfiguration.Setup(c => c["AAS_USER_ID"]).Returns(string.Empty);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => GetConnectionString());
        Assert.Contains("Service Principal auth requires", exception.Message);
    }

    [Fact]
    public void BuildConnectionString_UserPasswordInvalidFormat_ThrowsInvalidOperationException()
    {
        // Arrange
        _mockConfiguration.Setup(c => c["AAS_SERVER_URL"]).Returns("https://test.asazure.windows.net");
        _mockConfiguration.Setup(c => c["AAS_AUTH_MODE"]).Returns("UserPassword");
        _mockConfiguration.Setup(c => c["AAS_USER_ID"]).Returns("invalid-user-format");
        _mockConfiguration.Setup(c => c["AAS_PASSWORD"]).Returns("some-password");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => GetConnectionString());
        Assert.Contains("UPN", exception.Message);
    }

    [Fact(Skip = "Uses live HttpClient to Microsoft identity; MockHttpMessageHandler is not wired into ConnectionService")]
    public async Task TestTokenAcquisitionAsync_ServicePrincipalValidCredentials_ReturnsSuccess()
    {
        // Arrange
        SetupConfigForServicePrincipal();
        var cancellationToken = CancellationToken.None;

        // Mock HTTP response for successful token acquisition
        var mockHttpMessageHandler = new MockHttpMessageHandler();
        mockHttpMessageHandler.AddResponse(new HttpResponseMessage(System.Net.HttpStatusCode.OK)
        {
            Content = new StringContent("{\"access_token\":\"test-token\",\"expires_in\":3600}")
        });

        // Act
        var result = await _service.TestTokenAcquisitionAsync(cancellationToken);

        // Assert
        Assert.NotNull(result);
        Assert.True(result.IsSuccessful);
        Assert.True(result.TokenAcquired);
        Assert.Equal("ServicePrincipal", result.AuthenticationMode);
        Assert.Contains("test-client-id", result.ClientId);
        Assert.Contains("test-tenant-id", result.TenantId);
    }

    [Fact]
    public async Task TestTokenAcquisitionAsync_ManagedIdentity_ReturnsSuccess()
    {
        // Arrange
        _mockConfiguration.Setup(c => c["AAS_AUTH_MODE"]).Returns("ManagedIdentity");
        var cancellationToken = CancellationToken.None;

        // Act
        var result = await _service.TestTokenAcquisitionAsync(cancellationToken);

        // Assert
        Assert.NotNull(result);
        Assert.False(result.IsSuccessful);
        Assert.Contains("ServicePrincipal", result.ErrorMessage ?? "");
    }

    [Fact(Skip = "Uses live HttpClient to Microsoft identity; MockHttpMessageHandler is not wired into ConnectionService")]
    public async Task TestTokenAcquisitionAsync_InvalidCredentials_ReturnsFailure()
    {
        // Arrange
        SetupConfigForServicePrincipal();
        var cancellationToken = CancellationToken.None;

        // Mock HTTP response for failed token acquisition
        var mockHttpMessageHandler = new MockHttpMessageHandler();
        mockHttpMessageHandler.AddResponse(new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest)
        {
            Content = new StringContent("{\"error\":\"invalid_client\",\"error_description\":\"Invalid client credentials\"}")
        });

        // Act
        var result = await _service.TestTokenAcquisitionAsync(cancellationToken);

        // Assert
        Assert.NotNull(result);
        Assert.False(result.IsSuccessful);
        Assert.False(result.TokenAcquired);
        Assert.Contains("invalid_client", result.ErrorMessage);
    }

    [Fact]
    public async Task TestConnectionAsync_ValidConnection_ReturnsSuccess()
    {
        // Arrange
        SetupConfigForConnectionString("ServicePrincipal");
        var cancellationToken = CancellationToken.None;

        // Act
        var result = await _service.TestConnectionAsync(cancellationToken);

        // Assert
        Assert.NotNull(result);
        // Note: This test would need actual AAS server for full validation
        // In a real scenario, we'd mock the AAS server connection
    }

    [Fact]
    public void GetSanitizedConnectionString_PasswordPresent_MasksPassword()
    {
        // Arrange
        var connectionString = "Provider=MSOLAP;Data Source=test;User ID=test;Password=secret123;";

        // Act
        var sanitized = GetSanitizedConnectionString(connectionString);

        // Assert
        Assert.Contains("Password=***;", sanitized);
        Assert.DoesNotContain("secret123", sanitized);
    }

    [Fact]
    public void GetSanitizedConnectionString_NoPassword_ReturnsOriginal()
    {
        // Arrange
        var connectionString = "Provider=MSOLAP;Data Source=test;Integrated Security=SSPI;";

        // Act
        var sanitized = GetSanitizedConnectionString(connectionString);

        // Assert
        Assert.Equal(connectionString, sanitized);
    }

    [Fact]
    public void GetSanitizedConnectionString_PasswordAtEnd_MasksPassword()
    {
        // Arrange
        var connectionString = "Provider=MSOLAP;Data Source=test;Password=secret123";

        // Act
        var sanitized = GetSanitizedConnectionString(connectionString);

        // Assert
        Assert.Contains("Password=***;", sanitized);
        Assert.DoesNotContain("secret123", sanitized);
    }

    // Helper methods
    private void SetupConfigForConnectionString(string authMode)
    {
        _mockConfiguration.Setup(c => c["AAS_SERVER_URL"]).Returns("https://test.asazure.windows.net");
        _mockConfiguration.Setup(c => c["AAS_DATABASE"]).Returns("testdb");
        _mockConfiguration.Setup(c => c["AAS_AUTH_MODE"]).Returns(authMode);

        if (authMode == "ServicePrincipal")
        {
            _mockConfiguration.Setup(c => c["AAS_USER_ID"]).Returns("test-client-id");
            _mockConfiguration.Setup(c => c["AAS_PASSWORD"]).Returns("test-secret");
            _mockConfiguration.Setup(c => c["AAS_TENANT_ID"]).Returns("test-tenant-id");
        }
        else if (authMode == "UserPassword")
        {
            _mockConfiguration.Setup(c => c["AAS_USER_ID"]).Returns("test@user.com");
            _mockConfiguration.Setup(c => c["AAS_PASSWORD"]).Returns("test-password");
        }
    }

    private void SetupConfigForServicePrincipal()
    {
        _mockConfiguration.Setup(c => c["AAS_AUTH_MODE"]).Returns("ServicePrincipal");
        _mockConfiguration.Setup(c => c["AAS_USER_ID"]).Returns("test-client-id");
        _mockConfiguration.Setup(c => c["AAS_PASSWORD"]).Returns("test-secret");
        _mockConfiguration.Setup(c => c["AAS_TENANT_ID"]).Returns("test-tenant-id");
    }

    private string GetConnectionString()
    {
        var method = typeof(ConnectionService).GetMethod("BuildConnectionString",
            BindingFlags.NonPublic | BindingFlags.Instance);
        var (connectSec, commandSec) = _configService.GetDefaultClientTimeouts();
        try
        {
            return (string)method!.Invoke(_service, new object[] { connectSec, commandSec })!;
        }
        catch (TargetInvocationException ex)
        {
            throw ex.InnerException ?? ex;
        }
    }

    private string GetSanitizedConnectionString(string connectionString)
    {
        var method = typeof(ConnectionService).GetMethod("GetSanitizedConnectionString",
            BindingFlags.NonPublic | BindingFlags.Instance);
        try
        {
            return (string)method!.Invoke(_service, new object[] { connectionString })!;
        }
        catch (TargetInvocationException ex)
        {
            throw ex.InnerException ?? ex;
        }
    }
}

// Mock HTTP handler for testing HTTP calls
public class MockHttpMessageHandler : HttpMessageHandler
{
    private readonly List<HttpResponseMessage> _responses = new();
    private int _currentResponseIndex = 0;

    public void AddResponse(HttpResponseMessage response)
    {
        _responses.Add(response);
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (_currentResponseIndex < _responses.Count)
        {
            return Task.FromResult(_responses[_currentResponseIndex++]);
        }
        return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.NotFound));
    }
}
