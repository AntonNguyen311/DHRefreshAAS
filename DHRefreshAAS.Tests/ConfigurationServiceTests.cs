using Xunit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Moq;
using DHRefreshAAS;

namespace DHRefreshAAS.Tests;

public class ConfigurationServiceTests
{
    private readonly Mock<IConfiguration> _mockConfiguration;
    private readonly Mock<ILogger<ConfigurationService>> _mockLogger;
    private readonly ConfigurationService _service;

    public ConfigurationServiceTests()
    {
        _mockConfiguration = new Mock<IConfiguration>();
        _mockLogger = new Mock<ILogger<ConfigurationService>>();
        _service = new ConfigurationService(_mockConfiguration.Object, _mockLogger.Object);
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesService()
    {
        // Arrange & Act
        var service = new ConfigurationService(_mockConfiguration.Object, _mockLogger.Object);

        // Assert
        Assert.NotNull(service);
    }

    [Fact]
    public void Constructor_NullConfiguration_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ConfigurationService(null!, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ConfigurationService(_mockConfiguration.Object, null!));
    }

    [Theory]
    [InlineData("5", 5)]
    [InlineData("0", 0)]
    [InlineData("-1", -1)]
    [InlineData("999", 999)]
    public void GetConfigValue_Int_ValidValue_ReturnsParsedValue(string configValue, int expectedResult)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["TEST_KEY"]).Returns(configValue);

        // Act
        var result = _service.GetConfigValue("TEST_KEY", 10);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not_a_number")]
    public void GetConfigValue_Int_InvalidValue_ReturnsDefaultValue(string configValue, int defaultValue = 10)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["TEST_KEY"]).Returns(configValue);

        // Act
        var result = _service.GetConfigValue("TEST_KEY", defaultValue);

        // Assert
        Assert.Equal(defaultValue, result);
    }

    [Theory]
    [InlineData("test_value")]
    [InlineData("another test")]
    [InlineData("123")]
    [InlineData("special_chars!@#")]
    public void GetConfigValue_String_ValidValue_ReturnsValue(string configValue)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["TEST_KEY"]).Returns(configValue);

        // Act
        var result = _service.GetConfigValue("TEST_KEY", "default");

        // Assert
        Assert.Equal(configValue, result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void GetConfigValue_String_InvalidValue_ReturnsDefaultValue(string configValue, string defaultValue = "default")
    {
        // Arrange
        _mockConfiguration.Setup(x => x["TEST_KEY"]).Returns(configValue);

        // Act
        var result = _service.GetConfigValue("TEST_KEY", defaultValue);

        // Assert
        Assert.Equal(defaultValue, result);
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("True", true)]
    [InlineData("TRUE", true)]
    [InlineData("false", false)]
    [InlineData("False", false)]
    [InlineData("FALSE", false)]
    public void GetConfigValue_Bool_ValidValue_ReturnsParsedValue(string configValue, bool expectedResult)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["TEST_KEY"]).Returns(configValue);

        // Act
        var result = _service.GetConfigValue("TEST_KEY", !expectedResult);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not_a_bool")]
    [InlineData("yes")]
    [InlineData("no")]
    public void GetConfigValue_Bool_InvalidValue_ReturnsDefaultValue(string configValue, bool defaultValue = false)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["TEST_KEY"]).Returns(configValue);

        // Act
        var result = _service.GetConfigValue("TEST_KEY", defaultValue);

        // Assert
        Assert.Equal(defaultValue, result);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(5, 5)]
    [InlineData(10, 10)]
    public void MaxRetryAttempts_ConfiguredValue_ReturnsConfiguredValue(int configValue, int expectedValue)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["MAX_RETRY_ATTEMPTS"]).Returns(configValue.ToString());

        // Act
        var result = _service.MaxRetryAttempts;

        // Assert
        Assert.Equal(expectedValue, result);
    }

    [Fact]
    public void MaxRetryAttempts_NoConfiguration_ReturnsDefaultValue()
    {
        // Arrange
        _mockConfiguration.Setup(x => x["MAX_RETRY_ATTEMPTS"]).Returns((string)null!);

        // Act
        var result = _service.MaxRetryAttempts;

        // Assert
        Assert.Equal(3, result);
    }

    [Theory]
    [InlineData(10, 10)]
    [InlineData(60, 60)]
    [InlineData(120, 120)]
    public void BaseDelaySeconds_ConfiguredValue_ReturnsConfiguredValue(int configValue, int expectedValue)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["BASE_DELAY_SECONDS"]).Returns(configValue.ToString());

        // Act
        var result = _service.BaseDelaySeconds;

        // Assert
        Assert.Equal(expectedValue, result);
    }

    [Fact]
    public void BaseDelaySeconds_NoConfiguration_ReturnsDefaultValue()
    {
        // Arrange
        _mockConfiguration.Setup(x => x["BASE_DELAY_SECONDS"]).Returns((string)null!);

        // Act
        var result = _service.BaseDelaySeconds;

        // Assert
        Assert.Equal(30, result);
    }

    [Theory]
    [InlineData(5, 5)]
    [InlineData(15, 15)]
    [InlineData(30, 30)]
    public void ConnectionTimeoutMinutes_ConfiguredValue_ReturnsConfiguredValue(int configValue, int expectedValue)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["CONNECTION_TIMEOUT_MINUTES"]).Returns(configValue.ToString());

        // Act
        var result = _service.ConnectionTimeoutMinutes;

        // Assert
        Assert.Equal(expectedValue, result);
    }

    [Theory]
    [InlineData(30, 30)]
    [InlineData(120, 120)]
    [InlineData(240, 240)]
    public void OperationTimeoutMinutes_ConfiguredValue_ReturnsConfiguredValue(int configValue, int expectedValue)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["OPERATION_TIMEOUT_MINUTES"]).Returns(configValue.ToString());

        // Act
        var result = _service.OperationTimeoutMinutes;

        // Assert
        Assert.Equal(expectedValue, result);
    }

    [Fact]
    public void AasServerUrl_ConfiguredValue_ReturnsConfiguredValue()
    {
        // Arrange
        var expectedUrl = "https://test.asazure.windows.net";
        _mockConfiguration.Setup(x => x["AAS_SERVER_URL"]).Returns(expectedUrl);

        // Act
        var result = _service.AasServerUrl;

        // Assert
        Assert.Equal(expectedUrl, result);
    }

    [Fact]
    public void AasServerUrl_NoConfiguration_ReturnsDefaultValue()
    {
        // Arrange
        _mockConfiguration.Setup(x => x["AAS_SERVER_URL"]).Returns((string)null!);

        // Act
        var result = _service.AasServerUrl;

        // Assert
        Assert.Equal("asazure://southeastasia.asazure.windows.net/deheusaas", result);
    }

    [Fact]
    public void AasDatabase_ConfiguredValue_ReturnsConfiguredValue()
    {
        // Arrange
        var expectedDb = "TestDatabase";
        _mockConfiguration.Setup(x => x["AAS_DATABASE"]).Returns(expectedDb);

        // Act
        var result = _service.AasDatabase;

        // Assert
        Assert.Equal(expectedDb, result);
    }

    [Fact]
    public void AasAuthMode_ConfiguredValue_ReturnsConfiguredValue()
    {
        // Arrange
        var expectedMode = "ServicePrincipal";
        _mockConfiguration.Setup(x => x["AAS_AUTH_MODE"]).Returns(expectedMode);

        // Act
        var result = _service.AasAuthMode;

        // Assert
        Assert.Equal(expectedMode, result);
    }

    [Fact]
    public void AasAuthMode_NoConfiguration_ReturnsDefaultValue()
    {
        // Arrange
        _mockConfiguration.Setup(x => x["AAS_AUTH_MODE"]).Returns((string)null!);

        // Act
        var result = _service.AasAuthMode;

        // Assert
        Assert.Equal("ManagedIdentity", result);
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void EnableDetailedLogging_ConfiguredValue_ReturnsConfiguredValue(string configValue, bool expectedValue)
    {
        // Arrange
        _mockConfiguration.Setup(x => x["ENABLE_DETAILED_LOGGING"]).Returns(configValue);

        // Act
        var result = _service.EnableDetailedLogging;

        // Assert
        Assert.Equal(expectedValue, result);
    }

    [Fact]
    public void EnableDetailedLogging_NoConfiguration_ReturnsDefaultValue()
    {
        // Arrange
        _mockConfiguration.Setup(x => x["ENABLE_DETAILED_LOGGING"]).Returns((string)null!);

        // Act
        var result = _service.EnableDetailedLogging;

        // Assert
        Assert.True(result); // Default is true
    }

    [Fact]
    public void HeartbeatIntervalSeconds_ConfiguredValue_ReturnsConfiguredValue()
    {
        // Arrange
        _mockConfiguration.Setup(x => x["HEARTBEAT_INTERVAL_SECONDS"]).Returns("60");

        // Act
        var result = _service.HeartbeatIntervalSeconds;

        // Assert
        Assert.Equal(60, result);
    }

    [Theory]
    [InlineData(10, 600)]
    [InlineData(0, 30)]
    [InlineData(-5, 30)]
    [InlineData(61, 3600)]
    public void GetConnectTimeoutSeconds_ClampAndScale(int minutes, int expectedSeconds)
    {
        Assert.Equal(expectedSeconds, _service.GetConnectTimeoutSeconds(minutes));
    }

    [Theory]
    [InlineData(60, 15, 3720)]
    [InlineData(15, 60, 3720)]
    [InlineData(120, 15, 7200)]
    public void GetCommandTimeoutSeconds_UsesMaxMinutesPlusMargin(int opMin, int saveMin, int expectedSeconds)
    {
        Assert.Equal(expectedSeconds, _service.GetCommandTimeoutSeconds(opMin, saveMin));
    }

    [Fact]
    public void GetDefaultClientTimeouts_UsesConfigProperties()
    {
        _mockConfiguration.Setup(x => x["CONNECTION_TIMEOUT_MINUTES"]).Returns("10");
        _mockConfiguration.Setup(x => x["OPERATION_TIMEOUT_MINUTES"]).Returns("60");
        _mockConfiguration.Setup(x => x["SAVE_CHANGES_TIMEOUT_MINUTES"]).Returns("15");

        var (connect, command) = _service.GetDefaultClientTimeouts();

        Assert.Equal(600, connect);
        Assert.Equal(3720, command);
    }
}
