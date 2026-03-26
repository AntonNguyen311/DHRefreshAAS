using Xunit;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using DHRefreshAAS;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DHRefreshAAS.Services;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Tests;

public class RequestProcessingServiceTests
{
    private readonly Mock<ILogger<RequestProcessingService>> _mockLogger;
    private readonly RequestProcessingService _service;

    public RequestProcessingServiceTests()
    {
        _mockLogger = new Mock<ILogger<RequestProcessingService>>();
        _service = new RequestProcessingService(_mockLogger.Object);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_ValidRequest_ReturnsPostData()
    {
        // Arrange
        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[]
            {
                new RefreshObject { Table = "Table1" },
                new RefreshObject { Table = "Table2" }
            }
        };

        var request = CreateMockHttpRequest(requestData);

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("TestDB", result.DatabaseName);
        Assert.NotNull(result.RefreshObjects);
        Assert.Equal(2, result.RefreshObjects.Length);
        Assert.Equal("Table1", result.RefreshObjects[0].Table);
        Assert.Equal("Table2", result.RefreshObjects[1].Table);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_EmptyRequestBody_ReturnsNull()
    {
        // Arrange
        var request = CreateMockHttpRequestWithBody("");

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_InvalidJson_ReturnsNull()
    {
        // Arrange
        var request = CreateMockHttpRequestWithBody("{ invalid json }");

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_NullDatabaseName_ReturnsNull()
    {
        // Arrange
        var requestData = new PostData
        {
            DatabaseName = null,
            RefreshObjects = new[] { new RefreshObject { Table = "Table1" } }
        };
        var request = CreateMockHttpRequest(requestData);

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_NoRefreshObjects_ReturnsNull()
    {
        // Arrange
        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = null
        };
        var request = CreateMockHttpRequest(requestData);

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_EmptyRefreshObjectsArray_ReturnsNull()
    {
        // Arrange
        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new RefreshObject[0]
        };
        var request = CreateMockHttpRequest(requestData);

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_InvalidRefreshObject_ReturnsNull()
    {
        // Arrange
        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[]
            {
                new RefreshObject { Table = "ValidTable" },
                new RefreshObject { Table = null }, // Invalid
                new RefreshObject { Table = "" }    // Invalid
            }
        };
        var request = CreateMockHttpRequest(requestData);

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_RefreshObjectWithWhitespaceTableName_ReturnsNull()
    {
        // Arrange
        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[]
            {
                new RefreshObject { Table = "ValidTable" },
                new RefreshObject { Table = "   " } // Whitespace only
            }
        };
        var request = CreateMockHttpRequest(requestData);

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void CreateEnhancedRequestData_ValidRequestData_ReturnsEnhancedData()
    {
        // Arrange
        var mockConfiguration = new Mock<IConfiguration>();
        mockConfiguration.Setup(c => c["MAX_RETRY_ATTEMPTS"]).Returns("3");
        mockConfiguration.Setup(c => c["BASE_DELAY_SECONDS"]).Returns("5");
        mockConfiguration.Setup(c => c["CONNECTION_TIMEOUT_MINUTES"]).Returns("10");
        mockConfiguration.Setup(c => c["OPERATION_TIMEOUT_MINUTES"]).Returns("30");
        var mockConfigLogger = new Mock<ILogger<ConfigurationService>>();
        var config = new ConfigurationService(mockConfiguration.Object, mockConfigLogger.Object);

        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[] { new RefreshObject { Table = "Table1" } }
        };

        // Act
        var result = _service.CreateEnhancedRequestData(requestData, config);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(requestData, result.OriginalRequest);
        Assert.Equal(3, result.MaxRetryAttempts);
        Assert.Equal(5, result.BaseDelaySeconds);
        Assert.Equal(10, result.ConnectionTimeoutMinutes);
        Assert.Equal(30, result.OperationTimeoutMinutes);
    }

    [Fact]
    public void CreateEnhancedRequestData_RequestDataOverridesConfig_ReturnsRequestValues()
    {
        // Arrange
        var mockConfiguration = new Mock<IConfiguration>();
        mockConfiguration.Setup(c => c["MAX_RETRY_ATTEMPTS"]).Returns("3");
        mockConfiguration.Setup(c => c["BASE_DELAY_SECONDS"]).Returns("5");
        mockConfiguration.Setup(c => c["CONNECTION_TIMEOUT_MINUTES"]).Returns("10");
        mockConfiguration.Setup(c => c["OPERATION_TIMEOUT_MINUTES"]).Returns("30");
        var mockConfigLogger = new Mock<ILogger<ConfigurationService>>();
        var config = new ConfigurationService(mockConfiguration.Object, mockConfigLogger.Object);

        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = new[] { new RefreshObject { Table = "Table1" } },
            MaxRetryAttempts = 5,      // Override config
            BaseDelaySeconds = 10,     // Override config
            ConnectionTimeoutMinutes = 20, // Override config
            OperationTimeoutMinutes = 60   // Override config
        };

        // Act
        var result = _service.CreateEnhancedRequestData(requestData, config);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(5, result.MaxRetryAttempts);
        Assert.Equal(10, result.BaseDelaySeconds);
        Assert.Equal(20, result.ConnectionTimeoutMinutes);
        Assert.Equal(60, result.OperationTimeoutMinutes);
    }

    [Theory]
    [InlineData(0, 5)]     // No tables
    [InlineData(1, 7)]     // 1 table
    [InlineData(5, 15)]    // 5 tables
    [InlineData(10, 25)]   // 10 tables
    [InlineData(50, 90)]   // 50 tables (capped at 90)
    public void EstimateOperationDuration_VariousTableCounts_ReturnsCorrectEstimate(int tableCount, int expectedMinutes)
    {
        // Arrange
        var refreshObjects = new RefreshObject[tableCount];
        for (int i = 0; i < tableCount; i++)
        {
            refreshObjects[i] = new RefreshObject { Table = $"Table{i}" };
        }

        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = refreshObjects
        };

        // Act
        var result = _service.EstimateOperationDuration(requestData);

        // Assert
        Assert.Equal(expectedMinutes, result);
    }

    [Fact]
    public void EstimateOperationDuration_NullRefreshObjects_ReturnsMinimumEstimate()
    {
        // Arrange
        var requestData = new PostData
        {
            DatabaseName = "TestDB",
            RefreshObjects = null
        };

        // Act
        var result = _service.EstimateOperationDuration(requestData);

        // Assert
        Assert.Equal(5, result); // Base estimate
    }

    [Fact]
    public async Task ParseAndValidateRequestAsync_CaseInsensitiveJson_ReturnsPostData()
    {
        // Arrange
        var json = @"{
            ""database_name"": ""TestDB"",
            ""refresh_objects"": [
                { ""table"": ""Table1"" }
            ]
        }";
        var request = CreateMockHttpRequestWithBody(json);

        // Act
        var result = await _service.ParseAndValidateRequestAsync(request.Object);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("TestDB", result.DatabaseName);
        Assert.NotNull(result.RefreshObjects);
        Assert.Single(result.RefreshObjects);
        Assert.Equal("Table1", result.RefreshObjects[0].Table);
    }

    // Helper methods
    private Mock<HttpRequestData> CreateMockHttpRequest(PostData data)
    {
        var json = JsonSerializer.Serialize(data);
        return CreateMockHttpRequestWithBody(json);
    }

    private Mock<HttpRequestData> CreateMockHttpRequestWithBody(string body)
    {
        var mockRequest = TestHttpHelpers.CreateHttpRequestMock();
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
        mockRequest.Setup(x => x.Body).Returns(stream);
        return mockRequest;
    }
}
