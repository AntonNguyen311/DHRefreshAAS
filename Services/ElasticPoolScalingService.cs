using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Identity;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS;

/// <summary>
/// Scales Azure SQL Elastic Pool up before AAS refresh and back down after,
/// following the same pattern as AasScalingService.
/// </summary>
public class ElasticPoolScalingService
{
    private readonly ConfigurationService _config;
    private readonly ILogger<ElasticPoolScalingService> _logger;
    private static readonly HttpClient _httpClient = new();

    public ElasticPoolScalingService(ConfigurationService config, ILogger<ElasticPoolScalingService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(logger);
        _config = config;
        _logger = logger;
    }

    public virtual async Task<bool> ScaleUpAsync(CancellationToken cancellationToken = default)
    {
        var targetDtu = _config.ElasticPoolScaleUpDtu;
        _logger.LogInformation("Elastic Pool auto-scaling initiated. Target DTU: {TargetDtu}, Pool: {PoolName}, Server: {ServerName}",
            targetDtu, _config.ElasticPoolName, _config.ElasticPoolServerName);
        return await ScaleToDtuAsync(targetDtu, cancellationToken);
    }

    public virtual async Task<bool> ScaleDownAsync(CancellationToken cancellationToken = default)
    {
        var originalDtu = _config.ElasticPoolOriginalDtu;
        _logger.LogInformation("Scaling Elastic Pool back to {OriginalDtu} DTU...", originalDtu);

        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                var result = await ScaleToDtuAsync(originalDtu, cancellationToken);
                if (result) return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Elastic Pool scale-down attempt {Attempt}/3 failed: {ErrorMessage}", attempt, ex.Message);
                if (attempt < 3)
                {
                    var delay = TimeSpan.FromSeconds(attempt * 10);
                    _logger.LogInformation("Retrying Elastic Pool scale-down in {DelaySeconds}s...", delay.TotalSeconds);
                    await Task.Delay(delay, cancellationToken);
                }
            }
        }

        _logger.LogCritical("CRITICAL: All 3 Elastic Pool scale-down attempts failed! Pool is still at higher DTU. Manual intervention required to avoid cost overrun.");
        return false;
    }

    private async Task<bool> ScaleToDtuAsync(int targetDtu, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            var currentDtu = await GetCurrentDtuAsync(cancellationToken);
            _logger.LogInformation("Current Elastic Pool DTU: {CurrentDtu}, Target: {TargetDtu}", currentDtu, targetDtu);

            if (currentDtu == targetDtu)
            {
                _logger.LogInformation("Elastic Pool is already at {TargetDtu} DTU, skipping scale operation", targetDtu);
                return true;
            }

            var token = await GetManagementTokenAsync(cancellationToken);
            var url = GetElasticPoolResourceUrl();
            var body = JsonSerializer.Serialize(new
            {
                sku = new { name = "StandardPool", tier = "Standard", capacity = targetDtu }
            });

            using var request = new HttpRequestMessage(HttpMethod.Patch, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");

            var response = await _httpClient.SendAsync(request, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("Elastic Pool scale failed. Status: {StatusCode}, Response: {Response}",
                    response.StatusCode, responseBody);
                return false;
            }

            _logger.LogInformation("Elastic Pool scale request accepted (Status: {StatusCode}). Waiting for completion...",
                response.StatusCode);

            var completed = await WaitForScalingCompleteAsync(targetDtu, cancellationToken);

            var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
            if (completed)
            {
                _logger.LogInformation("Elastic Pool scaled from {CurrentDtu} to {TargetDtu} DTU in {ElapsedSeconds:F1}s",
                    currentDtu, targetDtu, elapsed);
            }
            else
            {
                _logger.LogWarning("Elastic Pool scale to {TargetDtu} DTU may not have completed after {ElapsedSeconds:F1}s, continuing anyway",
                    targetDtu, elapsed);
            }

            return completed;
        }
        catch (Exception ex)
        {
            var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
            _logger.LogError(ex, "Failed to scale Elastic Pool to {TargetDtu} DTU after {ElapsedSeconds:F1}s: {ErrorMessage}",
                targetDtu, elapsed, ex.Message);
            throw;
        }
    }

    private async Task<int> GetCurrentDtuAsync(CancellationToken cancellationToken)
    {
        var token = await GetManagementTokenAsync(cancellationToken);
        var url = GetElasticPoolResourceUrl();

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode) return 0;

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("sku").GetProperty("capacity").GetInt32();
    }

    private async Task<bool> WaitForScalingCompleteAsync(int targetDtu, CancellationToken cancellationToken)
    {
        // Poll every 15 seconds, max 10 minutes (elastic pool scaling can be slower than AAS)
        for (int i = 0; i < 40; i++)
        {
            await Task.Delay(TimeSpan.FromSeconds(15), cancellationToken);

            try
            {
                var currentDtu = await GetCurrentDtuAsync(cancellationToken);
                var state = await GetPoolStateAsync(cancellationToken);

                _logger.LogInformation("Elastic Pool scaling poll: DTU={CurrentDtu}, State={State}", currentDtu, state);

                if (currentDtu == targetDtu && string.Equals(state, "Ready", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error polling Elastic Pool state during scaling");
            }
        }

        return false;
    }

    private async Task<string?> GetPoolStateAsync(CancellationToken cancellationToken)
    {
        var token = await GetManagementTokenAsync(cancellationToken);
        var url = GetElasticPoolResourceUrl();

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode) return null;

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("properties").GetProperty("state").GetString();
    }

    private async Task<string> GetManagementTokenAsync(CancellationToken cancellationToken)
    {
        var credential = new ClientSecretCredential(
            _config.AasTenantId,
            _config.AasUserId,
            _config.AasPassword);

        var tokenResult = await credential.GetTokenAsync(
            new Azure.Core.TokenRequestContext(new[] { "https://management.azure.com/.default" }),
            cancellationToken);

        return tokenResult.Token;
    }

    private string GetElasticPoolResourceUrl()
    {
        return $"https://management.azure.com/subscriptions/{_config.AasSubscriptionId}" +
               $"/resourceGroups/{_config.ElasticPoolResourceGroup}" +
               $"/providers/Microsoft.Sql/servers/{_config.ElasticPoolServerName}" +
               $"/elasticPools/{_config.ElasticPoolName}" +
               "?api-version=2021-11-01";
    }
}
