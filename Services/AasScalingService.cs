using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Identity;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS;

public class AasScalingService
{
    private readonly ConfigurationService _config;
    private readonly ILogger<AasScalingService> _logger;
    private static readonly HttpClient _httpClient = new();

    public AasScalingService(ConfigurationService config, ILogger<AasScalingService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(logger);
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Scale AAS up to the target SKU. Returns true if scaling succeeded.
    /// </summary>
    public virtual async Task<bool> ScaleUpAsync(CancellationToken cancellationToken = default)
    {
        var targetSku = _config.AasScaleUpSku;
        _logger.LogInformation("Scaling AAS up to {TargetSku}...", targetSku);
        return await ScaleToSkuAsync(targetSku, cancellationToken);
    }

    /// <summary>
    /// Scale AAS back to original SKU. Returns true if scaling succeeded.
    /// Retries 3 times to ensure scale-down happens (prevent burning money).
    /// </summary>
    public virtual async Task<bool> ScaleDownAsync(CancellationToken cancellationToken = default)
    {
        var originalSku = _config.AasOriginalSku;
        _logger.LogInformation("Scaling AAS back to {OriginalSku}...", originalSku);

        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                var result = await ScaleToSkuAsync(originalSku, cancellationToken);
                if (result) return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Scale-down attempt {Attempt}/3 failed: {ErrorMessage}", attempt, ex.Message);
                if (attempt < 3)
                {
                    var delay = TimeSpan.FromSeconds(attempt * 10);
                    _logger.LogInformation("Retrying scale-down in {DelaySeconds}s...", delay.TotalSeconds);
                    await Task.Delay(delay, cancellationToken);
                }
            }
        }

        _logger.LogCritical("CRITICAL: All 3 scale-down attempts failed! AAS is still at higher SKU. Manual intervention required.");
        return false;
    }

    private async Task<bool> ScaleToSkuAsync(string targetSku, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            // Get current SKU first
            var currentSku = await GetCurrentSkuAsync(cancellationToken);
            _logger.LogInformation("Current AAS SKU: {CurrentSku}, Target: {TargetSku}", currentSku, targetSku);

            if (string.Equals(currentSku, targetSku, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("AAS is already at {TargetSku}, skipping scale operation", targetSku);
                return true;
            }

            // Scale using REST API (PATCH)
            var token = await GetManagementTokenAsync(cancellationToken);
            var url = GetAasResourceUrl();
            var body = JsonSerializer.Serialize(new { sku = new { name = targetSku, tier = "Standard" } });

            using var request = new HttpRequestMessage(HttpMethod.Patch, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");

            var response = await _httpClient.SendAsync(request, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("AAS scale failed. Status: {StatusCode}, Response: {Response}",
                    response.StatusCode, responseBody);
                return false;
            }

            _logger.LogInformation("AAS scale request accepted (Status: {StatusCode}). Waiting for completion...",
                response.StatusCode);

            // Poll until scaling completes (max 5 minutes)
            var completed = await WaitForScalingCompleteAsync(targetSku, cancellationToken);

            var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
            if (completed)
            {
                _logger.LogInformation("AAS scaled from {CurrentSku} to {TargetSku} in {ElapsedSeconds:F1}s",
                    currentSku, targetSku, elapsed);
            }
            else
            {
                _logger.LogWarning("AAS scale to {TargetSku} may not have completed after {ElapsedSeconds:F1}s",
                    targetSku, elapsed);
            }

            return completed;
        }
        catch (Exception ex)
        {
            var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
            _logger.LogError(ex, "Failed to scale AAS to {TargetSku} after {ElapsedSeconds:F1}s: {ErrorMessage}",
                targetSku, elapsed, ex.Message);
            throw;
        }
    }

    private async Task<string?> GetCurrentSkuAsync(CancellationToken cancellationToken)
    {
        var token = await GetManagementTokenAsync(cancellationToken);
        var url = GetAasResourceUrl();

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode) return null;

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("sku").GetProperty("name").GetString();
    }

    private async Task<bool> WaitForScalingCompleteAsync(string targetSku, CancellationToken cancellationToken)
    {
        // Poll every 10 seconds, max 5 minutes
        for (int i = 0; i < 30; i++)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

            try
            {
                var currentSku = await GetCurrentSkuAsync(cancellationToken);
                var state = await GetServerStateAsync(cancellationToken);

                _logger.LogInformation("AAS scaling poll: SKU={CurrentSku}, State={State}", currentSku, state);

                if (string.Equals(currentSku, targetSku, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(state, "Succeeded", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error polling AAS state during scaling");
            }
        }

        return false;
    }

    private async Task<string?> GetServerStateAsync(CancellationToken cancellationToken)
    {
        var token = await GetManagementTokenAsync(cancellationToken);
        var url = GetAasResourceUrl();

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

    private string GetAasResourceUrl()
    {
        return $"https://management.azure.com/subscriptions/{_config.AasSubscriptionId}" +
               $"/resourceGroups/{_config.AasResourceGroup}" +
               $"/providers/Microsoft.AnalysisServices/servers/{_config.AasServerName}" +
               "?api-version=2017-08-01";
    }
}
