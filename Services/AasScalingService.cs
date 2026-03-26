using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Resources;
using Microsoft.Extensions.Logging;

namespace DHRefreshAAS;

public class AasScalingService
{
    private readonly ConfigurationService _config;
    private readonly ILogger<AasScalingService> _logger;

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

        // Retry scale-down 3 times - this is critical to avoid cost overrun
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
            var armClient = CreateArmClient();
            var resourceId = GetAasResourceId();

            _logger.LogInformation("Updating AAS SKU to {TargetSku} (resource: {ResourceId})", targetSku, resourceId);

            var genericResource = armClient.GetGenericResource(new Azure.Core.ResourceIdentifier(resourceId));
            var resource = await genericResource.GetAsync(cancellationToken);

            if (resource?.Value == null)
            {
                _logger.LogError("AAS resource not found: {ResourceId}", resourceId);
                return false;
            }

            // Check current SKU
            var currentSku = resource.Value.Data.Sku?.Name;
            _logger.LogInformation("Current AAS SKU: {CurrentSku}, Target: {TargetSku}", currentSku, targetSku);

            if (string.Equals(currentSku, targetSku, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("AAS is already at {TargetSku}, skipping scale operation", targetSku);
                return true;
            }

            // Update SKU
            var data = resource.Value.Data;
            data.Sku = new Azure.ResourceManager.Resources.Models.ResourcesSku { Name = targetSku, Tier = "Standard" };

            var updateOperation = await genericResource.UpdateAsync(Azure.WaitUntil.Completed, data, cancellationToken);

            var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
            _logger.LogInformation("AAS scaled from {CurrentSku} to {TargetSku} in {ElapsedSeconds:F1}s",
                currentSku, targetSku, elapsed);

            return updateOperation.HasCompleted;
        }
        catch (Exception ex)
        {
            var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
            _logger.LogError(ex, "Failed to scale AAS to {TargetSku} after {ElapsedSeconds:F1}s: {ErrorMessage}",
                targetSku, elapsed, ex.Message);
            throw;
        }
    }

    private ArmClient CreateArmClient()
    {
        var tenantId = _config.AasTenantId;
        var clientId = _config.AasUserId;
        var clientSecret = _config.AasPassword;

        if (string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret))
        {
            throw new InvalidOperationException(
                "AAS auto-scaling requires Service Principal credentials (AAS_TENANT_ID, AAS_USER_ID, AAS_PASSWORD)");
        }

        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        return new ArmClient(credential, _config.AasSubscriptionId);
    }

    private string GetAasResourceId()
    {
        return $"/subscriptions/{_config.AasSubscriptionId}" +
               $"/resourceGroups/{_config.AasResourceGroup}" +
               $"/providers/Microsoft.AnalysisServices/servers/{_config.AasServerName}";
    }
}
