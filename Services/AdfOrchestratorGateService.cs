using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

public sealed class AdfOrchestratorDispatchResult
{
    public bool Started { get; set; }
    public string Status { get; set; } = "";
    public string Message { get; set; } = "";
    public string? RunId { get; set; }
    public int ActiveRunCount { get; set; }
    public List<string> ActiveRunIds { get; set; } = new();
}

public class AdfOrchestratorGateService : IAdfOrchestratorGateService
{
    private static readonly HashSet<string> ActiveStatuses = new(StringComparer.OrdinalIgnoreCase)
    {
        "InProgress",
        "Queued",
        "Canceling"
    };

    private const string ManagementScope = "https://management.azure.com/.default";
    private const string QueryApiVersion = "2018-06-01";
    private const string CreateRunApiVersion = "2018-06-01";

    private readonly IConfigurationService _config;
    private readonly IOperationStorageService _operationStorage;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly TokenCredential _credential;
    private readonly ILogger<AdfOrchestratorGateService> _logger;

    public AdfOrchestratorGateService(
        IConfigurationService config,
        IOperationStorageService operationStorage,
        IHttpClientFactory httpClientFactory,
        ILogger<AdfOrchestratorGateService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(operationStorage);
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(logger);

        _config = config;
        _operationStorage = operationStorage;
        _httpClientFactory = httpClientFactory;
        _credential = new DefaultAzureCredential();
        _logger = logger;
    }

    public async Task<AdfOrchestratorDispatchResult> TryStartElJobIfIdleAsync(
        AdfOrchestratorGateRequest? request,
        CancellationToken cancellationToken)
    {
        var target = ResolveTarget(request);
        var leaseOwner = $"adf-gate-{Guid.NewGuid():N}";
        var staleAfter = TimeSpan.FromMinutes(_config.GetConfigValue("ADF_ORCHESTRATOR_GATE_STALE_MINUTES", 2));

        var leaseAcquired = await _operationStorage.TryAcquireQueueLeaseAsync(target.GateScope, leaseOwner, staleAfter, cancellationToken);
        if (!leaseAcquired)
        {
            return new AdfOrchestratorDispatchResult
            {
                Started = false,
                Status = "lease-busy",
                Message = $"Skipped {target.PipelineName} because another dispatch check is already in progress for factory {target.FactoryName}."
            };
        }

        try
        {
            var activeRuns = await QueryActiveRunsAsync(target, cancellationToken);
            if (activeRuns.Count > 0)
            {
                return new AdfOrchestratorDispatchResult
                {
                    Started = false,
                    Status = "already-active",
                    Message = $"Skipped {target.PipelineName} because {activeRuns.Count} active orchestrator run(s) already exist in {target.FactoryName}.",
                    ActiveRunCount = activeRuns.Count,
                    ActiveRunIds = activeRuns.Select(run => run.RunId).ToList()
                };
            }

            var runId = await CreateRunAsync(target, cancellationToken);
            await WaitForRunVisibilityAsync(target, runId, cancellationToken);

            var runsAfterStart = await QueryActiveRunsAsync(target, cancellationToken);

            return new AdfOrchestratorDispatchResult
            {
                Started = true,
                Status = "started",
                Message = $"Started {target.PipelineName} in {target.FactoryName} through the gate endpoint.",
                RunId = runId,
                ActiveRunCount = runsAfterStart.Count,
                ActiveRunIds = runsAfterStart.Select(run => run.RunId).ToList()
            };
        }
        finally
        {
            await _operationStorage.ReleaseQueueLeaseAsync(target.GateScope, leaseOwner, cancellationToken);
        }
    }

    private async Task<List<AdfPipelineRunSummary>> QueryActiveRunsAsync(
        AdfOrchestratorTarget target,
        CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        var lastUpdatedAfter = now.AddHours(-_config.GetConfigValue("ADF_ORCHESTRATOR_QUERY_LOOKBACK_HOURS", 13));
        var lastUpdatedBefore = now.AddMinutes(5);
        var result = new List<AdfPipelineRunSummary>();

        foreach (var status in ActiveStatuses)
        {
            var runsForStatus = await QueryRunsByStatusAsync(
                target,
                status,
                lastUpdatedAfter,
                lastUpdatedBefore,
                cancellationToken);

            foreach (var run in runsForStatus)
            {
                if (result.Any(existing => string.Equals(existing.RunId, run.RunId, StringComparison.OrdinalIgnoreCase)))
                {
                    continue;
                }

                result.Add(run);
            }
        }

        return result;
    }

    private async Task<List<AdfPipelineRunSummary>> QueryRunsByStatusAsync(
        AdfOrchestratorTarget target,
        string status,
        DateTime lastUpdatedAfter,
        DateTime lastUpdatedBefore,
        CancellationToken cancellationToken)
    {
        var payload = new
        {
            lastUpdatedAfter = ToAdfTimestamp(lastUpdatedAfter),
            lastUpdatedBefore = ToAdfTimestamp(lastUpdatedBefore),
            filters = new object[]
            {
                new
                {
                    operand = "PipelineName",
                    @operator = "Equals",
                    values = new[] { target.PipelineName }
                },
                new
                {
                    operand = "Status",
                    @operator = "Equals",
                    values = new[] { status }
                }
            }
        };

        using var request = await CreateArmRequestAsync(
            HttpMethod.Post,
            BuildQueryPipelineRunsUri(target),
            JsonSerializer.Serialize(payload),
            cancellationToken);

        using var client = _httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, cancellationToken);
        var content = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"ADF queryPipelineRuns failed with {(int)response.StatusCode}: {content}");
        }

        using var document = JsonDocument.Parse(content);
        if (!document.RootElement.TryGetProperty("value", out var runsElement) || runsElement.ValueKind != JsonValueKind.Array)
        {
            return new List<AdfPipelineRunSummary>();
        }

        var result = new List<AdfPipelineRunSummary>();

        foreach (var runElement in runsElement.EnumerateArray())
        {
            var runId = GetString(runElement, "runId");
            var currentStatus = GetString(runElement, "status");
            if (string.IsNullOrWhiteSpace(runId))
            {
                continue;
            }

            result.Add(new AdfPipelineRunSummary(runId, currentStatus));
        }

        return result;
    }

    private async Task<string> CreateRunAsync(AdfOrchestratorTarget target, CancellationToken cancellationToken)
    {
        using var request = await CreateArmRequestAsync(
            HttpMethod.Post,
            BuildCreateRunUri(target),
            "{}",
            cancellationToken);

        using var client = _httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, cancellationToken);
        var content = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"ADF createRun failed with {(int)response.StatusCode}: {content}");
        }

        using var document = JsonDocument.Parse(content);
        var runId = GetString(document.RootElement, "runId");
        if (string.IsNullOrWhiteSpace(runId))
        {
            throw new InvalidOperationException("ADF createRun succeeded but no runId was returned.");
        }

        _logger.LogInformation("Started ADF orchestrator pipeline {PipelineName} in factory {FactoryName} with runId {RunId}",
            target.PipelineName,
            target.FactoryName,
            runId);

        return runId;
    }

    private async Task WaitForRunVisibilityAsync(
        AdfOrchestratorTarget target,
        string runId,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(_config.GetConfigValue("ADF_ORCHESTRATOR_VISIBILITY_WAIT_SECONDS", 20));

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var activeRuns = await QueryActiveRunsAsync(target, cancellationToken);
            if (activeRuns.Any(run => string.Equals(run.RunId, runId, StringComparison.OrdinalIgnoreCase)))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }

        _logger.LogWarning(
            "ADF orchestrator run {RunId} in factory {FactoryName} was not visible before the gate lock timeout expired.",
            runId,
            target.FactoryName);
    }

    private async Task<HttpRequestMessage> CreateArmRequestAsync(
        HttpMethod method,
        string uri,
        string? jsonBody,
        CancellationToken cancellationToken)
    {
        var token = await _credential.GetTokenAsync(
            new TokenRequestContext(new[] { ManagementScope }),
            cancellationToken);

        var request = new HttpRequestMessage(method, uri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);

        if (jsonBody != null)
        {
            request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
        }

        return request;
    }

    private string BuildQueryPipelineRunsUri(AdfOrchestratorTarget target)
    {
        return
            $"https://management.azure.com/subscriptions/{target.SubscriptionId}" +
            $"/resourceGroups/{target.ResourceGroup}" +
            $"/providers/Microsoft.DataFactory/factories/{target.FactoryName}" +
            $"/queryPipelineRuns?api-version={QueryApiVersion}";
    }

    private string BuildCreateRunUri(AdfOrchestratorTarget target)
    {
        return
            $"https://management.azure.com/subscriptions/{target.SubscriptionId}" +
            $"/resourceGroups/{target.ResourceGroup}" +
            $"/providers/Microsoft.DataFactory/factories/{target.FactoryName}" +
            $"/pipelines/{Uri.EscapeDataString(target.PipelineName)}" +
            $"/createRun?api-version={CreateRunApiVersion}";
    }

    private AdfOrchestratorTarget ResolveTarget(AdfOrchestratorGateRequest? request)
    {
        var subscriptionId = request?.SubscriptionId;
        if (string.IsNullOrWhiteSpace(subscriptionId))
        {
            subscriptionId = _config.GetConfigValue("ADF_SUBSCRIPTION_ID", _config.AasSubscriptionId);
        }

        var resourceGroup = request?.ResourceGroup;
        if (string.IsNullOrWhiteSpace(resourceGroup))
        {
            resourceGroup = _config.GetConfigValue("ADF_RESOURCE_GROUP", "vn-rg-sa-sdp-solution-p");
        }

        var factoryName = request?.FactoryName;
        if (string.IsNullOrWhiteSpace(factoryName))
        {
            factoryName = _config.GetConfigValue("ADF_FACTORY_NAME", "vn-adf-sa-sdp-solution-p-42");
        }

        var pipelineName = request?.PipelineName;
        if (string.IsNullOrWhiteSpace(pipelineName))
        {
            pipelineName = _config.GetConfigValue("ADF_ORCHESTRATOR_PIPELINE_NAME", "EL Job - Start");
        }

        var gateScope = request?.GateScope;
        if (string.IsNullOrWhiteSpace(gateScope))
        {
            gateScope = $"adf-el-job-start-gate:{resourceGroup}:{factoryName}:{pipelineName}".ToLowerInvariant();
        }

        return new AdfOrchestratorTarget(subscriptionId, resourceGroup, factoryName, pipelineName, gateScope);
    }

    private static string GetString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return "";
        }

        return property.ValueKind == JsonValueKind.String
            ? property.GetString() ?? ""
            : property.ToString();
    }

    private static string ToAdfTimestamp(DateTime value)
    {
        return value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
    }

    private sealed record AdfOrchestratorTarget(
        string SubscriptionId,
        string ResourceGroup,
        string FactoryName,
        string PipelineName,
        string GateScope);

    private sealed record AdfPipelineRunSummary(string RunId, string Status);
}
