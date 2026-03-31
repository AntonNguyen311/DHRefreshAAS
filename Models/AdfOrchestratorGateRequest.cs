namespace DHRefreshAAS.Models;

public sealed class AdfOrchestratorGateRequest
{
    public string? SubscriptionId { get; set; }
    public string? ResourceGroup { get; set; }
    public string? FactoryName { get; set; }
    public string? PipelineName { get; set; }
    public string? GateScope { get; set; }
}
