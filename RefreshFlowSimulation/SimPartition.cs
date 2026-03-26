namespace DHRefreshAAS.Simulation;

/// <summary>
/// Stylized partition work used only for timing simulation (no AMO / AAS).
/// </summary>
public sealed record SimPartition(string Id, int ExtractMs, int LoadMs);
