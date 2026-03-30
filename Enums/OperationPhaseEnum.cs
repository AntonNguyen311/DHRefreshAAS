namespace DHRefreshAAS.Enums;

/// <summary>
/// Operation phase values
/// </summary>
public static class OperationPhaseEnum
{
    public const string Initializing = "Initializing";
    public const string Queued = "Queued";
    public const string ProcessingTables = "Processing Tables";
    public const string SavingChanges = "Saving Changes";
    public const string Completed = "Completed";
    public const string Failed = "Failed";
}
