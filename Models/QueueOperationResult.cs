namespace DHRefreshAAS.Models;

public class QueueOperationResult
{
    public string OperationId { get; set; } = string.Empty;
    public int EstimatedDurationMinutes { get; set; }
    public bool StartedImmediately { get; set; }
    public string Status { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public int? QueuePosition { get; set; }
    public string? QueueScope { get; set; }
}
