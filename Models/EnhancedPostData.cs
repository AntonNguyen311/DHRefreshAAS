namespace DHRefreshAAS.Models;

/// <summary>
/// Internal enhanced request data for processing
/// </summary>
public class EnhancedPostData
{
    public required PostData OriginalRequest { get; set; }
    public int MaxRetryAttempts { get; set; }
    public int BaseDelaySeconds { get; set; }
    public int ConnectionTimeoutMinutes { get; set; }
    public int OperationTimeoutMinutes { get; set; }
}
