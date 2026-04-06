using DHRefreshAAS.Models;
using DHRefreshAAS.Services;

namespace DHRefreshAAS;

/// <summary>
/// Applies slow-table severity metrics and performance warnings to refresh responses.
/// </summary>
public class SlowTableMetricsService
{
    private readonly IConfigurationService _config;

    public SlowTableMetricsService(IConfigurationService config)
    {
        _config = config;
    }

    public virtual void ApplySlowTableMetrics(ActivityResponse response, EnhancedPostData requestData)
    {
        var dbName = requestData.OriginalRequest?.DatabaseName?.Trim() ?? "";
        var warnSec = _config.SlowTableWarningSeconds;
        var critSec = _config.SlowTableCriticalSeconds;

        foreach (var r in response.TopSlowTables ?? [])
        {
            r.DatabaseName = dbName;
            r.Severity = ClassifySlowTableSeverity(r.ProcessingTimeSeconds, warnSec, critSec);
        }

        response.PerformanceWarnings = response.RefreshResults
            .Where(r => r.ProcessingTimeSeconds.HasValue && r.ProcessingTimeSeconds >= warnSec)
            .Select(r => new PerformanceWarningItem
            {
                DatabaseName = dbName,
                TableName = r.TableName,
                PartitionName = r.PartitionName ?? "",
                ProcessingTimeSeconds = r.ProcessingTimeSeconds ?? 0,
                Severity = ClassifySlowTableSeverity(r.ProcessingTimeSeconds, warnSec, critSec) ?? "warning"
            })
            .OrderByDescending(w => w.ProcessingTimeSeconds)
            .ToList();
    }

    public static string ClassifySlowTableSeverity(double? seconds, int warnSec, int critSec)
    {
        if (!seconds.HasValue || seconds.Value <= 0) return "normal";
        if (seconds.Value >= critSec) return "critical";
        if (seconds.Value >= warnSec) return "warning";
        return "normal";
    }
}
