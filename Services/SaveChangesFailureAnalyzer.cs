using DHRefreshAAS.Models;

namespace DHRefreshAAS;

/// <summary>
/// Static utility class for analyzing SaveChanges failures and categorizing their root causes.
/// </summary>
public static class SaveChangesFailureAnalyzer
{
    public static (string Category, string Source, List<string> Signals) AnalyzeSaveChangesFailure(
        Exception? exception,
        string? message,
        bool timedOut,
        bool canceled)
    {
        var combined = $"{message} {exception}".ToLowerInvariant();
        var signals = new List<string>();

        if (timedOut)
        {
            signals.Add("save-changes-timeout");
            return ("Timeout", "Unknown", signals);
        }

        if (canceled)
        {
            signals.Add("operation-canceled");
            return ("Canceled", "Unknown", signals);
        }

        if (combined.Contains("deadlock"))
        {
            signals.Add("deadlock");
            return ("Deadlock", "Unknown", signals);
        }

        if (combined.Contains("long running xmla request") ||
            combined.Contains("service upgrade") ||
            combined.Contains("server restart") ||
            combined.Contains("stuck without any updates") ||
            combined.Contains("internal service issue") ||
            combined.Contains("analysis services") ||
            combined.Contains("asazure"))
        {
            if (combined.Contains("long running xmla request")) signals.Add("xmla-request-interrupted");
            if (combined.Contains("service upgrade")) signals.Add("service-upgrade");
            if (combined.Contains("server restart")) signals.Add("server-restart");
            if (combined.Contains("stuck without any updates")) signals.Add("stuck-without-updates");
            return ("ServiceRestartOrNodeMove", "AAS", signals);
        }

        if (combined.Contains("out of memory") ||
            combined.Contains("memory error") ||
            combined.Contains("resource governing") ||
            combined.Contains("qpu") ||
            combined.Contains("capacity"))
        {
            if (combined.Contains("out of memory")) signals.Add("out-of-memory");
            if (combined.Contains("memory error")) signals.Add("memory-error");
            if (combined.Contains("resource governing")) signals.Add("resource-governing");
            return ("CapacityOrMemory", "AAS", signals);
        }

        if (combined.Contains("sql") ||
            combined.Contains("ole db") ||
            combined.Contains("odbc") ||
            combined.Contains("provider") ||
            combined.Contains("timeout expired") ||
            combined.Contains("transport-level error") ||
            combined.Contains("tcp provider") ||
            combined.Contains("login failed") ||
            combined.Contains("semaphore timeout period has expired") ||
            combined.Contains("network-related") ||
            combined.Contains("a network-related"))
        {
            if (combined.Contains("timeout expired")) signals.Add("sql-timeout");
            if (combined.Contains("login failed")) signals.Add("sql-login-failed");
            if (combined.Contains("transport-level error")) signals.Add("sql-transport-error");
            if (combined.Contains("tcp provider")) signals.Add("sql-tcp-provider");
            return ("DataSourceOrConnectivity", "AzureSQLOrDataSource", signals);
        }

        return ("Unknown", "Unknown", signals);
    }

    public static string FlattenExceptionMessages(Exception exception)
    {
        var messages = new List<string>();
        for (var current = exception; current != null; current = current.InnerException)
        {
            if (!string.IsNullOrWhiteSpace(current.Message))
            {
                messages.Add(current.Message.Trim());
            }
        }

        return messages.Count == 0
            ? exception.GetType().Name
            : string.Join(" --> ", messages.Distinct(StringComparer.Ordinal));
    }

    public static bool IsDeadlockException(Exception ex)
    {
        var message = ex.Message?.ToLowerInvariant() ?? "";
        return message.Contains("deadlock") ||
               message.Contains("was deadlocked") ||
               message.Contains("deadlock condition was detected") ||
               (message.Contains("operation was canceled") && message.Contains("deadlock"));
    }

    public static void PopulateFailureDiagnostic(
        SaveChangesDiagnostic diagnostic,
        Exception? exception,
        string fallbackMessage,
        bool timedOut,
        bool canceled)
    {
        diagnostic.IsSuccess = false;
        diagnostic.ExceptionType = exception?.GetType().Name;
        diagnostic.ErrorMessage = string.IsNullOrWhiteSpace(exception?.Message)
            ? fallbackMessage
            : FlattenExceptionMessages(exception!);

        var (category, source, signals) = AnalyzeSaveChangesFailure(exception, diagnostic.ErrorMessage, timedOut, canceled);
        diagnostic.FailureCategory = category;
        diagnostic.FailureSource = source;
        diagnostic.MatchedSignals = signals;
    }

    public static string BuildBatchFailureMessage(SaveChangesDiagnostic diagnostic)
    {
        var source = string.IsNullOrWhiteSpace(diagnostic.FailureSource) ? "UnknownSource" : diagnostic.FailureSource;
        var category = string.IsNullOrWhiteSpace(diagnostic.FailureCategory) ? "UnknownCategory" : diagnostic.FailureCategory;
        var details = string.IsNullOrWhiteSpace(diagnostic.ErrorMessage)
            ? "No inner SaveChanges error details were surfaced."
            : diagnostic.ErrorMessage;
        return $"SaveChanges failed for batch {diagnostic.BatchIndex}/{diagnostic.TotalBatches} [{source}/{category}]: {details}";
    }
}
