using System.Net;
using System.Text;
using DHRefreshAAS.Models;

namespace DHRefreshAAS.Services;

/// <summary>
/// Builds clean, per-database HTML tables for slow-table summary emails.
/// </summary>
public static class SlowTablesHtmlFormatter
{
    private const string ContainerStyle =
        "font-family:Segoe UI,Arial,sans-serif;font-size:14px;color:#333;max-width:960px;";

    private const string TableStyle =
        "border-collapse:collapse;width:100%;margin:0 0 20px 0;border:1px solid #ddd;";

    private const string ThStyle =
        "text-align:left;padding:8px 10px;background:#f4f6f8;border-bottom:1px solid #ddd;font-weight:600;";

    private const string TdStyle =
        "padding:8px 10px;border-bottom:1px solid #eee;vertical-align:top;";

    private const string H3Style =
        "margin:24px 0 8px 0;color:#2c3e50;font-size:16px;border-bottom:1px solid #e0e0e0;padding-bottom:4px;";

    /// <summary>
    /// Groups rows by database, sorts each group by processing time descending, emits one table per database.
    /// </summary>
    public static string BuildHtml(IEnumerable<SlowTableEmailRow>? rows)
    {
        if (rows == null)
            return "<p style=\"color:#7f8c8d;\">No slow table data available.</p>";

        var list = rows
            .Where(r => r != null && !string.IsNullOrWhiteSpace(r.Database))
            .ToList();

        if (list.Count == 0)
            return "<p style=\"color:#7f8c8d;\">No slow table data available.</p>";

        var byDb = list
            .GroupBy(r => r.Database!.Trim(), StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase);

        var sb = new StringBuilder();
        sb.Append("<div style=\"").Append(ContainerStyle).Append("\">");
        sb.Append("<p style=\"margin:0 0 12px 0;color:#555;\">Tables ranked by processing time within each database (slowest first).</p>");

        foreach (var group in byDb)
        {
            var dbName = WebUtility.HtmlEncode(group.Key);
            sb.Append("<h3 style=\"").Append(H3Style).Append("\">").Append(dbName).Append("</h3>");
            sb.Append("<table style=\"").Append(TableStyle).Append("\" role=\"presentation\">");
            sb.Append("<thead><tr>");
            sb.Append("<th style=\"").Append(ThStyle).Append("\">Table</th>");
            sb.Append("<th style=\"").Append(ThStyle).Append("\">Partition</th>");
            sb.Append("<th style=\"").Append(ThStyle).Append("\">Time (s)</th>");
            sb.Append("<th style=\"").Append(ThStyle).Append("\">Rows</th>");
            sb.Append("<th style=\"").Append(ThStyle).Append("\">Severity</th>");
            sb.Append("</tr></thead><tbody>");

            foreach (var row in group
                         .OrderByDescending(r => r.ProcessingTimeSeconds ?? 0)
                         .ThenBy(r => r.TableName))
            {
                var sev = FormatSeverityCell(row.Severity);
                sb.Append("<tr>");
                sb.Append("<td style=\"").Append(TdStyle).Append("\">")
                    .Append(WebUtility.HtmlEncode(row.TableName ?? "")).Append("</td>");
                sb.Append("<td style=\"").Append(TdStyle).Append("\">")
                    .Append(WebUtility.HtmlEncode(row.PartitionName ?? "")).Append("</td>");
                sb.Append("<td style=\"").Append(TdStyle).Append("\">")
                    .Append(row.ProcessingTimeSeconds.HasValue
                        ? Math.Round(row.ProcessingTimeSeconds.Value, 1).ToString(System.Globalization.CultureInfo.InvariantCulture)
                        : "—").Append("</td>");
                sb.Append("<td style=\"").Append(TdStyle).Append("\">")
                    .Append(row.RowCount.HasValue ? row.RowCount.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) : "—").Append("</td>");
                sb.Append("<td style=\"").Append(TdStyle).Append("\">").Append(sev).Append("</td>");
                sb.Append("</tr>");
            }

            sb.Append("</tbody></table>");
        }

        sb.Append("</div>");
        return sb.ToString();
    }

    private static string FormatSeverityCell(string? severity)
    {
        if (string.IsNullOrWhiteSpace(severity))
            return "—";

        var s = severity.Trim().ToLowerInvariant();
        var label = WebUtility.HtmlEncode(severity);
        if (s == "critical")
            return "<span style=\"background:#fdecea;color:#c0392b;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;\">" + label + "</span>";
        if (s == "warning")
            return "<span style=\"background:#fff8e6;color:#b9770e;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;\">" + label + "</span>";
        if (s == "normal")
            return "<span style=\"background:#eef2f5;color:#5d6d7e;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;\">" + label + "</span>";
        return label;
    }
}
