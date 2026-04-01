using DHRefreshAAS.Models;
using DHRefreshAAS.Services;
using System.Text.Json;
using Xunit;

namespace DHRefreshAAS.Tests;

public class SlowTablesHtmlFormatterTests
{
    [Fact]
    public void BuildHtml_Null_ReturnsPlaceholder()
    {
        var html = SlowTablesHtmlFormatter.BuildHtml(null);
        Assert.Contains("No slow table data available", html);
    }

    [Fact]
    public void BuildHtml_EmptyList_ReturnsPlaceholder()
    {
        var html = SlowTablesHtmlFormatter.BuildHtml(Array.Empty<SlowTableEmailRow>());
        Assert.Contains("No slow table data available", html);
    }

    [Fact]
    public void BuildHtml_GroupsByDatabase_SortsSlowestFirst()
    {
        var rows = new[]
        {
            new SlowTableEmailRow { Database = "DbA", TableName = "T1", ProcessingTimeSeconds = 10 },
            new SlowTableEmailRow { Database = "DbA", TableName = "T2", ProcessingTimeSeconds = 50 },
            new SlowTableEmailRow { Database = "DbB", TableName = "T3", ProcessingTimeSeconds = 5 }
        };

        var html = SlowTablesHtmlFormatter.BuildHtml(rows);

        var idxT2 = html.IndexOf("T2", StringComparison.Ordinal);
        var idxT1 = html.IndexOf("T1", StringComparison.Ordinal);
        Assert.True(idxT2 < idxT1, "Within DbA, T2 (50s) should appear before T1 (10s)");

        Assert.Contains("DbA", html);
        Assert.Contains("DbB", html);
    }

    [Fact]
    public void BuildHtml_SeverityCritical_IncludesStyledSpan()
    {
        var rows = new[]
        {
            new SlowTableEmailRow
            {
                Database = "D",
                TableName = "Slow",
                ProcessingTimeSeconds = 400,
                Severity = "critical"
            }
        };

        var html = SlowTablesHtmlFormatter.BuildHtml(rows);
        Assert.Contains("#c0392b", html);
        Assert.Contains("critical", html);
    }

    [Fact]
    public void BuildHtml_NormalSeverity_And_RowCount_AreRendered()
    {
        var rows = new[]
        {
            new SlowTableEmailRow
            {
                Database = "D",
                TableName = "Regular",
                ProcessingTimeSeconds = 33.5,
                RowCount = 12345,
                Severity = "normal"
            }
        };

        var html = SlowTablesHtmlFormatter.BuildHtml(rows);

        Assert.Contains("12345", html);
        Assert.Contains("normal", html);
        Assert.Contains("#5d6d7e", html);
    }

    [Fact]
    public void FormatRequest_Deserializes_StringNumbers_And_EmptyRowCount()
    {
        const string json = """
            {
              "rows": [
                {
                  "database": "MM_CubeModel",
                  "tableName": "MMWH vw_fNAVBudget",
                  "partitionName": "",
                  "processingTimeSeconds": "33.5",
                  "rowCount": "",
                  "severity": ""
                }
              ]
            }
            """;

        var request = JsonSerializer.Deserialize<FormatSlowTablesHtmlRequest>(json, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        var row = Assert.Single(Assert.IsType<List<SlowTableEmailRow>>(request?.Rows));
        Assert.Equal(33.5, row.ProcessingTimeSeconds);
        Assert.Null(row.RowCount);
        Assert.Equal("MM_CubeModel", row.Database);
    }
}
