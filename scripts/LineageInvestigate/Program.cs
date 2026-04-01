using Microsoft.Data.SqlClient;

var connStr = Environment.GetEnvironmentVariable("LINEAGE_SQL_CONNECTION_STRING")
              ?? Environment.GetEnvironmentVariable("SELF_SERVICE_SQL_CONNECTION_STRING");
if (string.IsNullOrWhiteSpace(connStr))
{
    Console.Error.WriteLine("Set SELF_SERVICE_SQL_CONNECTION_STRING or LINEAGE_SQL_CONNECTION_STRING (e.g. dot-source ..\\..\\load-env.ps1).");
    Environment.Exit(2);
}

await using var conn = new SqlConnection(connStr);
await conn.OpenAsync();

await DumpReaderAsync(conn, "Latest ETL.MasterEtlLineage (top 5)",
    """
    SELECT TOP 5
      LineageKey, PipelineRunId, PipelineName, CurrentStatus,
      PreviousCutoffTime, CurrentCutoffTime, CreatedDateUtc
    FROM ETL.MasterEtlLineage
    ORDER BY CreatedDateUtc DESC, LineageKey DESC;
    """);

var lkSql = """
    SELECT TOP 1 LineageKey
    FROM ETL.MasterEtlLineage
    ORDER BY CreatedDateUtc DESC, LineageKey DESC;
    """;
int latestLk;
await using (var cmd = new SqlCommand(lkSql, conn))
{
    var o = await cmd.ExecuteScalarAsync();
    latestLk = o == null || o == DBNull.Value ? -1 : Convert.ToInt32(o);
}

if (latestLk < 0)
{
    Console.WriteLine("No lineage row found.");
    return;
}

Console.WriteLine();
Console.WriteLine($"Latest LineageKey = {latestLk}");

await DumpReaderAsync(conn, $"ETL.p_GetMasterAutoRecoveryDecision @TargetLineageKey = {latestLk}",
    $"EXEC ETL.p_GetMasterAutoRecoveryDecision @TargetLineageKey = {latestLk};");

await DumpReaderAsync(conn, "vw_EtlStageSummary for latest lineage (EXTRACT/LOAD)",
    $"""
    SELECT LineageKey, StageName, TotalTableCount, SucceededCount,
           FailedTransientCount, FailedDeterministicCount, SkippedByPolicyCount, LatestActivityUtc
    FROM ETL.vw_EtlStageSummary
    WHERE LineageKey = {latestLk}
      AND StageName IN ('EXTRACT', 'LOAD')
    ORDER BY StageName;
    """);

await DumpReaderAsync(conn, "Recent MasterEtlLog for latest lineage",
    $"""
    SELECT TOP 15 LogId, LineageKey, PipelineRunId, PipelineName, EtlStatus, ErrorCode,
           LEFT(CAST(ErrorMessage AS NVARCHAR(MAX)), 180) AS ErrorMessageShort,
           CreatedDateUtc
    FROM ETL.MasterEtlLog
    WHERE LineageKey = {latestLk}
    ORDER BY CreatedDateUtc DESC, LogId DESC;
    """);

string? extractRunId = null;
await using (var cmd = new SqlCommand(
    $"""
    SELECT TOP 1 PipelineRunId
    FROM ETL.MasterEtlLog
    WHERE LineageKey = {latestLk}
      AND EtlStatus = N'EXTRACT FAILED'
    ORDER BY CreatedDateUtc DESC;
    """, conn))
{
    var o = await cmd.ExecuteScalarAsync();
    extractRunId = o as string;
}

if (!string.IsNullOrEmpty(extractRunId))
{
    await DumpReaderAsync(conn, $"ETL.EtlLog sample for EXTRACT FAILED pipeline run {extractRunId}",
        $"""
        SELECT TOP 40
          EtlLogId, LineageKey, StageName, SchemaName, TableName, PipelineRunId, ParentPipelineRunId, ChildPipelineRunId,
          EtlStatus, ActivityStatus, FailureCategory, IsTransient, CreatedDateUtc
        FROM ETL.EtlLog
        WHERE LineageKey = {latestLk}
          AND (PipelineRunId = @pr OR ParentPipelineRunId = @pr OR ChildPipelineRunId = @pr)
        ORDER BY CreatedDateUtc DESC;
        """,
        p => { p.Parameters.AddWithValue("@pr", extractRunId); });

    await DumpReaderAsync(conn, $"ETL.EtlLog EXTRACT rows for lineage {latestLk} (fallback if pipeline id match empty)",
        $"""
        SELECT TOP 40
          EtlLogId, LineageKey, StageName, SchemaName, TableName, PipelineRunId, ParentPipelineRunId, ChildPipelineRunId,
          EtlStatus, ActivityStatus, FailureCategory, IsTransient, CreatedDateUtc
        FROM ETL.EtlLog
        WHERE LineageKey = {latestLk}
          AND StageName = N'EXTRACT'
        ORDER BY CreatedDateUtc DESC;
        """);
}

await DumpReaderAsync(conn, "SSISJobInfo rows linked to latest lineage or its master run",
    $"""
    SELECT TOP 10
      j.JobID, j.JobStatus, j.JobTables, j.LineageKey, j.MasterPipelineRunId,
      j.InsertedDatetime, j.LastLinkedDateUtc
    FROM dbo.SSISJobInfo j
    WHERE j.LineageKey = {latestLk}
       OR j.MasterPipelineRunId IN (
           SELECT PipelineRunId FROM ETL.MasterEtlLineage WHERE LineageKey = {latestLk}
       )
    ORDER BY j.InsertedDatetime DESC;
    """);

Console.WriteLine();
Console.WriteLine("Done.");

static async Task RunGrantAsync(string connectionString, string[] args)
{
    var lineageKey = ParseIntArg(args, 1)
                     ?? ParseIntEnv("GRANT_LINEAGE_KEY")
                     ?? 29519;
    var jobId = ParseIntArg(args, 2)
                ?? ParseIntEnv("GRANT_RELATED_JOB_ID")
                ?? 18260;

    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();
    var expires = DateTime.UtcNow.AddHours(2);
    const string sql = """
        EXEC ETL.p_GrantMasterLineageOverride
            @TargetLineageKey = @lk,
            @ExpectedCurrentStatus = N'MASTER FAILED',
            @Reason = N'Manual unblock: EXTRACT orchestration failed with 0 table failures (ops script).',
            @RootCause = N'See docs/investigations/ADF_Extract_All_b166c7cb.md',
            @ValidatedBy = N'LineageInvestigate',
            @ApprovedBy = N'LineageInvestigate',
            @ExpiresAtUtc = @exp,
            @OverrideAction = N'ALLOW_NEXT_MASTER_RUN',
            @RelatedJobID = @job,
            @ExpectedJobStatus = N'FAILED';
        """;
    await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
    cmd.Parameters.AddWithValue("@lk", lineageKey);
    cmd.Parameters.AddWithValue("@exp", expires);
    cmd.Parameters.AddWithValue("@job", jobId);
    await using var r = await cmd.ExecuteReaderAsync();
    if (r.HasRows)
    {
        while (await r.ReadAsync())
            Console.WriteLine($"MasterLineageOverrideId = {r[0]}");
    }
    Console.WriteLine("Grant completed. Re-run investigate mode to confirm p_GetMasterAutoRecoveryDecision / next master run.");
}

static async Task ApplyMigrationAsync(string connectionString, string[] args)
{
    var path = args.Length > 1 ? args[1] : FindMigrationPath();
    if (string.IsNullOrEmpty(path) || !File.Exists(path))
    {
        Console.Error.WriteLine("Migration file not found. Pass path as second arg or place docs/migration_master_recovery_orchestration_failure.sql under repo.");
        Environment.Exit(4);
    }

    var text = await File.ReadAllTextAsync(path);
    var batches = text.Split(new[] { "\r\nGO\r\n", "\nGO\n", "\r\nGO\n", "\nGO\r\n" }, StringSplitOptions.None);
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();
    foreach (var batch in batches)
    {
        var t = batch.Trim();
        if (t.Length == 0) continue;
        await using var cmd = new SqlCommand(t, conn) { CommandTimeout = 300 };
        await cmd.ExecuteNonQueryAsync();
    }
    Console.WriteLine($"Applied migration batches from: {path}");
}

static string? FindMigrationPath()
{
    for (var dir = new DirectoryInfo(Directory.GetCurrentDirectory()); dir != null; dir = dir.Parent)
    {
        var p = Path.Combine(dir.FullName, "docs", "migration_master_recovery_orchestration_failure.sql");
        if (File.Exists(p)) return p;
    }
    var asm = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
    for (var dir = asm != null ? new DirectoryInfo(asm) : null; dir != null; dir = dir.Parent)
    {
        var p = Path.Combine(dir.FullName, "docs", "migration_master_recovery_orchestration_failure.sql");
        if (File.Exists(p)) return p;
    }
    return null;
}

static int? ParseIntArg(string[] args, int index) =>
    args.Length > index && int.TryParse(args[index], out var v) ? v : null;

static int? ParseIntEnv(string name) =>
    int.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v : null;

static async Task DumpReaderAsync(SqlConnection conn, string title, string sql, Action<SqlCommand>? bind = null)
{
    Console.WriteLine();
    Console.WriteLine($"=== {title} ===");
    await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
    bind?.Invoke(cmd);
    await using var r = await cmd.ExecuteReaderAsync();
    if (!r.HasRows)
    {
        Console.WriteLine("(no rows)");
        return;
    }
    var names = Enumerable.Range(0, r.FieldCount).Select(i => r.GetName(i)).ToArray();
    Console.WriteLine(string.Join(" | ", names));
    Console.WriteLine(new string('-', Math.Min(120, names.Sum(n => n.Length + 3))));
    while (await r.ReadAsync())
    {
        var cells = new string[r.FieldCount];
        for (var i = 0; i < r.FieldCount; i++)
        {
            var v = r.IsDBNull(i) ? "NULL" : r.GetValue(i);
            var s = v is DateTime dt ? dt.ToString("o") : v?.ToString() ?? "";
            if (s.Length > 200) s = s[..197] + "...";
            cells[i] = s;
        }
        Console.WriteLine(string.Join(" | ", cells));
    }
}
