using System.Diagnostics;
using DHRefreshAAS.Simulation;

static void PrintRow(string label, TimeSpan elapsed, string? note = null)
{
    var sec = elapsed.TotalSeconds.ToString("F2");
    var extra = string.IsNullOrEmpty(note) ? "" : $"  ({note})";
    Console.WriteLine($"  {label,-28} {sec,8} s{extra}");
}

Console.WriteLine("DHRefreshAAS refresh flow simulation (no AAS — wall-clock model only)");
Console.WriteLine("Current function app queues RequestRefresh per table/partition, then one SaveChanges.");
Console.WriteLine("Server-side parallelism inside SaveChanges is not modeled exactly; use scenarios below as bounds.\n");

var rnd = new Random(42);
var partitionCount = args.Length > 0 && int.TryParse(args[0], out var n) && n > 0 ? n : 4;
var commitMs = args.Length > 1 && int.TryParse(args[1], out var c) ? Math.Max(0, c) : 800;

var parts = new List<SimPartition>();
for (var i = 0; i < partitionCount; i++)
{
    var ex = 200 + rnd.Next(600);
    var ld = 150 + rnd.Next(500);
    parts.Add(new SimPartition($"P{i + 1}", ex, ld));
}

Console.WriteLine($"Partitions: {partitionCount}, simulated SaveChanges(commit): {commitMs} ms");
Console.WriteLine("Per-partition model delay (ms):");
foreach (var p in parts)
{
    Console.WriteLine($"    {p.Id}: Extract={p.ExtractMs}, Load={p.LoadMs} (sum={p.ExtractMs + p.LoadMs})");
}

var sumEL = parts.Sum(p => p.ExtractMs + p.LoadMs);
var maxEL = parts.Max(p => p.ExtractMs + p.LoadMs);
var maxE = parts.Max(p => p.ExtractMs);
var maxL = parts.Max(p => p.LoadMs);
Console.WriteLine();
Console.WriteLine($"Theory lower/upper hints: sum(E+L)={sumEL} ms, max(E+L)={maxEL} ms, max(E)+max(L)={maxE + maxL} ms (+ commit)\n");

using var cts = new CancellationTokenSource();
Console.WriteLine("Timed runs:");
var sw = Stopwatch.StartNew();
var t1 = await RefreshFlowSimulator.RunFullySequentialAsync(parts, commitMs, cts.Token);
PrintRow("Fully sequential", t1, "each partition E then L, one after another");

var t2 = await RefreshFlowSimulator.RunParallelPartitionsAsync(parts, commitMs, cts.Token);
PrintRow("Parallel per partition", t2, "all partitions overlap E+L");

var t3 = await RefreshFlowSimulator.RunWaveExtractThenLoadAsync(parts, commitMs, cts.Token);
PrintRow("Wave: all E, then all L", t3, "overlap across phases");

var t4 = await RefreshFlowSimulator.RunLimitedParallelismAsync(parts, commitMs, maxConcurrent: 2, cts.Token);
PrintRow("Max concurrency 2", t4, "e.g. throttled engine");

sw.Stop();
Console.WriteLine();
Console.WriteLine($"Total harness time (all runs): {sw.Elapsed.TotalSeconds:F2} s");
Console.WriteLine("\nNote: Real Tabular AMO uses one connection; parallel RequestRefresh on the same Model may not be safe.");
Console.WriteLine("Use this tool to size timeouts and compare strategies before changing production code.");
