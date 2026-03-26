using System.Diagnostics;

namespace DHRefreshAAS.Simulation;

/// <summary>
/// Simulates refresh wall-clock time for different orchestration strategies.
/// Real AAS behavior depends on engine parallelism during a single SaveChanges; this is a lab model only.
/// </summary>
public static class RefreshFlowSimulator
{
    /// <summary>
    /// One partition fully finishes extract+load before the next starts (worst-case queuing).
    /// </summary>
    public static async Task<TimeSpan> RunFullySequentialAsync(
        IReadOnlyList<SimPartition> parts,
        int commitMs,
        CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        foreach (var p in parts)
        {
            await Task.Delay(p.ExtractMs, cancellationToken).ConfigureAwait(false);
            await Task.Delay(p.LoadMs, cancellationToken).ConfigureAwait(false);
        }

        await Task.Delay(commitMs, cancellationToken).ConfigureAwait(false);
        sw.Stop();
        return sw.Elapsed;
    }

    /// <summary>
    /// Each partition runs extract then load on its own task; all partitions overlap.
    /// Wall time ~= max(Extract+Load) per partition, then commit.
    /// </summary>
    public static async Task<TimeSpan> RunParallelPartitionsAsync(
        IReadOnlyList<SimPartition> parts,
        int commitMs,
        CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        var tasks = parts.Select(async p =>
        {
            await Task.Delay(p.ExtractMs, cancellationToken).ConfigureAwait(false);
            await Task.Delay(p.LoadMs, cancellationToken).ConfigureAwait(false);
        });
        await Task.WhenAll(tasks).ConfigureAwait(false);
        await Task.Delay(commitMs, cancellationToken).ConfigureAwait(false);
        sw.Stop();
        return sw.Elapsed;
    }

    /// <summary>
    /// All extracts in parallel, then all loads in parallel (two waves), then commit.
    /// Wall time ~= max(Extract) + max(Load) + commit (when sources and sinks can overlap this way).
    /// </summary>
    public static async Task<TimeSpan> RunWaveExtractThenLoadAsync(
        IReadOnlyList<SimPartition> parts,
        int commitMs,
        CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        await Task.WhenAll(parts.Select(p => Task.Delay(p.ExtractMs, cancellationToken))).ConfigureAwait(false);
        await Task.WhenAll(parts.Select(p => Task.Delay(p.LoadMs, cancellationToken))).ConfigureAwait(false);
        await Task.Delay(commitMs, cancellationToken).ConfigureAwait(false);
        sw.Stop();
        return sw.Elapsed;
    }

    /// <summary>
    /// Bounded parallelism (e.g. server max concurrent partition refreshes).
    /// </summary>
    public static async Task<TimeSpan> RunLimitedParallelismAsync(
        IReadOnlyList<SimPartition> parts,
        int commitMs,
        int maxConcurrent,
        CancellationToken cancellationToken = default)
    {
        maxConcurrent = Math.Max(1, maxConcurrent);
        var gate = new SemaphoreSlim(maxConcurrent);
        var sw = Stopwatch.StartNew();
        var tasks = parts.Select(async p =>
        {
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await Task.Delay(p.ExtractMs, cancellationToken).ConfigureAwait(false);
                await Task.Delay(p.LoadMs, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        });
        await Task.WhenAll(tasks).ConfigureAwait(false);
        await Task.Delay(commitMs, cancellationToken).ConfigureAwait(false);
        sw.Stop();
        return sw.Elapsed;
    }
}
