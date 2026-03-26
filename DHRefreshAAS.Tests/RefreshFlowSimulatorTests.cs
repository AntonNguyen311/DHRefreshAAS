using DHRefreshAAS.Simulation;
using Xunit;

namespace DHRefreshAAS.Tests;

public class RefreshFlowSimulatorTests
{
    [Fact]
    public async Task FullySequential_takes_sum_of_partition_work_plus_commit()
    {
        var parts = new[]
        {
            new SimPartition("a", 30, 20),
            new SimPartition("b", 10, 40),
        };
        var elapsed = await RefreshFlowSimulator.RunFullySequentialAsync(parts, commitMs: 100);
        Assert.InRange(elapsed.TotalMilliseconds, 180, 400);
    }

    [Fact]
    public async Task ParallelPartitions_faster_than_sequential_for_identical_partitions()
    {
        // Use larger delays so the difference is clear even on a slow CI runner
        var parts = new[]
        {
            new SimPartition("a", 200, 200),
            new SimPartition("b", 200, 200),
            new SimPartition("c", 200, 200),
        };
        var sequential = await RefreshFlowSimulator.RunFullySequentialAsync(parts, commitMs: 0);
        var parallel = await RefreshFlowSimulator.RunParallelPartitionsAsync(parts, commitMs: 0);
        // Parallel should be at least 40% faster than sequential (3 partitions × 400ms vs ~400ms)
        Assert.True(parallel.TotalMilliseconds < sequential.TotalMilliseconds * 0.6,
            $"sequential={sequential.TotalMilliseconds:F0}ms parallel={parallel.TotalMilliseconds:F0}ms — parallel should be at least 40% faster");
    }

    [Fact]
    public async Task WaveExtractThenLoad_matches_max_extract_plus_max_load_plus_commit()
    {
        var parts = new[]
        {
            new SimPartition("a", 100, 30),
            new SimPartition("b", 40, 80),
        };
        var elapsed = await RefreshFlowSimulator.RunWaveExtractThenLoadAsync(parts, commitMs: 25);
        Assert.InRange(elapsed.TotalMilliseconds, 200, 350);
    }
}
