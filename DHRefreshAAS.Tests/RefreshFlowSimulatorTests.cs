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
        var parts = new[]
        {
            new SimPartition("a", 50, 50),
            new SimPartition("b", 50, 50),
            new SimPartition("c", 50, 50),
        };
        var sequential = await RefreshFlowSimulator.RunFullySequentialAsync(parts, commitMs: 0);
        var parallel = await RefreshFlowSimulator.RunParallelPartitionsAsync(parts, commitMs: 0);
        Assert.True(sequential > parallel, $"sequential={sequential.TotalMilliseconds}ms should exceed parallel={parallel.TotalMilliseconds}ms");
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
