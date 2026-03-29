package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.models.LppGroup;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Uniform (least-loaded) allocation strategy.
 *
 * <p>For each shard, picks the {@code numGroups} groups that currently have the
 * fewest shards assigned to them in this allocation pass. Ties broken by group ID
 * for determinism. The result is an even spread of shards across the cluster with
 * no group receiving disproportionately more load than others.
 *
 * <p>Example — 6 groups, 6 index shards, numGroups=1:
 * <pre>
 *   shard 0 → [group-0]          loads: {g0=1, g1=0, g2=0, g3=0, g4=0, g5=0}
 *   shard 1 → [group-1]          loads: {g0=1, g1=1, g2=0, g3=0, g4=0, g5=0}
 *   shard 2 → [group-2]          ...
 *   shard 3 → [group-0]          (g0 tied with g1..g5, tiebreak by id)
 *   ...
 * </pre>
 *
 * <p>Example — 3 groups, 2 index shards, numGroups=3 (full replication across all groups):
 * <pre>
 *   shard 0 → [group-0, group-1, group-2]   loads: {g0=1, g1=1, g2=1}
 *   shard 1 → [group-0, group-1, group-2]   loads: {g0=2, g1=2, g2=2}
 * </pre>
 */
public class UniformAllocationStrategy implements AllocationStrategy {

    @Override
    public List<LppGroup> selectGroups(List<LppGroup> eligibleGroups, int numGroups,
                                       Map<String, Integer> groupShardCount) {
        int take = Math.min(numGroups, eligibleGroups.size());

        // Sort by current load ascending, break ties by groupId for determinism
        List<LppGroup> sorted = new ArrayList<>(eligibleGroups);
        sorted.sort(Comparator
                .comparingInt((LppGroup g) -> groupShardCount.getOrDefault(g.getGroupId(), 0))
                .thenComparing(LppGroup::getGroupId));

        List<LppGroup> selected = sorted.subList(0, take);

        // Increment load counts for selected groups
        for (LppGroup g : selected) {
            groupShardCount.merge(g.getGroupId(), 1, Integer::sum);
        }

        return new ArrayList<>(selected);
    }
}
