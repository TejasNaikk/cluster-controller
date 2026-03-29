package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.models.LppGroup;

import java.util.List;
import java.util.Map;

/**
 * Strategy for selecting which groups to assign to a given shard.
 *
 * <p>Implementations receive the full list of eligible groups and a live view of
 * how many shards each group has already been assigned in the current allocation
 * pass. They return exactly {@code numGroups} groups (or fewer if the pool is
 * smaller) that should serve the shard.
 *
 * <p>Built-in implementations:
 * <ul>
 *   <li>{@link UniformAllocationStrategy} — always picks the N least-loaded groups,
 *       producing a balanced spread across the cluster.</li>
 * </ul>
 *
 * <p>Future implementations can incorporate shard size estimates, zone affinity,
 * network topology, or historical load data without changing any other planner code.
 */
public interface AllocationStrategy {

    /**
     * Select {@code numGroups} groups to serve a shard.
     *
     * @param eligibleGroups  healthy, role-matched groups available for assignment
     * @param numGroups       how many groups should serve this shard (replication factor)
     * @param groupShardCount live shard count per group in this allocation pass;
     *                        implementations should increment counts for selected groups
     * @return the selected groups (may be fewer than numGroups if pool is smaller)
     */
    List<LppGroup> selectGroups(List<LppGroup> eligibleGroups, int numGroups,
                                Map<String, Integer> groupShardCount);
}
