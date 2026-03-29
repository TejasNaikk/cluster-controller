package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Bin-packing planner for LPP shards.
 *
 * <p>For each registered index the planner:
 * <ol>
 *   <li>Collects all healthy, role-matched groups from the current topology.</li>
 *   <li>For each shard (0..numShards-1), calls the {@link AllocationStrategy} to
 *       pick {@code numGroups} groups — the strategy tracks per-group load so
 *       different shards land on different (least-loaded) groups.</li>
 *   <li>All nodes within each assigned group will serve that shard (they are replicas
 *       of each other). Group IDs have no inherent relationship to shard IDs — the
 *       planner's output is the authoritative assignment.</li>
 *   <li>Writes the resulting {@link LppShardPlannedAllocation} to etcd only when the
 *       assignment has changed (stable-allocation check).</li>
 * </ol>
 *
 * <p><b>Example</b> — 3 groups (g0, g1, g2), 6 shards, numGroups=1:
 * <pre>
 *   shard 0 → group g0   shard 3 → group g0
 *   shard 1 → group g1   shard 4 → group g1
 *   shard 2 → group g2   shard 5 → group g2
 * </pre>
 *
 * <p><b>Example</b> — 3 groups, 2 shards, numGroups=2:
 * <pre>
 *   shard 0 → [g0, g1]   (two groups independently replicate this shard)
 *   shard 1 → [g1, g2]   (least-loaded picks the next two)
 * </pre>
 *
 * <p>Strategy is pluggable via {@link AllocationStrategy}. The default is
 * {@link UniformAllocationStrategy} (least-loaded, deterministic). A future
 * implementation can incorporate shard size, zone diversity, or observed load
 * without changing this class.
 */
@Slf4j
public class LppShardAllocator {

    private final LppMetadataStore metadataStore;
    private final AllocationStrategy strategy;

    /** Default constructor uses random allocation strategy. */
    public LppShardAllocator(LppMetadataStore metadataStore) {
        this(metadataStore, new RandomAllocationStrategy());
    }

    /** Constructor for injecting a custom allocation strategy. */
    public LppShardAllocator(LppMetadataStore metadataStore, AllocationStrategy strategy) {
        this.metadataStore = metadataStore;
        this.strategy = strategy;
    }

    /**
     * Run a full allocation pass over all registered indices given the current group topology.
     *
     * <p>The strategy is called once per shard — not once per index — so load is tracked
     * across all shards within an allocation pass and different shards land on different groups.
     *
     * @param groups   current group topology from discovery (groupId → LppGroup)
     * @param indices  index definitions from etcd
     * @return map of shardKey → planned allocation for all processed shards
     */
    public Map<String, LppShardPlannedAllocation> allocate(
            Map<String, LppGroup> groups,
            List<LppIndexDefinition> indices) {

        Map<String, LppShardPlannedAllocation> result = new LinkedHashMap<>();

        // Live shard-count per group, shared across all indices in this pass.
        // The strategy uses this to implement bin packing.
        Map<String, Integer> groupShardCount = new HashMap<>();

        for (LppIndexDefinition index : indices) {
            log.info("LPP planner: index {} — {} shards, {} groups per shard",
                    index.getKey(), index.getNumShards(), index.getNumGroups());

            List<LppGroup> eligible = selectEligibleGroups(groups, index);
            if (eligible.isEmpty()) {
                log.warn("LPP planner: no eligible groups for index {}, skipping", index.getKey());
                continue;
            }

            for (int shardId = 0; shardId < index.getNumShards(); shardId++) {
                LppShardEntry entry = new LppShardEntry(
                        index.getCollection(),
                        index.getIndexName(),
                        index.getFullIndexName(),
                        shardId);

                String shardKey = entry.getKey();

                // Ask the strategy which groups should serve this specific shard.
                // The strategy updates groupShardCount internally.
                List<LppGroup> selectedGroups =
                        strategy.selectGroups(eligible, index.getNumGroups(), groupShardCount);

                if (selectedGroups.isEmpty()) {
                    log.warn("LPP planner: strategy returned no groups for shard {}, skipping", shardKey);
                    continue;
                }

                // Stable allocation check: if the shard already has a valid allocation
                // with exactly the same group set, keep it to avoid unnecessary etcd writes
                // and goal-state churn downstream.
                Optional<LppShardPlannedAllocation> existing =
                        metadataStore.getShardPlannedAllocation(shardKey);
                if (existing.isPresent() && isAllocationStable(existing.get(), selectedGroups)) {
                    log.debug("LPP planner: shard {} stable — groups {}", shardKey,
                            existing.get().getAssignedGroupIds());
                    result.put(shardKey, existing.get());
                    continue;
                }

                LppShardPlannedAllocation allocation = new LppShardPlannedAllocation(entry);
                for (LppGroup group : selectedGroups) {
                    allocation.addGroup(group);
                }
                allocation.setLastUpdatedMs(System.currentTimeMillis());

                metadataStore.putShardPlannedAllocation(allocation);
                result.put(shardKey, allocation);

                log.info("LPP planner: shard {} → groups {} (nodes: {})",
                        shardKey,
                        allocation.getAssignedGroupIds(),
                        allocation.getAssignedNodeNames());
            }
        }

        return result;
    }

    /**
     * Returns groups that are healthy, non-drained, and match the index role.
     * Role must match exactly — INGEST indices only go to INGEST groups, etc.
     */
    private List<LppGroup> selectEligibleGroups(Map<String, LppGroup> groups,
                                                LppIndexDefinition index) {
        return groups.values().stream()
                .filter(g -> g.getRole() != null && g.getRole().equalsIgnoreCase(index.getRole()))
                .filter(g -> !g.getNodes().isEmpty())
                .filter(LppGroup::isHealthy)
                .collect(Collectors.toList());
    }

    /**
     * An allocation is stable when its assigned group IDs match the selected groups exactly.
     * Any difference (group added, removed, or swapped) triggers a rewrite.
     */
    private boolean isAllocationStable(LppShardPlannedAllocation existing,
                                       List<LppGroup> selectedGroups) {
        Set<String> selectedIds = selectedGroups.stream()
                .map(LppGroup::getGroupId)
                .collect(Collectors.toSet());
        Set<String> existingIds = new HashSet<>(existing.getAssignedGroupIds());
        return selectedIds.equals(existingIds);
    }
}
