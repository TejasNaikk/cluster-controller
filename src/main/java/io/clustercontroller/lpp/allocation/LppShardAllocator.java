package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.config.LppConstants;
import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Bin-packing allocator for LPP shards.
 *
 * <p>For each registered index the allocator:
 * <ol>
 *   <li>Enumerates all shards (0..numShards-1)</li>
 *   <li>Selects {@code numGroups} groups from the available pool that are healthy and
 *       not drained</li>
 *   <li>Assigns each shard to all selected groups (every group serves every shard —
 *       groups are full replicas, not partial)</li>
 *   <li>Writes the resulting {@link LppShardPlannedAllocation} to etcd</li>
 * </ol>
 *
 * <p>Stable allocation: if a shard already has a valid allocation whose groups are all
 * still healthy, the existing allocation is kept unchanged to avoid unnecessary churn.
 */
@Slf4j
public class LppShardAllocator {

    private final LppMetadataStore metadataStore;

    public LppShardAllocator(LppMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Run a full allocation pass over all registered indices given the current group topology.
     *
     * @param groups   current group topology from {@link io.clustercontroller.lpp.discovery.LppDiscovery}
     * @param indices  index definitions from {@link LppMetadataStore#getAllIndexDefinitions()}
     * @return map of shardKey → planned allocation
     */
    public Map<String, LppShardPlannedAllocation> allocate(
            Map<String, LppGroup> groups,
            List<LppIndexDefinition> indices) {

        Map<String, LppShardPlannedAllocation> result = new LinkedHashMap<>();

        for (LppIndexDefinition index : indices) {
            log.info("LPP allocator: processing index {} ({} shards, {} desired groups)",
                    index.getKey(), index.getNumShards(), index.getNumGroups());

            List<LppGroup> eligibleGroups = selectEligibleGroups(groups, index);
            if (eligibleGroups.isEmpty()) {
                log.warn("LPP allocator: no eligible groups for index {}, skipping", index.getKey());
                continue;
            }

            List<LppGroup> selectedGroups = selectGroups(eligibleGroups, index.getNumGroups());
            log.info("LPP allocator: selected {} groups for index {}", selectedGroups.size(), index.getKey());

            for (int shardId = 0; shardId < index.getNumShards(); shardId++) {
                LppShardEntry entry = new LppShardEntry(
                        index.getCollection(),
                        index.getIndexName(),
                        index.getFullIndexName(),
                        shardId);

                String shardKey = entry.getKey();

                // Stable allocation check
                Optional<LppShardPlannedAllocation> existing =
                        metadataStore.getShardPlannedAllocation(shardKey);
                if (existing.isPresent() && isAllocationStable(existing.get(), selectedGroups)) {
                    log.debug("LPP allocator: shard {} allocation is stable, keeping", shardKey);
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
                log.info("LPP allocator: shard {} → groups {}", shardKey, allocation.getAssignedGroupIds());
            }
        }

        return result;
    }

    /**
     * Filter groups to those that match the index role, are healthy, and are not drained.
     */
    private List<LppGroup> selectEligibleGroups(Map<String, LppGroup> groups, LppIndexDefinition index) {
        return groups.values().stream()
                .filter(g -> g.getRole() != null && g.getRole().equalsIgnoreCase(index.getRole()))
                .filter(g -> !g.getNodes().isEmpty())
                .filter(LppGroup::isHealthy)
                .collect(Collectors.toList());
    }

    /**
     * Select up to {@code numGroups} groups. If fewer healthy groups are available,
     * use all of them (best-effort).
     *
     * <p>Selection is currently in insertion order (stable). Future improvement:
     * use load-based selection (least shards assigned).
     */
    private List<LppGroup> selectGroups(List<LppGroup> eligible, int numGroups) {
        if (eligible.size() <= numGroups) {
            return new ArrayList<>(eligible);
        }
        return eligible.subList(0, numGroups);
    }

    /**
     * An existing allocation is stable if every assigned group is still in the eligible
     * (healthy, non-drained) group set.
     */
    private boolean isAllocationStable(LppShardPlannedAllocation existing, List<LppGroup> selectedGroups) {
        Set<String> selectedIds = selectedGroups.stream()
                .map(LppGroup::getGroupId)
                .collect(Collectors.toSet());
        return selectedIds.containsAll(existing.getAssignedGroupIds())
                && existing.getAssignedGroupIds().containsAll(selectedIds);
    }
}
