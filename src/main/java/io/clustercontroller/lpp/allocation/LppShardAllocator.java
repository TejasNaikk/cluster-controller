package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Bin-packing planner for LPP shards.
 *
 * <p>For each registered index the planner does two passes:
 * <ol>
 *   <li><b>Ingest pass</b>: picks {@code numIngestGroups} groups from the pool of eligible
 *       INGEST groups per shard. These groups receive segment data from the pipeline.</li>
 *   <li><b>Search pass</b>: picks {@code numSearchGroups} groups from the pool of eligible
 *       SEARCH groups per shard. These groups serve read/query traffic.</li>
 * </ol>
 *
 * <p>Today all groups handle both ingest and search ({@link LppGroup.GroupType} defaults to
 * {@code {INGEST, SEARCH}}), so the two passes select from the same pool and typically
 * produce the same assignment. The split is the infrastructure hook for introducing
 * dedicated ingest-only or search-only groups in the future.
 *
 * <p>The strategy is called separately for each pass. Load is tracked across all shards
 * and indices in a single allocation run so different shards land on different groups.
 *
 * <p><b>Example</b> — 3 groups, 6 shards, numIngestGroups=1, numSearchGroups=1:
 * <pre>
 *   shard 0 → ingest:[g0] search:[g0]   shard 3 → ingest:[g0] search:[g0]
 *   shard 1 → ingest:[g1] search:[g1]   shard 4 → ingest:[g1] search:[g1]
 *   shard 2 → ingest:[g2] search:[g2]   shard 5 → ingest:[g2] search:[g2]
 * </pre>
 *
 * <p>Strategy is pluggable via {@link AllocationStrategy}. The default is
 * {@link RandomAllocationStrategy}.
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
     * <p>Load counters are shared across all indices so the strategy can bin-pack across
     * the entire shard set in one run.
     *
     * @param groups   current group topology from discovery (groupId → LppGroup)
     * @param indices  index definitions from etcd
     * @return map of shardKey → planned allocation for all processed shards
     */
    public Map<String, LppShardPlannedAllocation> allocate(
            Map<String, LppGroup> groups,
            List<LppIndexDefinition> indices) {

        Map<String, LppShardPlannedAllocation> result = new LinkedHashMap<>();

        // Separate eligible pools for ingest and search (same today; different when dedicated groups exist)
        List<LppGroup> eligibleIngest = selectEligibleGroups(groups, LppGroup.GroupType.INGEST);
        List<LppGroup> eligibleSearch = selectEligibleGroups(groups, LppGroup.GroupType.SEARCH);

        // Per-group shard load counters — shared across all indices in this pass
        Map<String, Integer> ingestCounts = new HashMap<>();
        Map<String, Integer> searchCounts = new HashMap<>();

        for (LppIndexDefinition index : indices) {
            log.info("LPP planner: index {} — {} shards, {} ingest groups / {} search groups per shard",
                    index.getKey(), index.getNumShards(),
                    index.getNumIngestGroups(), index.getNumSearchGroups());

            if (eligibleIngest.isEmpty()) {
                log.warn("LPP planner: no eligible ingest groups for index {}, skipping", index.getKey());
                continue;
            }

            for (int shardId = 0; shardId < index.getNumShards(); shardId++) {
                LppShardEntry entry = new LppShardEntry(
                        index.getCollection(),
                        index.getIndexName(),
                        index.getFullIndexName(),
                        shardId);
                String shardKey = entry.getKey();

                // --- Ingest pass ---
                List<LppGroup> ingestGroups =
                        strategy.selectGroups(eligibleIngest, index.getNumIngestGroups(), ingestCounts);

                // --- Search pass ---
                List<LppGroup> searchGroups = eligibleSearch.isEmpty()
                        ? ingestGroups  // fall back to ingest groups when no search-specific pool
                        : strategy.selectGroups(eligibleSearch, index.getNumSearchGroups(), searchCounts);

                if (ingestGroups.isEmpty()) {
                    log.warn("LPP planner: strategy returned no groups for shard {}, skipping", shardKey);
                    continue;
                }

                // Stable allocation check — skip etcd write if group assignment unchanged
                Optional<LppShardPlannedAllocation> existing =
                        metadataStore.getShardPlannedAllocation(shardKey);
                if (existing.isPresent()
                        && isAllocationStable(existing.get(), ingestGroups, searchGroups)) {
                    log.debug("LPP planner: shard {} stable — ingest:{} search:{}", shardKey,
                            existing.get().getIngestGroupIds(), existing.get().getSearchGroupIds());
                    result.put(shardKey, existing.get());
                    continue;
                }

                LppShardPlannedAllocation allocation = new LppShardPlannedAllocation(entry);
                for (LppGroup g : ingestGroups) {
                    allocation.addIngestGroup(g);
                }
                for (LppGroup g : searchGroups) {
                    allocation.addSearchGroup(g);
                }
                allocation.setLastUpdatedMs(System.currentTimeMillis());

                metadataStore.putShardPlannedAllocation(allocation);
                result.put(shardKey, allocation);

                log.info("LPP planner: shard {} → ingest:{} search:{} (nodes:{})",
                        shardKey,
                        allocation.getIngestGroupIds(),
                        allocation.getSearchGroupIds(),
                        allocation.getAssignedNodeNames());
            }
        }

        return result;
    }

    /**
     * Returns all groups eligible for the given traffic type. A group must:
     * <ul>
     *   <li>have at least one non-drained node</li>
     *   <li>declare that it can serve the requested type</li>
     * </ul>
     */
    private List<LppGroup> selectEligibleGroups(Map<String, LppGroup> groups,
                                                 LppGroup.GroupType type) {
        return groups.values().stream()
                .filter(LppGroup::hasEligibleNodes)
                .filter(g -> g.canServe(type))
                .collect(Collectors.toList());
    }

    /**
     * An allocation is stable when both the ingest and search group ID sets match exactly.
     * Any difference triggers a rewrite (and potentially a handoff for safe migration).
     */
    private boolean isAllocationStable(LppShardPlannedAllocation existing,
                                       List<LppGroup> ingestGroups,
                                       List<LppGroup> searchGroups) {
        Set<String> desiredIngest = groupIds(ingestGroups);
        Set<String> desiredSearch = groupIds(searchGroups);
        Set<String> existingIngest = new HashSet<>(existing.getIngestGroupIds());
        Set<String> existingSearch = new HashSet<>(existing.getSearchGroupIds());
        return desiredIngest.equals(existingIngest) && desiredSearch.equals(existingSearch);
    }

    private Set<String> groupIds(List<LppGroup> groups) {
        return groups.stream().map(LppGroup::getGroupId).collect(Collectors.toSet());
    }
}
