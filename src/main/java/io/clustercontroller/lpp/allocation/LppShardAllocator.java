package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Bin-packing planner for LPP shards.
 *
 * <h3>Hybrid mode (default)</h3>
 * <p>When all groups serve both ingest and search (the current default), a single allocation
 * pass runs per shard. The scale factor ({@code numIngestGroups}) controls how many groups
 * are assigned per shard. Each selected group is registered as both ingest and search, so
 * {@code ingestGroupIds == searchGroupIds} always.
 *
 * <p><b>Example</b> — 32 groups, 4 shards, scale=1:
 * <pre>
 *   shard 0 → [g0]   shard 1 → [g1]   shard 2 → [g2]   shard 3 → [g3]
 * </pre>
 * Each group's nodes (all replicas) serve that shard for both ingest and search.
 *
 * <h3>Reader-writer separation (future)</h3>
 * <p>When dedicated ingest-only or search-only groups are introduced, the eligible pools
 * will differ and two independent passes run: ingest pass picks {@code numIngestGroups}
 * from the ingest pool; search pass picks {@code numSearchGroups} from the search pool.
 *
 * <p>Strategy is pluggable via {@link AllocationStrategy}. The default is
 * {@link RandomAllocationStrategy}.
 */
@Slf4j
public class LppShardAllocator {

    private final LppMetadataStore metadataStore;
    private final AllocationStrategy strategy;
    private final boolean hybrid;

    /** Default constructor: hybrid mode, random allocation strategy. */
    public LppShardAllocator(LppMetadataStore metadataStore) {
        this(metadataStore, new RandomAllocationStrategy(), true);
    }

    /** Constructor for injecting a custom allocation strategy. */
    public LppShardAllocator(LppMetadataStore metadataStore, AllocationStrategy strategy) {
        this(metadataStore, strategy, true);
    }

    /** Full constructor — hybrid=true for hybrid mode, false for reader-writer separation. */
    public LppShardAllocator(LppMetadataStore metadataStore, AllocationStrategy strategy, boolean hybrid) {
        this.metadataStore = metadataStore;
        this.strategy = strategy;
        this.hybrid = hybrid;
    }

    /**
     * Run a full allocation pass over all registered indices given the current group topology.
     *
     * <h3>Hybrid mode (default)</h3>
     * <p>When all eligible groups can serve both ingest and search (the current default),
     * a single allocation pass is run per shard. The number of groups selected equals
     * {@code numIngestGroups} (the scale factor). Each selected group is registered as
     * both ingest and search via {@link LppShardPlannedAllocation#addGroup}, so
     * {@code ingestGroupIds == searchGroupIds} always. This ensures scale=1 means exactly
     * 1 group per shard, scale=2 means exactly 2 groups, etc.
     *
     * <h3>Reader-writer separation mode</h3>
     * <p>When dedicated ingest-only or search-only groups exist (the eligible pools differ),
     * two independent passes run: one to pick {@code numIngestGroups} from the ingest pool
     * and one to pick {@code numSearchGroups} from the search pool. Groups and nodes are
     * unioned into {@code assignedGroupIds} / {@code assignedNodeNames} for orchestration.
     *
     * <p>Load counters are shared across all indices in a single run so the strategy
     * can bin-pack across the entire shard set at once.
     *
     * @param groups   current group topology from discovery (groupId → LppGroup)
     * @param indices  index definitions from etcd
     * @return map of shardKey → planned allocation for all processed shards
     */
    public Map<String, LppShardPlannedAllocation> allocate(
            Map<String, LppGroup> groups,
            List<LppIndexDefinition> indices) {

        Map<String, LppShardPlannedAllocation> result = new LinkedHashMap<>();

        List<LppGroup> eligibleIngest = selectEligibleGroups(groups, LppGroup.GroupType.INGEST);
        List<LppGroup> eligibleSearch = selectEligibleGroups(groups, LppGroup.GroupType.SEARCH);

        log.info("LPP planner: mode={}, ingest-pool={} groups, search-pool={} groups",
                hybrid ? "HYBRID" : "READER-WRITER", eligibleIngest.size(), eligibleSearch.size());

        // Shared load counter across all indices so bin-packing works globally.
        // In hybrid mode one counter suffices; in reader-writer mode keep separate ones.
        Map<String, Integer> ingestCounts = new HashMap<>();
        Map<String, Integer> searchCounts = new HashMap<>();

        for (LppIndexDefinition index : indices) {
            log.info("LPP planner: index {} — {} shards, mode={}, scale_per_shard={}",
                    index.getKey(), index.getNumShards(),
                    hybrid ? "HYBRID" : "READER-WRITER",
                    index.getScalePerShard().isEmpty()
                            ? "uniform(" + index.getNumIngestGroups() + ")"
                            : index.getScalePerShard());

            if (eligibleIngest.isEmpty()) {
                log.warn("LPP planner: no eligible groups for index {}, skipping", index.getKey());
                continue;
            }

            for (int shardId = 0; shardId < index.getNumShards(); shardId++) {
                int scale = index.getShardScale(shardId); // per-shard scale from index conf
                LppShardEntry entry = new LppShardEntry(
                        index.getCollection(),
                        index.getIndexName(),
                        index.getFullIndexName(),
                        shardId);
                String shardKey = entry.getKey();

                // Stable allocation check — reuse existing etcd allocation before running the
                // strategy. This is critical: without this, random strategies produce a new
                // assignment every tick, making convergence impossible.
                //
                // We do reconcile the existing PA against the live topology:
                //  - Dead groups (no longer in Grail) are pruned from the PA.
                //  - assignedNodeNames is rebuilt from the live group's current node list,
                //    which also picks up new replicas that joined since initial allocation.
                //  - If after pruning the group count drops below required scale,
                //    we fall through to re-allocate.
                Optional<LppShardPlannedAllocation> existing =
                        metadataStore.getShardPlannedAllocation(shardKey);
                if (existing.isPresent()) {
                    LppShardPlannedAllocation reconciled =
                            reconcileAllocation(existing.get(), groups, scale, shardKey);
                    if (reconciled != null) {
                        result.put(shardKey, reconciled);
                        continue;
                    }
                    // reconciled == null means group count fell below scale — re-allocate below
                    log.info("LPP planner: shard {} under-allocated after pruning dead groups, re-allocating", shardKey);
                }

                // No existing allocation — run the strategy to pick initial placement.
                LppShardPlannedAllocation allocation = new LppShardPlannedAllocation(entry);

                if (hybrid) {
                    // Single pass: scale groups handle both ingest and search.
                    List<LppGroup> selected =
                            strategy.selectGroups(eligibleIngest, scale, ingestCounts);
                    if (selected.isEmpty()) {
                        log.warn("LPP planner: strategy returned no groups for shard {}, skipping", shardKey);
                        continue;
                    }
                    for (LppGroup g : selected) {
                        allocation.addGroup(g); // registers in both ingestGroupIds and searchGroupIds
                    }
                    log.info("LPP planner: shard {} → hybrid groups:{} scale={} (nodes:{})",
                            shardKey, allocation.getIngestGroupIds(), selected.size(),
                            allocation.getAssignedNodeNames());
                } else {
                    // Two passes: dedicated ingest and search pools.
                    List<LppGroup> ingestGroups =
                            strategy.selectGroups(eligibleIngest, index.getNumIngestGroups(), ingestCounts);
                    List<LppGroup> searchGroups =
                            strategy.selectGroups(eligibleSearch, index.getNumSearchGroups(), searchCounts);
                    if (ingestGroups.isEmpty()) {
                        log.warn("LPP planner: no ingest groups for shard {}, skipping", shardKey);
                        continue;
                    }
                    for (LppGroup g : ingestGroups) allocation.addIngestGroup(g);
                    for (LppGroup g : searchGroups) allocation.addSearchGroup(g);
                    log.info("LPP planner: shard {} → ingest:{} search:{} (nodes:{})",
                            shardKey, allocation.getIngestGroupIds(), allocation.getSearchGroupIds(),
                            allocation.getAssignedNodeNames());
                }

                allocation.setLastUpdatedMs(System.currentTimeMillis());
                metadataStore.putShardPlannedAllocation(allocation);
                result.put(shardKey, allocation);
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
     * Reconcile an existing PA against the current live group topology.
     *
     * <p>For each group ID still in the PA:
     * <ul>
     *   <li>If the group is still alive in Grail, rebuild its node list from the current
     *       topology (picks up new replicas, drops dead ones).</li>
     *   <li>If the group is gone from Grail, drop it from ingest/search/assigned lists.</li>
     * </ul>
     *
     * <p>Returns the reconciled PA (persisting to etcd if changed), or {@code null} if the
     * surviving group count is below the required scale — signalling the caller to re-allocate.
     */
    private LppShardPlannedAllocation reconcileAllocation(
            LppShardPlannedAllocation existing,
            Map<String, LppGroup> liveGroups,
            int requiredScale,
            String shardKey) {

        List<String> liveIngest  = new ArrayList<>();
        List<String> liveSearch  = new ArrayList<>();
        List<String> liveAssigned = new ArrayList<>();
        List<String> liveNodes   = new ArrayList<>();

        for (String gid : existing.getAssignedGroupIds()) {
            LppGroup liveGroup = liveGroups.get(gid);
            if (liveGroup == null) {
                log.info("LPP planner: shard {} — group {} no longer in topology, pruning from PA",
                        shardKey, gid);
                continue;
            }
            liveAssigned.add(gid);
            if (existing.getIngestGroupIds().contains(gid)) liveIngest.add(gid);
            if (existing.getSearchGroupIds().contains(gid)) liveSearch.add(gid);
            // Rebuild node list from live topology (picks up new replicas, drops dead ones)
            liveGroup.getNodes().forEach(n -> {
                if (!liveNodes.contains(n.getNodeName())) {
                    liveNodes.add(n.getNodeName());
                }
            });
        }

        // If surviving groups are below required scale, signal for re-allocation
        if (liveAssigned.size() < requiredScale) {
            return null;
        }

        boolean changed = !liveAssigned.equals(existing.getAssignedGroupIds())
                || !liveNodes.equals(existing.getAssignedNodeNames());

        existing.setAssignedGroupIds(liveAssigned);
        existing.setIngestGroupIds(liveIngest);
        existing.setSearchGroupIds(liveSearch);
        existing.setAssignedNodeNames(liveNodes);

        if (changed) {
            log.info("LPP planner: shard {} — PA reconciled ({} groups, {} nodes)",
                    shardKey, liveAssigned.size(), liveNodes.size());
            existing.setLastUpdatedMs(System.currentTimeMillis());
            metadataStore.putShardPlannedAllocation(existing);
        } else {
            log.debug("LPP planner: shard {} stable (etcd)", shardKey);
        }

        return existing;
    }
}
