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
     * <p>Group selection runs once on {@code ingestGroups} per shard. The same selected
     * group IDs are then validated against {@code searchGroups}: if any selected group ID
     * is missing from the searcher pool the shard is skipped with an error log (misconfiguration).
     *
     * <p>Load counters are shared across all indices in a single run so bin-packing works
     * globally across the full shard set.
     *
     * @param ingestGroups current ingester group topology (groupId → LppGroup of ingester nodes)
     * @param searchGroups current searcher group topology (groupId → LppGroup of searcher nodes)
     * @param indices      index definitions from etcd
     * @return map of shardKey → planned allocation for all processed shards
     */
    public Map<String, LppShardPlannedAllocation> allocate(
            Map<String, LppGroup> ingestGroups,
            Map<String, LppGroup> searchGroups,
            List<LppIndexDefinition> indices) {

        // Combined group map for legacy reconciliation (group presence check uses union)
        Map<String, LppGroup> allGroups = new LinkedHashMap<>(ingestGroups);
        allGroups.putAll(searchGroups);

        Map<String, LppShardPlannedAllocation> result = new LinkedHashMap<>();

        List<LppGroup> eligibleIngest = selectEligibleGroups(ingestGroups, LppGroup.GroupType.INGEST);

        log.info("LPP planner: role-aware mode, ingest-pool={} groups, search-pool={} groups",
                eligibleIngest.size(), searchGroups.size());

        // Shared load counter across all indices for global bin-packing
        Map<String, Integer> ingestCounts = new HashMap<>();

        for (LppIndexDefinition index : indices) {
            log.info("LPP planner: index {} — {} shards, scale_per_shard={}",
                    index.getKey(), index.getNumShards(),
                    index.getScalePerShard().isEmpty()
                            ? "uniform(" + index.getNumIngestGroups() + ")"
                            : index.getScalePerShard());

            if (eligibleIngest.isEmpty()) {
                log.warn("LPP planner: no eligible ingester groups for index {}, skipping", index.getKey());
                continue;
            }

            for (int shardId = 0; shardId < index.getNumShards(); shardId++) {
                int scale = index.getShardScale(shardId);
                LppShardEntry entry = new LppShardEntry(
                        index.getCollection(),
                        index.getIndexName(),
                        index.getFullIndexName(),
                        shardId);
                String shardKey = entry.getKey();

                // Stable allocation check — reuse existing etcd allocation before running the
                // strategy. Reconciles existing PA against live topology (prunes dead groups,
                // rebuilds node lists).
                Optional<LppShardPlannedAllocation> existing =
                        metadataStore.getShardPlannedAllocation(shardKey);
                if (existing.isPresent()) {
                    LppShardPlannedAllocation reconciled =
                            reconcileAllocation(existing.get(), ingestGroups, searchGroups, scale, shardKey);
                    if (reconciled != null) {
                        result.put(shardKey, reconciled);
                        continue;
                    }
                    log.info("LPP planner: shard {} under-allocated after pruning dead groups, re-allocating", shardKey);
                }

                // No stable allocation — run strategy on ingester pool to select group IDs
                List<LppGroup> selectedIngestGroups =
                        strategy.selectGroups(eligibleIngest, scale, ingestCounts);
                if (selectedIngestGroups.isEmpty()) {
                    log.warn("LPP planner: strategy returned no ingester groups for shard {}, skipping", shardKey);
                    continue;
                }

                // Validate that every selected group ID exists in the searcher pool
                boolean searcherMismatch = false;
                for (LppGroup ingestGroup : selectedIngestGroups) {
                    if (!searchGroups.containsKey(ingestGroup.getGroupId())) {
                        log.error("LPP planner: shard {} — selected ingester group '{}' has no corresponding " +
                                "searcher group; skipping shard (misconfiguration)",
                                shardKey, ingestGroup.getGroupId());
                        searcherMismatch = true;
                        break;
                    }
                }
                if (searcherMismatch) continue;

                LppShardPlannedAllocation allocation = new LppShardPlannedAllocation(entry);

                for (LppGroup ingestGroup : selectedIngestGroups) {
                    allocation.addIngestGroup(ingestGroup);
                    LppGroup searchGroup = searchGroups.get(ingestGroup.getGroupId());
                    allocation.addSearchGroup(searchGroup);
                }

                log.info("LPP planner: shard {} → groups:{} ingesters:{} searchers:{}",
                        shardKey,
                        allocation.getAssignedGroupIds(),
                        allocation.getAssignedIngesterNodeNames(),
                        allocation.getAssignedSearcherNodeNames());

                allocation.setLastUpdatedMs(System.currentTimeMillis());
                metadataStore.putShardPlannedAllocation(allocation);
                result.put(shardKey, allocation);
            }
        }

        return result;
    }

    // Legacy single-map overload — kept for backward compatibility with tests/callers
    @Deprecated
    public Map<String, LppShardPlannedAllocation> allocate(
            Map<String, LppGroup> groups,
            List<LppIndexDefinition> indices) {
        return allocate(groups, groups, indices);
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
     *   <li>If the group is still alive, rebuild its ingester and searcher node lists from
     *       the current topology (picks up new replicas, drops dead ones).</li>
     *   <li>If the group is gone from both pools, drop it from all lists.</li>
     * </ul>
     *
     * <p>Returns the reconciled PA (persisting to etcd if changed), or {@code null} if the
     * surviving group count is below the required scale — signalling the caller to re-allocate.
     */
    private LppShardPlannedAllocation reconcileAllocation(
            LppShardPlannedAllocation existing,
            Map<String, LppGroup> ingestGroups,
            Map<String, LppGroup> searchGroups,
            int requiredScale,
            String shardKey) {

        List<String> liveIngest   = new ArrayList<>();
        List<String> liveSearch   = new ArrayList<>();
        List<String> liveAssigned = new ArrayList<>();
        List<String> liveIngesterNodes = new ArrayList<>();
        List<String> liveSearcherNodes = new ArrayList<>();
        List<String> liveAllNodes = new ArrayList<>();

        for (String gid : existing.getAssignedGroupIds()) {
            LppGroup ingestGroup = ingestGroups.get(gid);
            LppGroup searchGroup = searchGroups.get(gid);

            if (ingestGroup == null && searchGroup == null) {
                log.info("LPP planner: shard {} — group {} no longer in topology, pruning from PA",
                        shardKey, gid);
                continue;
            }

            liveAssigned.add(gid);
            if (existing.getIngestGroupIds().contains(gid)) liveIngest.add(gid);
            if (existing.getSearchGroupIds().contains(gid)) liveSearch.add(gid);

            if (ingestGroup != null) {
                ingestGroup.getNodes().forEach(n -> {
                    if (!liveIngesterNodes.contains(n.getNodeName())) liveIngesterNodes.add(n.getNodeName());
                    if (!liveAllNodes.contains(n.getNodeName())) liveAllNodes.add(n.getNodeName());
                });
            }
            if (searchGroup != null) {
                searchGroup.getNodes().forEach(n -> {
                    if (!liveSearcherNodes.contains(n.getNodeName())) liveSearcherNodes.add(n.getNodeName());
                    if (!liveAllNodes.contains(n.getNodeName())) liveAllNodes.add(n.getNodeName());
                });
            }
        }

        if (liveAssigned.size() < requiredScale) {
            return null;
        }

        boolean changed = !liveAssigned.equals(existing.getAssignedGroupIds())
                || !liveAllNodes.equals(existing.getAssignedNodeNames());

        existing.setAssignedGroupIds(liveAssigned);
        existing.setIngestGroupIds(liveIngest);
        existing.setSearchGroupIds(liveSearch);
        existing.setAssignedNodeNames(liveAllNodes);
        existing.setAssignedIngesterNodeNames(liveIngesterNodes);
        existing.setAssignedSearcherNodeNames(liveSearcherNodes);

        if (changed) {
            log.info("LPP planner: shard {} — PA reconciled ({} groups, {} ingesters, {} searchers)",
                    shardKey, liveAssigned.size(), liveIngesterNodes.size(), liveSearcherNodes.size());
            existing.setLastUpdatedMs(System.currentTimeMillis());
            metadataStore.putShardPlannedAllocation(existing);
        } else {
            log.debug("LPP planner: shard {} stable (etcd)", shardKey);
        }

        return existing;
    }
}
