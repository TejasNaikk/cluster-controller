package io.clustercontroller.lpp.orchestration;

import io.clustercontroller.lpp.config.LppConstants;
import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Computes and writes the LPP routing table goal state.
 *
 * <p>The routing table is consumed by the LPP gateway to route ingest and search traffic.
 * It is recomputed every controller tick after shard orchestration completes.
 *
 * <h3>Routing decision per shard</h3>
 * <p>PA (Planned Allocation) is the source of truth for which nodes SHOULD serve a shard.
 * AA (Actual-state, ACTIVE) tells us which nodes ARE actively serving it right now.
 *
 * <ol>
 *   <li><b>Primary path — PA ∩ AA</b>: nodes in PA that have the shard ACTIVE + fresh heartbeat.</li>
 *   <li><b>Fallback</b>: if PA ∩ AA is empty, any node with shard ACTIVE + fresh heartbeat.</li>
 *   <li><b>Empty</b>: no routes — gateway must treat as temporarily unavailable.</li>
 * </ol>
 *
 * <h3>Output format</h3>
 * <pre>
 * {
 *   "version": 5,
 *   "index_shard_routing": {
 *     "deals_index.v1": { "0": ["node-a", "node-b"], "1": ["node-c"] },
 *     "local_index.v1": { ... }
 *   }
 * }
 * </pre>
 *
 * <h3>Version tracking</h3>
 * <p>Version bumps only when content changes (order-insensitive node set comparison).
 */
@Slf4j
public class LppRoutingTableOrchestrator {

    private final LppMetadataStore metadataStore;

    public LppRoutingTableOrchestrator(LppMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Recompute the routing table from current planned allocations and actual states,
     * then persist to etcd if changed.
     */
    public LppRoutingTable computeAndPersist(Map<String, LppShardPlannedAllocation> plannedAllocations) {
        if (plannedAllocations.isEmpty()) {
            log.debug("LPP routing: no planned allocations, skipping");
            return new LppRoutingTable(0);
        }

        Map<String, LppNodeActualState> allActualStates = metadataStore.getAllNodeActualStates();

        LppRoutingTable existing = metadataStore.getRoutingTable().orElse(null);
        long nextVersion = (existing != null) ? existing.getVersion() : 0;

        LppRoutingTable newTable = buildRoutingTable(plannedAllocations, allActualStates, nextVersion);

        if (hasChanged(existing, newTable)) {
            newTable.setVersion(nextVersion + 1);
            newTable.setLastUpdatedMs(System.currentTimeMillis());
            metadataStore.putRoutingTable(newTable);
            int totalShards = newTable.getRoutes().values().stream()
                    .mapToInt(Map::size).sum();
            log.info("LPP routing: table updated → version={}, indices={}, shards={}",
                    newTable.getVersion(), newTable.getRoutes().size(), totalShards);
        } else {
            log.debug("LPP routing: table unchanged (version={})", nextVersion);
        }

        return newTable;
    }

    /**
     * Build the routing table from PA + AA without touching etcd or version state.
     * Exposed for unit testing.
     */
    LppRoutingTable buildRoutingTable(
            Map<String, LppShardPlannedAllocation> plannedAllocations,
            Map<String, LppNodeActualState> allActualStates,
            long version) {

        LppRoutingTable table = new LppRoutingTable(version);

        for (LppShardPlannedAllocation alloc : plannedAllocations.values()) {
            String shardKey = alloc.getShardKey();
            String fullIndexName = alloc.getFullIndexName();
            int shardId = alloc.getShardId();

            List<String> nodes = resolveNodes(shardKey, alloc, allActualStates);
            table.setShardNodes(fullIndexName, shardId, nodes);

            if (nodes.isEmpty()) {
                log.warn("LPP routing: {}/{} has no routable nodes (PA={}, fallback exhausted)",
                        fullIndexName, shardId, alloc.getAssignedNodeNames().size());
            } else {
                log.debug("LPP routing: {}/{} → {} nodes", fullIndexName, shardId, nodes.size());
            }
        }

        return table;
    }

    /**
     * Resolve routable node names for a single shard.
     * Primary: PA ∩ AA. Fallback: all ACTIVE+fresh nodes.
     */
    List<String> resolveNodes(
            String shardKey,
            LppShardPlannedAllocation alloc,
            Map<String, LppNodeActualState> allActualStates) {

        // Use searcher-specific nodes if available; fall back to combined list for legacy PAs
        List<String> searcherNodes = alloc.getAssignedSearcherNodeNames();
        Set<String> paNodes = new HashSet<>(searcherNodes.isEmpty()
                ? alloc.getAssignedNodeNames()
                : searcherNodes);

        // Primary: PA ∩ AA
        List<String> primary = paNodes.stream()
                .map(allActualStates::get)
                .filter(Objects::nonNull)
                .filter(state -> !state.isStale(LppConstants.STALE_NODE_TIMEOUT_MS))
                .filter(state -> isShardActive(state, shardKey))
                .map(LppNodeActualState::getNodeName)
                .collect(Collectors.toList());

        if (!primary.isEmpty()) {
            return primary;
        }

        // Fallback: any ACTIVE+fresh searcher node for this shard
        log.info("LPP routing: {}/{} PA∩AA empty — fallback to all ACTIVE searcher nodes",
                alloc.getFullIndexName(), alloc.getShardId());

        Set<String> searcherNodeSet = new HashSet<>(searcherNodes);
        return allActualStates.values().stream()
                .filter(state -> searcherNodeSet.isEmpty() || searcherNodeSet.contains(state.getNodeName()))
                .filter(state -> !state.isStale(LppConstants.STALE_NODE_TIMEOUT_MS))
                .filter(state -> isShardActive(state, shardKey))
                .map(LppNodeActualState::getNodeName)
                .collect(Collectors.toList());
    }

    /**
     * Returns true if the new routing table differs from the existing one.
     * Compares per-shard node sets (order-insensitive).
     */
    boolean hasChanged(LppRoutingTable existing, LppRoutingTable newTable) {
        if (existing == null) {
            return !newTable.getRoutes().isEmpty();
        }
        Map<String, Map<String, List<String>>> oldRoutes = existing.getRoutes();
        Map<String, Map<String, List<String>>> newRoutes = newTable.getRoutes();

        if (!oldRoutes.keySet().equals(newRoutes.keySet())) return true;

        for (String indexName : newRoutes.keySet()) {
            Map<String, List<String>> oldShards = oldRoutes.get(indexName);
            Map<String, List<String>> newShards = newRoutes.get(indexName);
            if (!oldShards.keySet().equals(newShards.keySet())) return true;
            for (String shardId : newShards.keySet()) {
                Set<String> oldNodes = new HashSet<>(oldShards.getOrDefault(shardId, List.of()));
                Set<String> newNodes = new HashSet<>(newShards.getOrDefault(shardId, List.of()));
                if (!oldNodes.equals(newNodes)) return true;
            }
        }

        return false;
    }

    private boolean isShardActive(LppNodeActualState state, String shardKey) {
        return state.getShardStates().stream()
                .anyMatch(ss -> shardKey.equals(ss.getShardKey()) && ss.isActive());
    }
}
