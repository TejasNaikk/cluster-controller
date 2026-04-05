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
 *   <li><b>Primary path — PA ∩ AA</b>: nodes that appear in the shard's planned allocation
 *       AND have the shard marked ACTIVE in their actual state, with a fresh heartbeat.
 *       These are the preferred routing targets.</li>
 *   <li><b>Fallback path</b>: if the intersection is empty (e.g. all planned nodes are still
 *       loading, or mid-handoff), fall back to ANY node with shard ACTIVE + fresh heartbeat,
 *       regardless of whether it is in the planned allocation. This prevents a dark shard
 *       during migrations.</li>
 *   <li><b>Empty result</b>: if even the fallback is empty, the shard gets no routes.
 *       The gateway must handle this as a temporarily unavailable shard.</li>
 * </ol>
 *
 * <h3>Version tracking</h3>
 * <p>The routing table carries a monotonically increasing {@code version} field.
 * The version is bumped only when the routing table content actually changes
 * (new nodes added, nodes removed, or shard set changes). Unchanged ticks do not
 * increment the version.
 */
@Slf4j
public class LppRoutingTableOrchestrator {

    private final LppMetadataStore metadataStore;

    public LppRoutingTableOrchestrator(LppMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Recompute the routing table from current planned allocations and actual states,
     * then persist to etcd if it changed.
     *
     * @param plannedAllocations current PA map (shardKey → allocation)
     * @return the new routing table (regardless of whether it changed)
     */
    public LppRoutingTable computeAndPersist(Map<String, LppShardPlannedAllocation> plannedAllocations) {
        if (plannedAllocations.isEmpty()) {
            log.debug("LPP routing: no planned allocations, skipping");
            return new LppRoutingTable(0);
        }

        // Load all actual states once — avoids N etcd reads in the loop
        Map<String, LppNodeActualState> allActualStates = metadataStore.getAllNodeActualStates();

        LppRoutingTable existing = metadataStore.getRoutingTable().orElse(null);
        long nextVersion = (existing != null) ? existing.getVersion() : 0;

        LppRoutingTable newTable = buildRoutingTable(plannedAllocations, allActualStates, nextVersion);

        if (hasChanged(existing, newTable)) {
            newTable.setVersion(nextVersion + 1);
            newTable.setLastUpdatedMs(System.currentTimeMillis());
            metadataStore.putRoutingTable(newTable);
            log.info("LPP routing: table updated → version={}, shards={}", newTable.getVersion(), newTable.getShardRoutes().size());
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

        for (Map.Entry<String, LppShardPlannedAllocation> entry : plannedAllocations.entrySet()) {
            String shardKey = entry.getKey();
            LppShardPlannedAllocation alloc = entry.getValue();

            List<LppShardRoute> routes = resolveRoutes(shardKey, alloc, allActualStates);
            table.setRoutes(shardKey, routes);

            if (routes.isEmpty()) {
                log.warn("LPP routing: shard {} has no routable nodes (PA={}, fallback exhausted)",
                        shardKey, alloc.getAssignedNodeNames());
            } else {
                log.debug("LPP routing: shard {} → {} nodes: {}",
                        shardKey, routes.size(),
                        routes.stream().map(LppShardRoute::getNodeName).collect(Collectors.joining(",")));
            }
        }

        return table;
    }

    /**
     * Resolve routing nodes for a single shard.
     *
     * <p>Primary: PA ∩ AA (planned + ACTIVE + fresh heartbeat).
     * Fallback: all ACTIVE + fresh heartbeat nodes for this shard.
     */
    List<LppShardRoute> resolveRoutes(
            String shardKey,
            LppShardPlannedAllocation alloc,
            Map<String, LppNodeActualState> allActualStates) {

        Set<String> paNodes = new HashSet<>(alloc.getAssignedNodeNames());

        // Primary: PA ∩ AA — nodes in PA that are ACTIVE and have fresh heartbeat
        List<LppShardRoute> primaryRoutes = paNodes.stream()
                .map(allActualStates::get)
                .filter(Objects::nonNull)
                .filter(state -> !state.isStale(LppConstants.STALE_NODE_TIMEOUT_MS))
                .filter(state -> isShardActive(state, shardKey))
                .map(state -> toRoute(state, alloc))
                .collect(Collectors.toList());

        if (!primaryRoutes.isEmpty()) {
            return primaryRoutes;
        }

        // Fallback: any node in AA with shard ACTIVE + fresh heartbeat, even if not in PA
        log.info("LPP routing: shard {} PA∩AA empty — using fallback (all ACTIVE nodes), PA={}",
                shardKey, paNodes);

        return allActualStates.values().stream()
                .filter(state -> !state.isStale(LppConstants.STALE_NODE_TIMEOUT_MS))
                .filter(state -> isShardActive(state, shardKey))
                .map(state -> toRoute(state, alloc))
                .collect(Collectors.toList());
    }

    private boolean isShardActive(LppNodeActualState state, String shardKey) {
        return state.getShardStates().stream()
                .anyMatch(ss -> shardKey.equals(ss.getShardKey()) && ss.isActive());
    }

    private LppShardRoute toRoute(LppNodeActualState state, LppShardPlannedAllocation alloc) {
        return new LppShardRoute(
                state.getNodeName(),
                state.getHost(),
                state.getPort(),
                state.getGrailShardId());
    }

    /**
     * Returns true if the new routing table differs from the existing one.
     * Compares shard-by-shard: same keys, same node names per shard.
     */
    boolean hasChanged(LppRoutingTable existing, LppRoutingTable newTable) {
        if (existing == null) {
            return !newTable.getShardRoutes().isEmpty();
        }
        Map<String, List<LppShardRoute>> oldRoutes = existing.getShardRoutes();
        Map<String, List<LppShardRoute>> newRoutes = newTable.getShardRoutes();

        if (!oldRoutes.keySet().equals(newRoutes.keySet())) {
            return true;
        }

        for (String shardKey : newRoutes.keySet()) {
            Set<String> oldNodes = nodeSet(oldRoutes.get(shardKey));
            Set<String> newNodes = nodeSet(newRoutes.get(shardKey));
            if (!oldNodes.equals(newNodes)) {
                return true;
            }
        }

        return false;
    }

    private Set<String> nodeSet(List<LppShardRoute> routes) {
        if (routes == null) return Collections.emptySet();
        return routes.stream().map(LppShardRoute::getNodeName).collect(Collectors.toSet());
    }
}
