package io.clustercontroller.lpp.tasks;

import io.clustercontroller.lpp.config.LppConstants;
import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Discovers LPP nodes from etcd actual-state heartbeats.
 *
 * <p>Replaces the Grail-based {@link LppDiscoveryTask}. Nodes self-report their actual state
 * to etcd at /lpp/{region}/nodes/{nodeName}/actual-state, including their {@code group_id}
 * and {@code role} ("ingester" or "searcher").
 *
 * <h3>Stale node cleanup</h3>
 * <p>Nodes whose heartbeat is older than {@link LppConstants#STALE_NODE_TIMEOUT_MS} (10 min)
 * are treated as dead and cleaned up everywhere:
 * <ul>
 *   <li>Actual-state deleted from etcd</li>
 *   <li>Goal-state deleted from etcd</li>
 *   <li>Removed from any planned allocations that reference them</li>
 * </ul>
 *
 * <h3>Group topology</h3>
 * <p>Live nodes are grouped by {@code group_id} into two separate maps:
 * <ul>
 *   <li>{@code ingestGroups} — groups containing ingester nodes (role = "ingester")</li>
 *   <li>{@code searchGroups} — groups containing searcher nodes (role = "searcher")</li>
 * </ul>
 * The same {@code group_id} can appear in both maps. These are stored in
 * {@link LppTaskContext} for the allocation and orchestration tasks.
 */
@Slf4j
public class LppActualStateDiscoveryTask {

    public static final String NAME = "lpp-actual-state-discovery";

    private static final String ROLE_INGESTER = "ingester";
    private static final String ROLE_SEARCHER = "searcher";

    private final LppTaskContext ctx;

    public LppActualStateDiscoveryTask(LppTaskContext ctx) {
        this.ctx = ctx;
    }

    public String execute() {
        try {
            LppMetadataStore store = ctx.getMetadataStore();

            Map<String, LppNodeActualState> allActualStates = store.getAllNodeActualStates();
            log.info("LPP actual-state discovery: {} nodes in etcd", allActualStates.size());

            List<String> staleNodes = new ArrayList<>();
            Map<String, LppGroup> ingestGroups = new LinkedHashMap<>();
            Map<String, LppGroup> searchGroups = new LinkedHashMap<>();

            for (Map.Entry<String, LppNodeActualState> entry : allActualStates.entrySet()) {
                String nodeName = entry.getKey();
                LppNodeActualState state = entry.getValue();

                if (state.isStale(LppConstants.STALE_NODE_TIMEOUT_MS)) {
                    staleNodes.add(nodeName);
                    continue;
                }

                String groupId = resolveGroupId(state);
                if (groupId == null || groupId.isBlank()) {
                    log.warn("LPP discovery: node {} has no group_id, skipping", nodeName);
                    continue;
                }

                String role = state.getRole();
                LppNode node = buildNode(state);

                if (ROLE_INGESTER.equalsIgnoreCase(role)) {
                    ingestGroups.computeIfAbsent(groupId, id -> new LppGroup(id, state.getZone(), ROLE_INGESTER))
                            .addNode(node);
                } else if (ROLE_SEARCHER.equalsIgnoreCase(role)) {
                    searchGroups.computeIfAbsent(groupId, id -> new LppGroup(id, state.getZone(), ROLE_SEARCHER))
                            .addNode(node);
                } else {
                    log.warn("LPP discovery: node {} has unknown role '{}', skipping", nodeName, role);
                }
            }

            if (!staleNodes.isEmpty()) {
                log.info("LPP discovery: {} stale node(s) — cleaning up: {}", staleNodes.size(), staleNodes);
                cleanupStaleNodes(staleNodes, store);
            }

            ctx.setCurrentIngestGroups(ingestGroups);
            ctx.setCurrentSearchGroups(searchGroups);

            log.info("LPP actual-state discovery complete: {} ingester group(s), {} searcher group(s), {} stale removed",
                    ingestGroups.size(), searchGroups.size(), staleNodes.size());
            return "SUCCESS";
        } catch (Exception e) {
            log.error("LPP actual-state discovery task failed", e);
            return "FAILED";
        }
    }

    /**
     * Returns the group ID for a node. Prefers {@code groupId} (self-reported), falls back
     * to {@code grailShardId} for backward compatibility with legacy nodes.
     */
    private String resolveGroupId(LppNodeActualState state) {
        if (state.getGroupId() != null && !state.getGroupId().isBlank()) {
            return state.getGroupId();
        }
        return state.getGrailShardId();
    }

    private LppNode buildNode(LppNodeActualState state) {
        LppNode node = new LppNode(
                state.getNodeName(),
                state.getOdinInstance(),
                state.getGrailShardId(),
                state.getRole(),
                state.getZone());
        node.setHost(state.getHost());
        node.setPort(state.getPort() > 0 ? state.getPort() : 9090);
        node.setLastHeartbeatMs(state.getHeartbeatTimestampMs());
        node.setHealthState("GREEN"); // self-reporting nodes are considered healthy
        return node;
    }

    /**
     * Cleans up stale nodes from etcd: deletes their actual-state, goal-state, and removes
     * them from any planned allocations that still reference them.
     */
    private void cleanupStaleNodes(List<String> staleNodes, LppMetadataStore store) {
        Set<String> staleSet = new HashSet<>(staleNodes);

        // Remove from planned allocations first (before deleting actual-state so logs are clear)
        Map<String, LppShardPlannedAllocation> allPAs = store.getAllShardPlannedAllocations();
        for (LppShardPlannedAllocation pa : allPAs.values()) {
            boolean changed = pa.getAssignedNodeNames().removeIf(staleSet::contains)
                    | pa.getAssignedIngesterNodeNames().removeIf(staleSet::contains)
                    | pa.getAssignedSearcherNodeNames().removeIf(staleSet::contains);
            if (changed) {
                log.info("LPP discovery: removed stale node(s) from PA {}", pa.getShardKey());
                store.putShardPlannedAllocation(pa);
            }
        }

        // Delete actual-state and goal-state for each stale node
        for (String nodeName : staleNodes) {
            log.info("LPP discovery: deleting stale node {} (heartbeat expired)", nodeName);
            store.deleteNodeActualState(nodeName);
            store.deleteNodeGoalState(nodeName);
        }
    }

    public String getName() { return NAME; }
}
