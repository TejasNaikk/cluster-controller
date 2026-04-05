package io.clustercontroller.lpp.discovery;

import io.clustercontroller.lpp.config.LppConstants;
import io.clustercontroller.lpp.models.LppGroup;
import io.clustercontroller.lpp.models.LppNode;
import io.clustercontroller.lpp.models.LppNodeActualState;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Discovers LPP nodes and builds the group topology.
 *
 * <p>Discovery runs in two phases:
 * <ol>
 *   <li><b>Grail phase</b>: Fetches live node topology from Grail (always runs). This works
 *       even when LPP nodes are not yet reporting to etcd. Node actual-states derived from
 *       Grail are written to etcd so the allocator has a stable view.</li>
 *   <li><b>etcd phase</b>: Merges richer shard-level data from LPP node self-reports
 *       (published at /lpp/{env}/nodes/{name}/actual-state). This phase enriches the
 *       Grail-sourced view once LPP nodes are ready.</li>
 * </ol>
 *
 * <p>Groups are derived from the {@code grailShardId} field of each node. One Odin
 * instance is the entire LPP multi-tenant cluster; within it, each {@code grailShardId}
 * identifies one replica group (3 nodes that all serve the same set of shards).
 */
@Slf4j
public class LppDiscovery {

    private final GrailClient grailClient;
    private final LppMetadataStore metadataStore;
    private final String namespace;

    public LppDiscovery(GrailClient grailClient, LppMetadataStore metadataStore, String namespace) {
        this.grailClient = grailClient;
        this.metadataStore = metadataStore;
        this.namespace = namespace;
    }

    /**
     * Run a full discovery cycle. Returns the current map of groups keyed by grailShardId.
     * The namespace identifies the Odin instance (the whole LPP cluster); groups within it
     * are differentiated by grailShardId.
     */
    public Map<String, LppGroup> discover() {
        log.info("LPP discovery starting for namespace: {}", namespace);

        // Phase 1: fetch from Grail and write to etcd as actual-state
        List<LppNodeActualState> grailNodes = grailClient.fetchNodes(namespace);
        log.info("LPP discovery: {} nodes from Grail", grailNodes.size());

        for (LppNodeActualState state : grailNodes) {
            state.setHeartbeatTimestampMs(System.currentTimeMillis());
            // Preserve shard states written by shadow sim or real LP nodes — only update
            // the Grail-derived metadata (host, port, zone, etc.).
            Optional<LppNodeActualState> existing = metadataStore.getNodeActualState(state.getNodeName());
            if (existing.isPresent() && !existing.get().getShardStates().isEmpty()) {
                state.setShardStates(existing.get().getShardStates());
                state.setShardCount(existing.get().getShardCount());
            }
            metadataStore.putNodeActualState(state);
        }

        // Phase 2: read all actual-states from etcd (may include richer LPP self-reports)
        Map<String, LppNodeActualState> etcdStates = metadataStore.getAllNodeActualStates();
        log.info("LPP discovery: {} nodes in etcd after merge", etcdStates.size());

        // Phase 3: prune stale nodes
        int pruned = pruneStaleNodes(etcdStates);
        if (pruned > 0) {
            log.info("LPP discovery: pruned {} stale nodes", pruned);
            // Re-read after pruning
            etcdStates = metadataStore.getAllNodeActualStates();
        }

        // Phase 4: build group topology
        Map<String, LppGroup> groups = buildGroups(etcdStates);
        log.info("LPP discovery complete: {} groups, {} total nodes",
                groups.size(), etcdStates.size());
        return groups;
    }

    /**
     * Remove nodes whose heartbeat is older than {@link LppConstants#STALE_NODE_TIMEOUT_MS}.
     * Also deletes the goal-state for pruned nodes so orphaned goal-states don't accumulate.
     * Returns count of pruned nodes.
     */
    private int pruneStaleNodes(Map<String, LppNodeActualState> states) {
        int count = 0;
        for (LppNodeActualState state : states.values()) {
            if (state.isStale(LppConstants.STALE_NODE_TIMEOUT_MS)) {
                log.warn("LPP discovery: pruning stale node {} (last heartbeat {}ms ago)",
                        state.getNodeName(),
                        System.currentTimeMillis() - state.getHeartbeatTimestampMs());
                metadataStore.deleteNodeActualState(state.getNodeName());
                metadataStore.deleteNodeGoalState(state.getNodeName());
                count++;
            }
        }
        return count;
    }

    /**
     * Build LppGroup objects from actual-states. Groups are keyed by grailShardId.
     * One Odin instance = the whole LPP cluster; grailShardId identifies each replica group
     * within it (each group = 3 nodes all serving the same shards).
     */
    private Map<String, LppGroup> buildGroups(Map<String, LppNodeActualState> states) {
        Map<String, LppGroup> groups = new LinkedHashMap<>();

        for (LppNodeActualState state : states.values()) {
            String groupId = state.getGrailShardId();
            if (groupId == null || groupId.isBlank()) {
                log.warn("LPP discovery: node {} has no grailShardId, skipping", state.getNodeName());
                continue;
            }

            LppGroup group = groups.computeIfAbsent(groupId,
                    id -> new LppGroup(id, state.getZone(), state.getRole()));

            LppNode node = new LppNode(
                    state.getNodeName(),
                    state.getOdinInstance(),
                    state.getGrailShardId(),
                    state.getRole(),
                    state.getZone());
            node.setHost(state.getHost());
            node.setPort(state.getPort());
            node.setLastHeartbeatMs(state.getHeartbeatTimestampMs());
            // Health is UNKNOWN until LPP nodes self-report; treat Grail-derived as healthy
            node.setHealthState("GREEN");

            group.addNode(node);
        }

        return groups;
    }
}
