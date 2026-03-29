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
 * <p>Groups are derived from the {@code odinInstance} field of each node — one Odin
 * instance = one replica group. The {@code grailShardId} is a temporary secondary
 * grouping key used until LPP nodes expose their own shard registry.
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
     * Run a full discovery cycle. Returns the current map of groups keyed by groupId
     * (= Odin instance name).
     */
    public Map<String, LppGroup> discover() {
        log.info("LPP discovery starting for namespace: {}", namespace);

        // Phase 1: fetch from Grail and write to etcd as actual-state
        List<LppNodeActualState> grailNodes = grailClient.fetchNodes(namespace);
        log.info("LPP discovery: {} nodes from Grail", grailNodes.size());

        for (LppNodeActualState state : grailNodes) {
            state.setHeartbeatTimestampMs(System.currentTimeMillis());
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
                count++;
            }
        }
        return count;
    }

    /**
     * Build LppGroup objects from actual-states. Groups are keyed by odinInstance.
     * Nodes are classified by grailShardId within each group.
     */
    private Map<String, LppGroup> buildGroups(Map<String, LppNodeActualState> states) {
        Map<String, LppGroup> groups = new LinkedHashMap<>();

        for (LppNodeActualState state : states.values()) {
            String groupId = state.getOdinInstance();
            if (groupId == null || groupId.isBlank()) {
                log.warn("LPP discovery: node {} has no odinInstance, skipping", state.getNodeName());
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
