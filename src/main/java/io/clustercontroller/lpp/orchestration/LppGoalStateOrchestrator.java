package io.clustercontroller.lpp.orchestration;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Pushes goal states (manifests) to LPP nodes via etcd.
 *
 * <p>For each node in the planned allocations, builds an {@link LppNodeGoalState}
 * containing the list of shards that node should serve, then writes it to etcd at
 * /lpp/{env}/nodes/{nodeName}/goal-state.
 *
 * <p>Rolling update: nodes are updated one at a time per invocation to avoid a
 * thundering herd of simultaneous shard downloads. Each call to
 * {@link #orchestrate(Map, Map, String)} processes at most one changed node.
 * Call repeatedly (e.g., from a recurring task) until all nodes converge.
 *
 * <p>Observe-only mode: when {@code observeOnly=true} the goal state is computed
 * but NOT written to etcd. Useful during the initial rollout phase where the
 * controller is just building state.
 */
@Slf4j
public class LppGoalStateOrchestrator {

    private final LppMetadataStore metadataStore;
    private final boolean observeOnly;

    public LppGoalStateOrchestrator(LppMetadataStore metadataStore, boolean observeOnly) {
        this.metadataStore = metadataStore;
        this.observeOnly = observeOnly;
    }

    /**
     * Compute and (optionally) push one node's goal state.
     *
     * @param allocations  current planned allocations from {@link io.clustercontroller.lpp.allocation.LppShardAllocator}
     * @param groups       current group topology
     * @param region       region to set in goal state
     * @return the node name that was updated, or empty if all nodes are already converged
     */
    public Optional<String> orchestrate(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> groups,
            String region) {

        // Build desired goal state per node: nodeName → LppNodeGoalState
        Map<String, LppNodeGoalState> desired = buildDesiredGoalStates(allocations, groups, region);

        for (Map.Entry<String, LppNodeGoalState> entry : desired.entrySet()) {
            String nodeName = entry.getKey();
            LppNodeGoalState desiredState = entry.getValue();

            Optional<LppNodeGoalState> current = metadataStore.getNodeGoalState(nodeName);

            if (current.isPresent() && isSameGoalState(current.get(), desiredState)) {
                log.debug("LPP orchestrator: node {} goal state unchanged, skipping", nodeName);
                continue;
            }

            desiredState.bumpVersion();

            if (observeOnly) {
                log.info("LPP orchestrator [observe-only]: would update node {} → {} shards",
                        nodeName, desiredState.getShards().size());
            } else {
                metadataStore.putNodeGoalState(desiredState);
                log.info("LPP orchestrator: pushed goal state to node {} ({} shards)",
                        nodeName, desiredState.getShards().size());
            }

            // Rolling update: process one node per call
            return Optional.of(nodeName);
        }

        log.debug("LPP orchestrator: all nodes converged");
        return Optional.empty();
    }

    /**
     * Build the full desired goal state map: nodeName → LppNodeGoalState.
     * Each node gets the shards from every allocation that includes it.
     */
    Map<String, LppNodeGoalState> buildDesiredGoalStates(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> groups,
            String region) {

        // nodeName → node's role (derived from its group)
        Map<String, String> nodeRoles = new HashMap<>();
        groups.values().forEach(g -> g.getNodes().forEach(n -> nodeRoles.put(n.getNodeName(), g.getRole())));

        // nodeName → LppNodeGoalState (accumulate shards)
        Map<String, LppNodeGoalState> goalStates = new LinkedHashMap<>();

        for (LppShardPlannedAllocation allocation : allocations.values()) {
            LppShardEntry shardEntry = new LppShardEntry(
                    allocation.getCollection(),
                    allocation.getIndexName(),
                    allocation.getFullIndexName(),
                    allocation.getShardId());

            for (String nodeName : allocation.getAssignedNodeNames()) {
                String role = nodeRoles.getOrDefault(nodeName, "INGEST");
                LppNodeGoalState gs = goalStates.computeIfAbsent(
                        nodeName,
                        n -> new LppNodeGoalState(n, role, region));
                gs.addShard(shardEntry);
            }
        }

        return goalStates;
    }

    /**
     * Two goal states are equivalent if they have the same set of shard keys and
     * the same fullIndexName per shard (i.e., no version change).
     */
    private boolean isSameGoalState(LppNodeGoalState current, LppNodeGoalState desired) {
        if (current.getShards().size() != desired.getShards().size()) return false;

        Map<String, String> currentMap = new HashMap<>();
        current.getShards().forEach(s -> currentMap.put(s.getKey(), s.getFullIndexName()));

        for (LppShardEntry s : desired.getShards()) {
            if (!s.getFullIndexName().equals(currentMap.get(s.getKey()))) return false;
        }
        return true;
    }
}
