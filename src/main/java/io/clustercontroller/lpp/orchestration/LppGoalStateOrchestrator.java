package io.clustercontroller.lpp.orchestration;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Pushes goal states (manifests) to LPP nodes via etcd.
 *
 * <h3>20% rollout policy</h3>
 * <p>When nodes within a group need updating, at most 20% of that group's nodes
 * are updated per orchestration call (minimum 1). This applies independently per
 * group, so multiple groups can make progress simultaneously while each group's
 * shard downloads are staggered to avoid a thundering herd.
 *
 * <p>Example — group with 3 replicas: ceil(20% × 3) = 1 node per tick.
 * Example — group with 10 replicas: ceil(20% × 10) = 2 nodes per tick.
 *
 * <h3>Goal state per node</h3>
 * <p>Each node's goal state is built by inverting the allocation map: for every
 * shard allocated to a group, all nodes in that group receive that shard in their
 * goal state. Nodes within the same group therefore always have identical goal
 * states — they are replicas.
 *
 * <h3>Convergence check</h3>
 * <p>Before pushing, the current etcd goal state is compared against the desired
 * state. A node is only updated if its shard set or any fullIndexName differs.
 *
 * <h3>Observe-only mode</h3>
 * <p>When {@code observeOnly=true} the goal state is computed and logged but NOT
 * written to etcd. Used during initial rollout for dry-run validation.
 */
@Slf4j
public class LppGoalStateOrchestrator {

    private static final double ROLLOUT_FRACTION = 0.20;

    private final LppMetadataStore metadataStore;
    private final boolean observeOnly;

    public LppGoalStateOrchestrator(LppMetadataStore metadataStore, boolean observeOnly) {
        this.metadataStore = metadataStore;
        this.observeOnly = observeOnly;
    }

    /**
     * Compute desired goal states for all nodes and push updates to divergent nodes,
     * respecting the 20%-per-group rollout policy.
     *
     * @param allocations current planned allocations (shardKey → allocation)
     * @param groups      current group topology (groupId → LppGroup)
     * @param region      region label written into goal states
     * @return list of node names updated this tick (empty = fully converged)
     */
    public List<String> orchestrate(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> groups,
            String region) {

        if (allocations.isEmpty()) {
            log.debug("LPP orchestrator: no allocations, nothing to do");
            return List.of();
        }

        Map<String, LppNodeGoalState> desired = buildDesiredGoalStates(allocations, groups, region);

        // Find all divergent nodes grouped by their replica group.
        // We need the groupId per node to apply the per-group rollout cap.
        Map<String, String> nodeToGroup = buildNodeToGroupMap(groups);

        // Collect divergent nodes per group
        Map<String, List<String>> divergentByGroup = new LinkedHashMap<>();

        for (Map.Entry<String, LppNodeGoalState> entry : desired.entrySet()) {
            String nodeName = entry.getKey();
            LppNodeGoalState desiredState = entry.getValue();

            Optional<LppNodeGoalState> current = metadataStore.getNodeGoalState(nodeName);
            if (current.isPresent()
                    && isSameGoalState(current.get(), desiredState)
                    && isActuallyConverged(nodeName, desiredState)) {
                log.debug("LPP orchestrator: node {} converged (goal state + actual state ACTIVE)", nodeName);
                continue;
            }

            String groupId = nodeToGroup.getOrDefault(nodeName, "unknown");
            divergentByGroup.computeIfAbsent(groupId, k -> new ArrayList<>()).add(nodeName);
        }

        if (divergentByGroup.isEmpty()) {
            log.info("LPP orchestrator: all nodes converged");
            return List.of();
        }

        // For each group, push to at most 20% of its divergent nodes
        List<String> updated = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : divergentByGroup.entrySet()) {
            String groupId = entry.getKey();
            List<String> divergentNodes = entry.getValue();

            int groupSize = groups.containsKey(groupId)
                    ? groups.get(groupId).getNodes().size()
                    : divergentNodes.size();

            int rolloutCap = Math.max(1, (int) Math.ceil(groupSize * ROLLOUT_FRACTION));
            List<String> batch = divergentNodes.subList(0, Math.min(rolloutCap, divergentNodes.size()));

            log.info("LPP orchestrator: group {} — {} divergent nodes, rolling out {}/{} (20% cap)",
                    groupId, divergentNodes.size(), batch.size(), groupSize);

            for (String nodeName : batch) {
                LppNodeGoalState desiredState = desired.get(nodeName);
                desiredState.bumpVersion();

                if (observeOnly) {
                    log.info("LPP orchestrator [observe-only]: would push to node {} ({} shards)",
                            nodeName, desiredState.getShards().size());
                } else {
                    metadataStore.putNodeGoalState(desiredState);
                    log.info("LPP orchestrator: pushed goal state to node {} ({} shards, version {})",
                            nodeName, desiredState.getShards().size(), desiredState.getVersion());
                }

                updated.add(nodeName);
            }
        }

        return updated;
    }

    /**
     * Invert the allocation map to produce a per-node goal state.
     *
     * <p>For every shard in every allocation, all nodes in the allocated groups
     * receive that shard. Nodes within the same group end up with identical shard
     * lists (they are replicas).
     */
    Map<String, LppNodeGoalState> buildDesiredGoalStates(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> groups,
            String region) {

        // nodeName → role (from its group). Only live nodes appear here — dead nodes
        // removed from Grail won't be in the group topology, so they are excluded.
        Map<String, String> nodeRoles = new HashMap<>();
        groups.values().forEach(g ->
                g.getNodes().forEach(n -> nodeRoles.put(n.getNodeName(), g.getRole())));

        Map<String, LppNodeGoalState> goalStates = new LinkedHashMap<>();

        for (LppShardPlannedAllocation allocation : allocations.values()) {
            LppShardEntry shardEntry = new LppShardEntry(
                    allocation.getCollection(),
                    allocation.getIndexName(),
                    allocation.getFullIndexName(),
                    allocation.getShardId());

            for (String nodeName : allocation.getAssignedNodeNames()) {
                // Skip nodes not present in the current live topology — they are stale
                // remnants in the PA from when they were alive. Pushing goal-states to them
                // creates orphaned keys since the node no longer has an actual-state.
                if (!nodeRoles.containsKey(nodeName)) {
                    log.debug("LPP orchestrator: skipping dead node {} (not in current group topology)", nodeName);
                    continue;
                }
                String role = nodeRoles.get(nodeName);
                LppNodeGoalState gs = goalStates.computeIfAbsent(
                        nodeName,
                        n -> new LppNodeGoalState(n, role, region));
                gs.addShard(shardEntry);
            }
        }

        return goalStates;
    }

    /**
     * Two goal states are the same if every shard key is present and the
     * fullIndexName matches (catches index version upgrades).
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

    /**
     * Returns true when all desired shards for this node are ACTIVE in the node's actual state.
     *
     * <p>This gates 20% batch progression on real convergence rather than just goal-state
     * written. In shadow mode, {@link LppShadowNodeSimulator} writes fake ACTIVE actual states
     * so the full loop can be exercised without real LP nodes.
     *
     * <p>If the node has no actual state recorded yet, it is not converged.
     */
    private boolean isActuallyConverged(String nodeName, LppNodeGoalState desired) {
        Optional<LppNodeActualState> actualOpt = metadataStore.getNodeActualState(nodeName);
        if (actualOpt.isEmpty()) {
            log.debug("LPP orchestrator: node {} has no actual state — not converged", nodeName);
            return false;
        }

        LppNodeActualState actual = actualOpt.get();
        Set<String> activeShardKeys = actual.getShardStates().stream()
                .filter(LppShardActualState::isActive)
                .map(LppShardActualState::getShardKey)
                .collect(Collectors.toSet());

        for (LppShardEntry shard : desired.getShards()) {
            if (!activeShardKeys.contains(shard.getKey())) {
                log.debug("LPP orchestrator: node {} shard {} not yet ACTIVE", nodeName, shard.getKey());
                return false;
            }
        }
        return true;
    }

    /** Build a reverse map: nodeName → groupId, for rollout cap calculation. */
    private Map<String, String> buildNodeToGroupMap(Map<String, LppGroup> groups) {
        Map<String, String> map = new HashMap<>();
        groups.values().forEach(g ->
                g.getNodes().forEach(n -> map.put(n.getNodeName(), g.getGroupId())));
        return map;
    }
}
