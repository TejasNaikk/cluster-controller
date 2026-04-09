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
     * <p>Ingesters roll out freely at 20% per group. Searchers roll out at 20% per group
     * but only after their ingester peer has the shard ACTIVE in its actual state.
     *
     * @param allocations  current planned allocations (shardKey → allocation)
     * @param ingestGroups current ingester group topology (groupId → LppGroup)
     * @param searchGroups current searcher group topology (groupId → LppGroup)
     * @param region       region label written into goal states
     * @return list of node names updated this tick (empty = fully converged)
     */
    public List<String> orchestrate(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> ingestGroups,
            Map<String, LppGroup> searchGroups,
            String region) {

        if (allocations.isEmpty()) {
            log.debug("LPP orchestrator: no allocations, nothing to do");
            return List.of();
        }

        // Combined view of all live nodes for orphan detection and rollout cap
        Map<String, LppGroup> allGroups = new LinkedHashMap<>(ingestGroups);
        allGroups.putAll(searchGroups);

        Map<String, LppNodeGoalState> desired = buildDesiredGoalStates(allocations, ingestGroups, searchGroups, region);

        // Clean up orphaned goal states for nodes no longer in any live group
        Map<String, LppNodeGoalState> allExistingGoalStates = metadataStore.getAllNodeGoalStates();
        for (String nodeName : allExistingGoalStates.keySet()) {
            if (!desired.containsKey(nodeName)) {
                if (observeOnly) {
                    log.info("LPP orchestrator [observe-only]: would delete orphaned goal state for dead node {}", nodeName);
                } else {
                    log.info("LPP orchestrator: deleting orphaned goal state for dead node {}", nodeName);
                    metadataStore.deleteNodeGoalState(nodeName);
                }
            }
        }

        // Build node → groupId reverse map across both pools
        Map<String, String> nodeToGroup = buildNodeToGroupMap(allGroups);
        // Build node → role map
        Map<String, String> nodeRoles = buildNodeRoleMap(ingestGroups, searchGroups);

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

        List<String> updated = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : divergentByGroup.entrySet()) {
            String groupId = entry.getKey();
            List<String> divergentNodes = entry.getValue();

            int groupSize = allGroups.containsKey(groupId)
                    ? allGroups.get(groupId).getNodes().size()
                    : divergentNodes.size();

            int rolloutCap = Math.max(1, (int) Math.ceil(groupSize * ROLLOUT_FRACTION));
            List<String> batch = divergentNodes.subList(0, Math.min(rolloutCap, divergentNodes.size()));

            log.info("LPP orchestrator: group {} — {} divergent nodes, rolling out {}/{} (20% cap)",
                    groupId, divergentNodes.size(), batch.size(), groupSize);

            for (String nodeName : batch) {
                String role = nodeRoles.getOrDefault(nodeName, "");

                // Searcher dependency gate: the ingester peer must have all assigned shards ACTIVE
                if ("searcher".equalsIgnoreCase(role)) {
                    LppNodeGoalState desiredState = desired.get(nodeName);
                    String ingesterPeer = nodeName.replace("-searcher", "-ingester");
                    if (!isIngesterPeerReady(ingesterPeer, desiredState.getShards())) {
                        log.info("LPP orchestrator: deferring searcher {} — ingester peer {} not yet ACTIVE for all assigned shards",
                                nodeName, ingesterPeer);
                        continue;
                    }
                }

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

    // Legacy overload — kept for backward compatibility. Treats all nodes as ingesters.
    @Deprecated
    public List<String> orchestrate(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> groups,
            String region) {
        return orchestrate(allocations, groups, Map.of(), region);
    }

    /**
     * Invert the allocation map to produce a per-node goal state.
     *
     * <p>Uses {@code assignedIngesterNodeNames} and {@code assignedSearcherNodeNames} from each
     * allocation to build role-correct goal states. Falls back to {@code assignedNodeNames} for
     * PAs that pre-date the role split.
     */
    Map<String, LppNodeGoalState> buildDesiredGoalStates(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> ingestGroups,
            Map<String, LppGroup> searchGroups,
            String region) {

        Map<String, String> nodeRoles = buildNodeRoleMap(ingestGroups, searchGroups);

        Map<String, LppNodeGoalState> goalStates = new LinkedHashMap<>();

        for (LppShardPlannedAllocation allocation : allocations.values()) {
            LppShardEntry shardEntry = new LppShardEntry(
                    allocation.getCollection(),
                    allocation.getIndexName(),
                    allocation.getFullIndexName(),
                    allocation.getShardId());

            // Use role-specific node lists; fall back to combined list for legacy PAs
            List<String> ingesterNodes = allocation.getAssignedIngesterNodeNames().isEmpty()
                    ? allocation.getAssignedNodeNames()
                    : allocation.getAssignedIngesterNodeNames();
            List<String> searcherNodes = allocation.getAssignedSearcherNodeNames();

            for (String nodeName : ingesterNodes) {
                if (!nodeRoles.containsKey(nodeName)) {
                    log.debug("LPP orchestrator: skipping dead ingester node {} (not in current topology)", nodeName);
                    continue;
                }
                goalStates.computeIfAbsent(nodeName, n -> new LppNodeGoalState(n, "ingester", region))
                        .addShard(shardEntry);
            }

            for (String nodeName : searcherNodes) {
                if (!nodeRoles.containsKey(nodeName)) {
                    log.debug("LPP orchestrator: skipping dead searcher node {} (not in current topology)", nodeName);
                    continue;
                }
                goalStates.computeIfAbsent(nodeName, n -> new LppNodeGoalState(n, "searcher", region))
                        .addShard(shardEntry);
            }
        }

        return goalStates;
    }

    // Legacy overload kept for tests. Treats all nodes as ingesters (no searcher dependency).
    Map<String, LppNodeGoalState> buildDesiredGoalStates(
            Map<String, LppShardPlannedAllocation> allocations,
            Map<String, LppGroup> groups,
            String region) {
        return buildDesiredGoalStates(allocations, groups, Map.of(), region);
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

    /** Build a reverse map: nodeName → role ("ingester" or "searcher"). */
    private Map<String, String> buildNodeRoleMap(
            Map<String, LppGroup> ingestGroups,
            Map<String, LppGroup> searchGroups) {
        Map<String, String> map = new HashMap<>();
        ingestGroups.values().forEach(g ->
                g.getNodes().forEach(n -> map.put(n.getNodeName(), "ingester")));
        searchGroups.values().forEach(g ->
                g.getNodes().forEach(n -> map.put(n.getNodeName(), "searcher")));
        return map;
    }

    /**
     * Returns true when the ingester peer node has ALL the given shards ACTIVE in its
     * actual state. Used to gate searcher goal-state pushes.
     */
    private boolean isIngesterPeerReady(String ingesterNode, List<LppShardEntry> requiredShards) {
        Optional<LppNodeActualState> actualOpt = metadataStore.getNodeActualState(ingesterNode);
        if (actualOpt.isEmpty()) {
            log.debug("LPP orchestrator: ingester peer {} has no actual state", ingesterNode);
            return false;
        }

        Set<String> activeShardKeys = actualOpt.get().getShardStates().stream()
                .filter(LppShardActualState::isActive)
                .map(LppShardActualState::getShardKey)
                .collect(Collectors.toSet());

        for (LppShardEntry shard : requiredShards) {
            if (!activeShardKeys.contains(shard.getKey())) {
                log.debug("LPP orchestrator: ingester peer {} shard {} not yet ACTIVE", ingesterNode, shard.getKey());
                return false;
            }
        }
        return true;
    }
}
