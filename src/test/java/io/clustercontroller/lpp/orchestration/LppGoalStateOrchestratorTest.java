package io.clustercontroller.lpp.orchestration;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LppGoalStateOrchestratorTest {

    @Mock private LppMetadataStore metadataStore;

    private LppGoalStateOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        orchestrator = new LppGoalStateOrchestrator(metadataStore, false);
        when(metadataStore.getNodeGoalState(any())).thenReturn(Optional.empty());
        when(metadataStore.getNodeActualState(any())).thenReturn(Optional.empty());
        when(metadataStore.getAllNodeGoalStates()).thenReturn(Map.of());
    }

    // ---- basic push ----

    @Test
    void orchestratePushesGoalStateWhenNodeDiverged() {
        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        List<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        assertThat(updated).contains("node-1");
        verify(metadataStore).putNodeGoalState(any());
    }

    @Test
    void orchestrateSkipsConvergedNode() {
        LppNodeGoalState existing = new LppNodeGoalState("node-1", "INGEST", "local");
        existing.addShard(new LppShardEntry("grocery", "local_index", "local_index.1", 0));
        when(metadataStore.getNodeGoalState("node-1")).thenReturn(Optional.of(existing));

        // Actual state: the shard is ACTIVE — node is fully converged
        LppNodeActualState actual = new LppNodeActualState("node-1", "odin-inst", "sg0", "INGEST", "zone-a");
        LppShardActualState shardActual = new LppShardActualState();
        shardActual.setShardKey("local_index.1/0");
        shardActual.setState("ACTIVE");
        actual.setShardStates(List.of(shardActual));
        when(metadataStore.getNodeActualState("node-1")).thenReturn(Optional.of(actual));

        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        List<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        assertThat(updated).isEmpty();
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    @Test
    void orchestrateReturnsEmptyWhenNoAllocations() {
        List<String> updated = orchestrator.orchestrate(Map.of(), Map.of(), "local");
        assertThat(updated).isEmpty();
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    @Test
    void orchestrateObserveOnlyDoesNotWriteToEtcd() {
        LppGoalStateOrchestrator observeOnly = new LppGoalStateOrchestrator(metadataStore, true);

        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        List<String> updated = observeOnly.orchestrate(allocations, groups, "local");

        assertThat(updated).contains("node-1");
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    // ---- 20% rollout policy ----

    @Test
    void rolloutCapIsOneNodeForSmallGroup() {
        // Group of 3 replicas — 20% = ceil(0.6) = 1 node per tick
        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations(
                "node-1", "node-2", "node-3", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1", "node-2", "node-3"));

        List<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        assertThat(updated).hasSize(1);
        verify(metadataStore, times(1)).putNodeGoalState(any());
    }

    @Test
    void rolloutCapIsTwoNodesForLargerGroup() {
        // Group of 10 replicas — 20% = ceil(2.0) = 2 nodes per tick
        List<String> nodeNames = new ArrayList<>();
        for (int i = 0; i < 10; i++) nodeNames.add("node-" + i);

        Map<String, LppShardPlannedAllocation> allocations =
                multiNodeSingleShardAllocations("g1", nodeNames);
        Map<String, LppGroup> groups = groupsWithNodes("g1", nodeNames);

        List<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        assertThat(updated).hasSize(2);
        verify(metadataStore, times(2)).putNodeGoalState(any());
    }

    @Test
    void multipleGroupsMakeProgressSimultaneously() {
        // Two groups, each with 3 nodes — both groups get 1 node updated per tick
        Map<String, LppShardPlannedAllocation> allocations = new LinkedHashMap<>();
        addAllocation(allocations, "grocery", "idx", "idx.1", 0,
                List.of("g1-node-0", "g1-node-1", "g1-node-2"), "g1");
        addAllocation(allocations, "grocery", "idx", "idx.1", 1,
                List.of("g2-node-0", "g2-node-1", "g2-node-2"), "g2");

        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groups.put("g1", groupsWithNodes("g1", List.of("g1-node-0", "g1-node-1", "g1-node-2")).get("g1"));
        groups.put("g2", groupsWithNodes("g2", List.of("g2-node-0", "g2-node-1", "g2-node-2")).get("g2"));

        List<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        // Both groups contribute 1 node each = 2 total
        assertThat(updated).hasSize(2);
        assertThat(updated.stream().anyMatch(n -> n.startsWith("g1-"))).isTrue();
        assertThat(updated.stream().anyMatch(n -> n.startsWith("g2-"))).isTrue();
    }

    // ---- goal state correctness ----

    @Test
    void pushedGoalStateHasCorrectShards() {
        Map<String, LppShardPlannedAllocation> allocations = new LinkedHashMap<>();
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 0,
                List.of("node-1"), "g1");
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 1,
                List.of("node-1"), "g1");

        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        orchestrator.orchestrate(allocations, groups, "local");

        ArgumentCaptor<LppNodeGoalState> captor = ArgumentCaptor.forClass(LppNodeGoalState.class);
        verify(metadataStore).putNodeGoalState(captor.capture());
        LppNodeGoalState pushed = captor.getValue();

        assertThat(pushed.getNodeName()).isEqualTo("node-1");
        assertThat(pushed.getShards()).hasSize(2);
    }

    @Test
    void pushedGoalStateHasBumpedVersion() {
        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        orchestrator.orchestrate(allocations, groups, "local");

        ArgumentCaptor<LppNodeGoalState> captor = ArgumentCaptor.forClass(LppNodeGoalState.class);
        verify(metadataStore).putNodeGoalState(captor.capture());
        assertThat(captor.getValue().getVersion()).isEqualTo(1);
    }

    @Test
    void buildDesiredGoalStatesMapsMultipleNodesToSameShards() {
        // Both nodes are in the same group so they should have identical goal states
        Map<String, LppShardPlannedAllocation> allocations = new LinkedHashMap<>();
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 0,
                List.of("node-1", "node-2"), "g1");

        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1", "node-2"));

        Map<String, LppNodeGoalState> desired =
                orchestrator.buildDesiredGoalStates(allocations, groups, "local");

        assertThat(desired).containsKeys("node-1", "node-2");
        assertThat(desired.get("node-1").getShards()).hasSize(1);
        assertThat(desired.get("node-2").getShards()).hasSize(1);
        // Same shard key on both
        assertThat(desired.get("node-1").getShards().get(0).getKey())
                .isEqualTo(desired.get("node-2").getShards().get(0).getKey());
    }

    @Test
    void buildDesiredGoalStatesSkipsDeadNodesNotInGroupTopology() {
        // PA references node-dead (stale, removed from Grail) + node-live (healthy).
        // Group topology only contains node-live. node-dead should be skipped.
        Map<String, LppShardPlannedAllocation> allocations = new LinkedHashMap<>();
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 0,
                List.of("node-live", "node-dead"), "g1");

        // Group topology only has node-live — node-dead was pruned from Grail
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-live"));

        Map<String, LppNodeGoalState> desired =
                orchestrator.buildDesiredGoalStates(allocations, groups, "local");

        assertThat(desired).containsKey("node-live");
        assertThat(desired).doesNotContainKey("node-dead");
    }

    @Test
    void orchestrateDoesNotPushGoalStateToDeadNode() {
        // PA has node-dead which is not in the group topology
        Map<String, LppShardPlannedAllocation> allocations = new LinkedHashMap<>();
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 0,
                List.of("node-dead"), "g1");

        // Empty groups — node-dead not discovered
        Map<String, LppGroup> groups = Map.of();

        List<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        assertThat(updated).isEmpty();
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    // ---- dead node cleanup ----

    @Test
    void orchestrateDeletesGoalStateForDeadNode() {
        // node-dead has a goal state in etcd but is no longer in any live group topology
        LppNodeGoalState deadGs = new LppNodeGoalState("node-dead", "INGEST", "local");
        when(metadataStore.getAllNodeGoalStates()).thenReturn(Map.of("node-dead", deadGs));

        // Live topology has only node-live
        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-live", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-live"));

        orchestrator.orchestrate(allocations, groups, "local");

        verify(metadataStore).deleteNodeGoalState("node-dead");
    }

    @Test
    void orchestrateDoesNotDeleteGoalStateForLiveNode() {
        LppNodeGoalState liveGs = new LppNodeGoalState("node-1", "INGEST", "local");
        when(metadataStore.getAllNodeGoalStates()).thenReturn(Map.of("node-1", liveGs));

        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        orchestrator.orchestrate(allocations, groups, "local");

        verify(metadataStore, never()).deleteNodeGoalState("node-1");
    }

    @Test
    void observeOnlyDoesNotDeleteGoalStateForDeadNode() {
        LppGoalStateOrchestrator observeOnly = new LppGoalStateOrchestrator(metadataStore, true);
        LppNodeGoalState deadGs = new LppNodeGoalState("node-dead", "INGEST", "local");
        when(metadataStore.getAllNodeGoalStates()).thenReturn(Map.of("node-dead", deadGs));

        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-live", "g1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-live"));

        observeOnly.orchestrate(allocations, groups, "local");

        verify(metadataStore, never()).deleteNodeGoalState(any());
    }

    // ---- searcher dependency gate ----

    @Test
    void searcherNodeDeferredWhenIngesterPeerHasNoActualState() {
        // Setup: ingester-1 and searcher-1 in separate role groups
        Map<String, LppGroup> ingestGroups = ingesterGroupsWithNodes("g1", List.of("node1-ingester"));
        Map<String, LppGroup> searchGroups = searcherGroupsWithNodes("g1", List.of("node1-searcher"));
        Map<String, LppShardPlannedAllocation> allocations = roleAwareAllocations(
                List.of("node1-ingester"), List.of("node1-searcher"), "g1");

        // Ingester peer has no actual state yet
        when(metadataStore.getNodeActualState("node1-ingester")).thenReturn(Optional.empty());

        List<String> updated = orchestrator.orchestrate(allocations, ingestGroups, searchGroups, "local");

        // Ingester should get its goal state, searcher deferred
        assertThat(updated).contains("node1-ingester");
        assertThat(updated).doesNotContain("node1-searcher");
    }

    @Test
    void searcherNodeDeferredWhenIngesterPeerShardNotActive() {
        Map<String, LppGroup> ingestGroups = ingesterGroupsWithNodes("g1", List.of("node1-ingester"));
        Map<String, LppGroup> searchGroups = searcherGroupsWithNodes("g1", List.of("node1-searcher"));
        Map<String, LppShardPlannedAllocation> allocations = roleAwareAllocations(
                List.of("node1-ingester"), List.of("node1-searcher"), "g1");

        // Ingester has actual state but shard is DOWNLOADING, not ACTIVE
        LppNodeActualState ingesterActual = actualStateWith("node1-ingester", "local_index.1/0", "DOWNLOADING");
        when(metadataStore.getNodeActualState("node1-ingester")).thenReturn(Optional.of(ingesterActual));

        List<String> updated = orchestrator.orchestrate(allocations, ingestGroups, searchGroups, "local");

        assertThat(updated).doesNotContain("node1-searcher");
    }

    @Test
    void searcherNodeProceedsWhenIngesterPeerShardIsActive() {
        Map<String, LppGroup> ingestGroups = ingesterGroupsWithNodes("g1", List.of("node1-ingester"));
        Map<String, LppGroup> searchGroups = searcherGroupsWithNodes("g1", List.of("node1-searcher"));
        Map<String, LppShardPlannedAllocation> allocations = roleAwareAllocations(
                List.of("node1-ingester"), List.of("node1-searcher"), "g1");

        // Ingester is fully converged — mark it as already having the goal state + ACTIVE
        LppNodeGoalState ingesterGs = new LppNodeGoalState("node1-ingester", "ingester", "local");
        ingesterGs.addShard(new LppShardEntry("grocery", "local_index", "local_index.1", 0));
        when(metadataStore.getNodeGoalState("node1-ingester")).thenReturn(Optional.of(ingesterGs));
        LppNodeActualState ingesterActual = actualStateWith("node1-ingester", "local_index.1/0", "ACTIVE");
        when(metadataStore.getNodeActualState("node1-ingester")).thenReturn(Optional.of(ingesterActual));

        List<String> updated = orchestrator.orchestrate(allocations, ingestGroups, searchGroups, "local");

        // Searcher should be pushed now that ingester is ACTIVE
        assertThat(updated).contains("node1-searcher");
    }

    @Test
    void ingesterNodesProcessNormallyWithoutDependencyCheck() {
        Map<String, LppGroup> ingestGroups = ingesterGroupsWithNodes("g1", List.of("node1-ingester", "node2-ingester", "node3-ingester"));
        Map<String, LppShardPlannedAllocation> allocations = roleAwareIngestOnlyAllocations(
                List.of("node1-ingester", "node2-ingester", "node3-ingester"), "g1");

        // No actual state needed for ingesters — they proceed freely
        List<String> updated = orchestrator.orchestrate(allocations, ingestGroups, Map.of(), "local");

        // 20% of 3 = ceil(0.6) = 1 ingester updated (no searcher dependency check)
        assertThat(updated).hasSize(1);
    }

    @Test
    void roleAwareDesiredGoalStatesAssignCorrectRoles() {
        Map<String, LppGroup> ingestGroups = ingesterGroupsWithNodes("g1", List.of("node1-ingester"));
        Map<String, LppGroup> searchGroups = searcherGroupsWithNodes("g1", List.of("node1-searcher"));
        Map<String, LppShardPlannedAllocation> allocations = roleAwareAllocations(
                List.of("node1-ingester"), List.of("node1-searcher"), "g1");

        Map<String, LppNodeGoalState> desired =
                orchestrator.buildDesiredGoalStates(allocations, ingestGroups, searchGroups, "local");

        assertThat(desired.get("node1-ingester").getRole()).isEqualTo("ingester");
        assertThat(desired.get("node1-searcher").getRole()).isEqualTo("searcher");
    }

    // ---- orphan cleanup when allocations are empty ----

    @Test
    void orphanGoalStateCleanedUpEvenWhenAllocationsAreEmpty() {
        // Previously, an early return prevented cleanup when allocations were empty.
        // This verifies that stale goal states are deleted even after all indices are removed.
        LppNodeGoalState staleGs = new LppNodeGoalState("dead-node", "ingester", "local");
        when(metadataStore.getAllNodeGoalStates()).thenReturn(Map.of("dead-node", staleGs));

        // No live nodes, no allocations
        orchestrator.orchestrate(Map.of(), Map.of(), "local");

        verify(metadataStore).deleteNodeGoalState("dead-node");
    }

    @Test
    void emptyAllocationsWithLiveNodesProducesNoGoalStatePushes() {
        when(metadataStore.getAllNodeGoalStates()).thenReturn(Map.of());

        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node1"));
        List<String> updated = orchestrator.orchestrate(Map.of(), groups, "local");

        assertThat(updated).isEmpty();
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    // ---- helper builders ----

    private Map<String, LppGroup> ingesterGroupsWithNodes(String groupId, List<String> nodeNames) {
        LppGroup group = new LppGroup(groupId, "zone-a", "ingester");
        for (String name : nodeNames) {
            LppNode node = new LppNode(name, "odin-instance", groupId, "ingester", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return Map.of(groupId, group);
    }

    private Map<String, LppGroup> searcherGroupsWithNodes(String groupId, List<String> nodeNames) {
        LppGroup group = new LppGroup(groupId, "zone-a", "searcher");
        for (String name : nodeNames) {
            LppNode node = new LppNode(name, "odin-instance", groupId, "searcher", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return Map.of(groupId, group);
    }

    /** PA with separate ingester and searcher node lists. */
    private Map<String, LppShardPlannedAllocation> roleAwareAllocations(
            List<String> ingesterNodes, List<String> searcherNodes, String groupId) {
        LppShardEntry entry = new LppShardEntry("grocery", "local_index", "local_index.1", 0);
        LppShardPlannedAllocation alloc = new LppShardPlannedAllocation(entry);
        alloc.getAssignedIngesterNodeNames().addAll(ingesterNodes);
        alloc.getAssignedSearcherNodeNames().addAll(searcherNodes);
        alloc.getAssignedNodeNames().addAll(ingesterNodes);
        alloc.getAssignedNodeNames().addAll(searcherNodes);
        alloc.getAssignedGroupIds().add(groupId);
        return Map.of(entry.getKey(), alloc);
    }

    private Map<String, LppShardPlannedAllocation> roleAwareIngestOnlyAllocations(
            List<String> ingesterNodes, String groupId) {
        LppShardEntry entry = new LppShardEntry("grocery", "local_index", "local_index.1", 0);
        LppShardPlannedAllocation alloc = new LppShardPlannedAllocation(entry);
        alloc.getAssignedIngesterNodeNames().addAll(ingesterNodes);
        alloc.getAssignedNodeNames().addAll(ingesterNodes);
        alloc.getAssignedGroupIds().add(groupId);
        return Map.of(entry.getKey(), alloc);
    }

    private LppNodeActualState actualStateWith(String nodeName, String shardKey, String state) {
        LppNodeActualState actual = new LppNodeActualState();
        actual.setNodeName(nodeName);
        LppShardActualState shardActual = new LppShardActualState();
        shardActual.setShardKey(shardKey);
        shardActual.setState(state);
        actual.setShardStates(List.of(shardActual));
        return actual;
    }

    private Map<String, LppShardPlannedAllocation> singleShardAllocations(String nodeName, String groupId) {
        return singleShardAllocations(new String[]{nodeName}, groupId);
    }

    private Map<String, LppShardPlannedAllocation> singleShardAllocations(
            String node1, String node2, String node3, String groupId) {
        return singleShardAllocations(new String[]{node1, node2, node3}, groupId);
    }

    private Map<String, LppShardPlannedAllocation> singleShardAllocations(
            String[] nodeNames, String groupId) {
        Map<String, LppShardPlannedAllocation> m = new LinkedHashMap<>();
        addAllocation(m, "grocery", "local_index", "local_index.1", 0, List.of(nodeNames), groupId);
        return m;
    }

    private Map<String, LppShardPlannedAllocation> multiNodeSingleShardAllocations(
            String groupId, List<String> nodeNames) {
        Map<String, LppShardPlannedAllocation> m = new LinkedHashMap<>();
        addAllocation(m, "grocery", "local_index", "local_index.1", 0, nodeNames, groupId);
        return m;
    }

    private void addAllocation(Map<String, LppShardPlannedAllocation> map,
                                String collection, String indexName, String fullIndex,
                                int shardId, List<String> nodeNames, String groupId) {
        LppShardEntry entry = new LppShardEntry(collection, indexName, fullIndex, shardId);
        LppShardPlannedAllocation alloc = new LppShardPlannedAllocation(entry);
        alloc.getAssignedNodeNames().addAll(nodeNames);
        alloc.getAssignedGroupIds().add(groupId);
        map.put(entry.getKey(), alloc);
    }

    private Map<String, LppGroup> groupsWithNodes(String groupId, List<String> nodeNames) {
        LppGroup group = new LppGroup(groupId, "zone-a", "INGEST");
        for (String name : nodeNames) {
            LppNode node = new LppNode(name, "odin-instance", groupId, "INGEST", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return Map.of(groupId, group);
    }
}
