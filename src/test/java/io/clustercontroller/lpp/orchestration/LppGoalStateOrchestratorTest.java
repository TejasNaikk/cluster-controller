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

    // -------------------------------------------------------------------------

    /** Allocation with nodes from a single named group, shard 0 of grocery/local_index. */
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
