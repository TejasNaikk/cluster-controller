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

    @Test
    void orchestratePushesGoalStateToFirstDivergentNode() {
        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        Optional<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        assertThat(updated).isPresent().hasValue("node-1");
        verify(metadataStore).putNodeGoalState(any());
    }

    @Test
    void orchestrateSkipsConvergedNode() {
        LppNodeGoalState existing = new LppNodeGoalState("node-1", "INGEST", "local");
        existing.addShard(new LppShardEntry("grocery", "local_index", "local_index.1", 0));
        when(metadataStore.getNodeGoalState("node-1")).thenReturn(Optional.of(existing));

        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        Optional<String> updated = orchestrator.orchestrate(allocations, groups, "local");

        assertThat(updated).isEmpty();
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    @Test
    void orchestrateReturnsEmptyWhenNoAllocations() {
        Optional<String> updated = orchestrator.orchestrate(Map.of(), Map.of(), "local");
        assertThat(updated).isEmpty();
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    @Test
    void orchestrateObserveOnlyDoesNotWriteToEtcd() {
        LppGoalStateOrchestrator observeOrchestrator =
                new LppGoalStateOrchestrator(metadataStore, true);

        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        Optional<String> updated = observeOrchestrator.orchestrate(allocations, groups, "local");

        // Node is returned as "would be updated" but nothing written
        assertThat(updated).isPresent();
        verify(metadataStore, never()).putNodeGoalState(any());
    }

    @Test
    void orchestrateSetsCorrectShardsOnGoalState() {
        Map<String, LppShardPlannedAllocation> allocations = new LinkedHashMap<>();
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 0, List.of("node-1"));
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 1, List.of("node-1"));

        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        orchestrator.orchestrate(allocations, groups, "local");

        ArgumentCaptor<LppNodeGoalState> captor = ArgumentCaptor.forClass(LppNodeGoalState.class);
        verify(metadataStore).putNodeGoalState(captor.capture());
        LppNodeGoalState pushed = captor.getValue();

        assertThat(pushed.getNodeName()).isEqualTo("node-1");
        assertThat(pushed.getShards()).hasSize(2);
    }

    @Test
    void orchestrateBumpsVersionOnPush() {
        Map<String, LppShardPlannedAllocation> allocations = singleShardAllocations("node-1");
        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1"));

        orchestrator.orchestrate(allocations, groups, "local");

        ArgumentCaptor<LppNodeGoalState> captor = ArgumentCaptor.forClass(LppNodeGoalState.class);
        verify(metadataStore).putNodeGoalState(captor.capture());
        assertThat(captor.getValue().getVersion()).isEqualTo(1);
    }

    @Test
    void buildDesiredGoalStatesMapsMultipleNodesToCorrectShards() {
        Map<String, LppShardPlannedAllocation> allocations = new LinkedHashMap<>();
        addAllocation(allocations, "grocery", "local_index", "local_index.1", 0, List.of("node-1", "node-2"));

        Map<String, LppGroup> groups = groupsWithNodes("g1", List.of("node-1", "node-2"));

        Map<String, LppNodeGoalState> desired =
                orchestrator.buildDesiredGoalStates(allocations, groups, "local");

        assertThat(desired).containsKeys("node-1", "node-2");
        assertThat(desired.get("node-1").getShards()).hasSize(1);
        assertThat(desired.get("node-2").getShards()).hasSize(1);
    }

    // -------------------------------------------------------------------------

    private Map<String, LppShardPlannedAllocation> singleShardAllocations(String... nodeNames) {
        Map<String, LppShardPlannedAllocation> m = new LinkedHashMap<>();
        addAllocation(m, "grocery", "local_index", "local_index.1", 0, List.of(nodeNames));
        return m;
    }

    private void addAllocation(Map<String, LppShardPlannedAllocation> map,
                                String collection, String indexName, String fullIndex,
                                int shardId, List<String> nodeNames) {
        LppShardEntry entry = new LppShardEntry(collection, indexName, fullIndex, shardId);
        LppShardPlannedAllocation alloc = new LppShardPlannedAllocation(entry);
        alloc.getAssignedNodeNames().addAll(nodeNames);
        map.put(entry.getKey(), alloc);
    }

    private Map<String, LppGroup> groupsWithNodes(String groupId, List<String> nodeNames) {
        LppGroup group = new LppGroup(groupId, "zone-a", "INGEST");
        for (String name : nodeNames) {
            LppNode node = new LppNode(name, groupId, "shard-0", "INGEST", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return Map.of(groupId, group);
    }
}
