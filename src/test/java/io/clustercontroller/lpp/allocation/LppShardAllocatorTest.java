package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
class LppShardAllocatorTest {

    @Mock private LppMetadataStore metadataStore;

    private LppShardAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new LppShardAllocator(metadataStore);
        // No existing allocations by default
        when(metadataStore.getShardPlannedAllocation(any())).thenReturn(Optional.empty());
    }

    @Test
    void allocatesAllShardsToSingleGroup() {
        LppGroup group = healthyGroup("g1", "INGEST", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 3, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        assertThat(result).hasSize(3);
        assertThat(result).containsKeys("grocery.local_index.0", "grocery.local_index.1", "grocery.local_index.2");
        result.values().forEach(a -> assertThat(a.getAssignedGroupIds()).containsExactly("g1"));
    }

    @Test
    void allocatesToMultipleGroupsWhenRequested() {
        LppGroup g1 = healthyGroup("g1", "INGEST", 2);
        LppGroup g2 = healthyGroup("g2", "INGEST", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 2, 2, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", g1, "g2", g2), List.of(index));

        assertThat(result).hasSize(2);
        result.values().forEach(a -> assertThat(a.getAssignedGroupIds()).containsExactlyInAnyOrder("g1", "g2"));
    }

    @Test
    void skipsUnhealthyGroups() {
        LppGroup healthy = healthyGroup("g-healthy", "INGEST", 2);
        LppGroup drained = healthyGroup("g-drained", "INGEST", 2);
        drained.getNodes().forEach(n -> n.setAdminState("DRAIN"));

        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 1, 2, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g-healthy", healthy, "g-drained", drained), List.of(index));

        assertThat(result.get("grocery.local_index.0").getAssignedGroupIds())
                .containsExactly("g-healthy")
                .doesNotContain("g-drained");
    }

    @Test
    void skipsGroupsWithWrongRole() {
        LppGroup ingestGroup = healthyGroup("g-ingest", "INGEST", 2);
        LppGroup searchGroup = healthyGroup("g-search", "SEARCH", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 1, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g-ingest", ingestGroup, "g-search", searchGroup), List.of(index));

        assertThat(result.get("grocery.local_index.0").getAssignedGroupIds())
                .containsExactly("g-ingest");
    }

    @Test
    void returnsEmptyWhenNoEligibleGroups() {
        LppGroup drained = healthyGroup("g1", "INGEST", 2);
        drained.getNodes().forEach(n -> n.setAdminState("DRAIN"));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 2, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", drained), List.of(index));

        assertThat(result).isEmpty();
    }

    @Test
    void keepsStableAllocationWhenGroupsUnchanged() {
        LppGroup group = healthyGroup("g1", "INGEST", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 1, 1, "INGEST");

        LppShardEntry entry = new LppShardEntry("grocery", "local_index", "local_index.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(group);

        when(metadataStore.getShardPlannedAllocation("grocery.local_index.0"))
                .thenReturn(Optional.of(existing));

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        // Should not write new allocation since it's stable
        verify(metadataStore, never()).putShardPlannedAllocation(any());
        assertThat(result).containsKey("grocery.local_index.0");
    }

    @Test
    void writesNewAllocationWhenGroupsChange() {
        LppGroup g1 = healthyGroup("g1", "INGEST", 2);
        LppGroup g2 = healthyGroup("g2", "INGEST", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 1, 2, "INGEST");

        // Existing allocation only has g1, but now 2 groups desired
        LppShardEntry entry = new LppShardEntry("grocery", "local_index", "local_index.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(g1);

        when(metadataStore.getShardPlannedAllocation("grocery.local_index.0"))
                .thenReturn(Optional.of(existing));

        allocator.allocate(Map.of("g1", g1, "g2", g2), List.of(index));

        verify(metadataStore).putShardPlannedAllocation(any());
    }

    @Test
    void allNodesFromGroupAreInAllocation() {
        LppGroup group = healthyGroup("g1", "INGEST", 3);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "local_index", "local_index.1", 1, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        LppShardPlannedAllocation alloc = result.get("grocery.local_index.0");
        assertThat(alloc.getAssignedNodeNames()).hasSize(3);
    }

    // -------------------------------------------------------------------------

    private LppGroup healthyGroup(String groupId, String role, int numNodes) {
        LppGroup group = new LppGroup(groupId, "zone-a", role);
        for (int i = 0; i < numNodes; i++) {
            LppNode node = new LppNode("node-" + groupId + "-" + i, groupId, "shard-0", role, "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return group;
    }
}
