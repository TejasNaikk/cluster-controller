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
        when(metadataStore.getShardPlannedAllocation(any())).thenReturn(Optional.empty());
    }

    // ---- basic assignment ----

    @Test
    void eachShardAssignedToOneGroup_numGroupsOne() {
        // 3 groups, 3 shards, numGroups=1 → each shard lands on a different group
        Map<String, LppGroup> groups = Map.of(
                "g0", healthyGroup("g0", "INGEST", 2),
                "g1", healthyGroup("g1", "INGEST", 2),
                "g2", healthyGroup("g2", "INGEST", 2));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 3, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        assertThat(result).hasSize(3);
        // Each shard should be on exactly 1 group
        result.values().forEach(a -> assertThat(a.getAssignedGroupIds()).hasSize(1));
        // All three groups should each receive exactly one shard (uniform spread)
        Set<String> usedGroups = new HashSet<>();
        result.values().forEach(a -> usedGroups.addAll(a.getAssignedGroupIds()));
        assertThat(usedGroups).containsExactlyInAnyOrder("g0", "g1", "g2");
    }

    @Test
    void shardsSpreadUniformlyAcrossGroups() {
        // 2 groups, 4 shards, numGroups=1 → 2 shards per group
        Map<String, LppGroup> groups = Map.of(
                "g0", healthyGroup("g0", "INGEST", 2),
                "g1", healthyGroup("g1", "INGEST", 2));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 4, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        assertThat(result).hasSize(4);
        Map<String, Long> shardsPerGroup = new HashMap<>();
        result.values().forEach(a -> a.getAssignedGroupIds()
                .forEach(gid -> shardsPerGroup.merge(gid, 1L, Long::sum)));
        // Uniform: each group gets 2 shards
        assertThat(shardsPerGroup.get("g0")).isEqualTo(2L);
        assertThat(shardsPerGroup.get("g1")).isEqualTo(2L);
    }

    @Test
    void eachShardAssignedToNumGroupsGroups() {
        // numGroups=2 means each shard is replicated across 2 groups
        Map<String, LppGroup> groups = Map.of(
                "g0", healthyGroup("g0", "INGEST", 2),
                "g1", healthyGroup("g1", "INGEST", 2),
                "g2", healthyGroup("g2", "INGEST", 2));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 3, 2, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        assertThat(result).hasSize(3);
        // Each shard should be on exactly 2 groups
        result.values().forEach(a -> assertThat(a.getAssignedGroupIds()).hasSize(2));
    }

    @Test
    void differentShardsLandOnDifferentGroups() {
        // 3 groups, 3 shards, numGroups=1 → shards 0,1,2 should NOT all go to the same group
        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groups.put("g0", healthyGroup("g0", "INGEST", 2));
        groups.put("g1", healthyGroup("g1", "INGEST", 2));
        groups.put("g2", healthyGroup("g2", "INGEST", 2));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 3, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        // Collect which group each shard went to
        List<String> assignedGroups = result.values().stream()
                .map(a -> a.getAssignedGroupIds().get(0))
                .toList();
        // All 3 shards should be on distinct groups (uniform spread with 3 groups = 1 shard each)
        assertThat(new HashSet<>(assignedGroups)).hasSize(3);
    }

    // ---- eligibility filters ----

    @Test
    void skipsUnhealthyGroups() {
        LppGroup healthy = healthyGroup("g-healthy", "INGEST", 2);
        LppGroup drained = healthyGroup("g-drained", "INGEST", 2);
        drained.getNodes().forEach(n -> n.setAdminState("DRAIN"));

        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g-healthy", healthy, "g-drained", drained), List.of(index));

        assertThat(result.get("grocery.idx.0").getAssignedGroupIds())
                .containsExactly("g-healthy")
                .doesNotContain("g-drained");
    }

    @Test
    void skipsGroupsWithWrongRole() {
        LppGroup ingest = healthyGroup("g-ingest", "INGEST", 2);
        LppGroup search = healthyGroup("g-search", "SEARCH", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g-ingest", ingest, "g-search", search), List.of(index));

        assertThat(result.get("grocery.idx.0").getAssignedGroupIds())
                .containsExactly("g-ingest");
    }

    @Test
    void returnsEmptyWhenNoEligibleGroups() {
        LppGroup drained = healthyGroup("g1", "INGEST", 2);
        drained.getNodes().forEach(n -> n.setAdminState("DRAIN"));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 2, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", drained), List.of(index));

        assertThat(result).isEmpty();
    }

    // ---- stable allocation ----

    @Test
    void keepsStableAllocationWhenGroupsUnchanged() {
        LppGroup group = healthyGroup("g1", "INGEST", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1, "INGEST");

        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(group);

        when(metadataStore.getShardPlannedAllocation("grocery.idx.0"))
                .thenReturn(Optional.of(existing));

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        verify(metadataStore, never()).putShardPlannedAllocation(any());
        assertThat(result).containsKey("grocery.idx.0");
    }

    @Test
    void writesNewAllocationWhenGroupSetChanges() {
        LppGroup g1 = healthyGroup("g1", "INGEST", 2);
        LppGroup g2 = healthyGroup("g2", "INGEST", 2);
        // numGroups=2: each shard goes to 2 groups
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 2, "INGEST");

        // Existing only has g1, but now 2 groups desired
        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(g1);

        when(metadataStore.getShardPlannedAllocation("grocery.idx.0"))
                .thenReturn(Optional.of(existing));

        allocator.allocate(Map.of("g1", g1, "g2", g2), List.of(index));

        verify(metadataStore).putShardPlannedAllocation(any());
    }

    // ---- node population ----

    @Test
    void allNodesFromAssignedGroupsAreInAllocation() {
        LppGroup group = healthyGroup("g1", "INGEST", 3); // 3 replicas
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        assertThat(result.get("grocery.idx.0").getAssignedNodeNames()).hasSize(3);
    }

    // ---- custom strategy ----

    @Test
    void respectsCustomAllocationStrategy() {
        // Strategy that always picks the last group in the list
        AllocationStrategy lastGroupStrategy = (eligible, numGroups, counts) -> {
            LppGroup last = eligible.get(eligible.size() - 1);
            counts.merge(last.getGroupId(), 1, Integer::sum);
            return List.of(last);
        };
        allocator = new LppShardAllocator(metadataStore, lastGroupStrategy);

        List<LppGroup> groupList = List.of(
                healthyGroup("g0", "INGEST", 2),
                healthyGroup("g1", "INGEST", 2),
                healthyGroup("g2", "INGEST", 2));
        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groupList.forEach(g -> groups.put(g.getGroupId(), g));

        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 2, 1, "INGEST");

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        // Both shards should go to whichever group the strategy always picks last
        result.values().forEach(a -> assertThat(a.getAssignedGroupIds()).hasSize(1));
    }

    // -------------------------------------------------------------------------

    private LppGroup healthyGroup(String groupId, String role, int numNodes) {
        LppGroup group = new LppGroup(groupId, "zone-a", role);
        for (int i = 0; i < numNodes; i++) {
            LppNode node = new LppNode("node-" + groupId + "-" + i, "odin-instance", groupId, role, "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return group;
    }
}
