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

    // ---- correct shard count ----

    @Test
    void producesOneAllocationPerShard() {
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 4, 1);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                threeGroups(), List.of(index));

        assertThat(result).hasSize(4);
        assertThat(result).containsKeys("grocery.idx.1.0", "grocery.idx.1.1", "grocery.idx.1.2", "grocery.idx.1.3");
    }

    // ---- numIngestGroups respected ----

    @Test
    void eachShardAssignedToExactlyNumIngestGroups_one() {
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 3, 1);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                threeGroups(), List.of(index));

        result.values().forEach(a ->
                assertThat(a.getIngestGroupIds()).hasSize(1));
    }

    @Test
    void eachShardAssignedToExactlyNumIngestGroups_two() {
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 3, 2);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                threeGroups(), List.of(index));

        result.values().forEach(a ->
                assertThat(a.getIngestGroupIds()).hasSize(2));
    }

    @Test
    void usesAllGroupsWhenNumGroupsExceedsPoolSize() {
        // numIngestGroups=5 but only 3 groups available → all 3 used
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 5);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                threeGroups(), List.of(index));

        assertThat(result.get("grocery.idx.1.0").getIngestGroupIds()).hasSize(3);
    }

    @Test
    void noGroupAssignedMoreThanOncePerShard() {
        // numIngestGroups=2, 3 eligible groups — no duplicates within a shard
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 6, 2);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                threeGroups(), List.of(index));

        result.values().forEach(a -> {
            List<String> ids = a.getIngestGroupIds();
            assertThat(ids).doesNotHaveDuplicates();
        });
    }

    // ---- eligibility filters ----

    @Test
    void partiallyDrainedGroupStillEligible() {
        // Group with 1 of 2 nodes DRAIN — should still receive shard assignments.
        // Ejecting it would cause unnecessary resharding; the remaining node can still serve.
        LppGroup partial = healthyGroup("partial", 2);
        partial.getNodes().get(0).setAdminState("DRAIN"); // only 1 of 2 drained
        Map<String, LppGroup> groups = Map.of("partial", partial);

        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 2, 1);
        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        assertThat(result).hasSize(2);
        result.values().forEach(a ->
                assertThat(a.getIngestGroupIds()).containsOnly("partial"));
    }

    @Test
    void fullyDrainedGroupExcluded() {
        // All nodes DRAIN → group ineligible (decommission complete)
        LppGroup allDrain = healthyGroup("g-drain", 2);
        allDrain.getNodes().forEach(n -> n.setAdminState("DRAIN"));
        LppGroup healthy = healthyGroup("g-healthy", 2);
        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groups.put("g-drain", allDrain);
        groups.put("g-healthy", healthy);

        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 2, 1);
        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        result.values().forEach(a ->
                assertThat(a.getIngestGroupIds()).containsOnly("g-healthy"));
    }

    @Test
    void onlyNonDrainedGroupsAssigned() {
        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groups.put("healthy", healthyGroup("healthy", 2));
        LppGroup drained = healthyGroup("drained", 2);
        drained.getNodes().forEach(n -> n.setAdminState("DRAIN"));
        groups.put("drained", drained);

        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 3, 1);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        result.values().forEach(a ->
                assertThat(a.getIngestGroupIds()).containsOnly("healthy"));
    }

    @Test
    void allGroupsEligibleRegardlessOfRole() {
        // All groups handle both ingest and search — no role-based exclusion
        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groups.put("g0", healthyGroup("g0", 2));
        groups.put("g1", healthyGroup("g1", 2));

        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 2);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        // Both groups should be assigned — no role filter removes either
        assertThat(result.get("grocery.idx.1.0").getIngestGroupIds()).hasSize(2);
    }

    @Test
    void returnsEmptyWhenNoEligibleGroups() {
        LppGroup drained = healthyGroup("g1", 2);
        drained.getNodes().forEach(n -> n.setAdminState("DRAIN"));

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", drained),
                List.of(new LppIndexDefinition("grocery", "idx", "idx.1", 2, 1)));

        assertThat(result).isEmpty();
    }

    // ---- stable allocation ----

    @Test
    void keepsStableAllocationWithoutRewriting() {
        LppGroup group = healthyGroup("g1", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(group);

        when(metadataStore.getShardPlannedAllocation("grocery.idx.1.0"))
                .thenReturn(Optional.of(existing));

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        verify(metadataStore, never()).putShardPlannedAllocation(any());
        assertThat(result).containsKey("grocery.idx.1.0");
    }

    @Test
    void rewritesAllocationWhenGroupSetChanges() {
        LppGroup g1 = healthyGroup("g1", 2);
        LppGroup g2 = healthyGroup("g2", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 2);

        // Existing only has g1, but now 2 groups desired
        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(g1);

        when(metadataStore.getShardPlannedAllocation("grocery.idx.1.0"))
                .thenReturn(Optional.of(existing));

        allocator.allocate(Map.of("g1", g1, "g2", g2), List.of(index));

        verify(metadataStore).putShardPlannedAllocation(any());
    }

    // ---- node population ----

    @Test
    void allNodesFromAssignedGroupsPopulatedInAllocation() {
        // Group with 3 replicas → all 3 node names in the allocation
        LppGroup group = healthyGroup("g1", 3);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        assertThat(result.get("grocery.idx.1.0").getAssignedNodeNames()).hasSize(3);
    }

    // ---- custom strategy injection ----

    @Test
    void customStrategyIsUsed() {
        // Strategy that always picks the first group — deterministic for this test
        AllocationStrategy firstGroupStrategy = (eligible, numGroups, counts) ->
                List.of(eligible.get(0));

        allocator = new LppShardAllocator(metadataStore, firstGroupStrategy);

        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groups.put("g0", healthyGroup("g0", 2));
        groups.put("g1", healthyGroup("g1", 2));

        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 3, 1);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(groups, List.of(index));

        // Every shard should go to g0 (always first in insertion-order LinkedHashMap)
        result.values().forEach(a ->
                assertThat(a.getIngestGroupIds()).containsExactly("g0"));
    }

    // -------------------------------------------------------------------------

    private Map<String, LppGroup> threeGroups() {
        Map<String, LppGroup> groups = new LinkedHashMap<>();
        groups.put("g0", healthyGroup("g0", 2));
        groups.put("g1", healthyGroup("g1", 2));
        groups.put("g2", healthyGroup("g2", 2));
        return groups;
    }

    private LppGroup healthyGroup(String groupId, int numNodes) {
        LppGroup group = new LppGroup(groupId, "zone-a", "INGEST");
        for (int i = 0; i < numNodes; i++) {
            LppNode node = new LppNode("node-" + groupId + "-" + i, "odin-instance", groupId, "INGEST", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return group;
    }
}
