package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.allocation.AllocationStrategy;
import io.clustercontroller.lpp.allocation.LppShardAllocator;
import io.clustercontroller.lpp.allocation.UniformAllocationStrategy;
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
        assertThat(result).containsKeys("idx.1/0", "idx.1/1", "idx.1/2", "idx.1/3");
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

        assertThat(result.get("idx.1/0").getIngestGroupIds()).hasSize(3);
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
        assertThat(result.get("idx.1/0").getIngestGroupIds()).hasSize(2);
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

    // ---- PA reconciliation — dead node / group pruning ----

    @Test
    void prunesDeadNodeFromExistingAllocation() {
        // Existing PA has 3 nodes from g1, but current topology only has 2 (one died)
        LppGroup g1 = healthyGroup("g1", 2);  // live topology: 2 nodes
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        // Existing PA was built when g1 had 3 nodes
        LppGroup g1Old = healthyGroup("g1", 3);
        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(g1Old);

        when(metadataStore.getShardPlannedAllocation("idx.1/0"))
                .thenReturn(Optional.of(existing));

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", g1), List.of(index));

        // PA updated in etcd (nodes changed)
        verify(metadataStore).putShardPlannedAllocation(any());
        assertThat(result.get("idx.1/0").getAssignedNodeNames()).hasSize(2);
        assertThat(result.get("idx.1/0").getAssignedGroupIds()).containsOnly("g1");
    }

    @Test
    void prunesDeadGroupFromExistingAllocationAndReAllocates() {
        // scale=2, existing PA has g1+g2, but g2 is dead — drops to 1 group < scale=2 → re-allocate
        LppGroup g1 = healthyGroup("g1", 2);
        LppGroup g3 = healthyGroup("g3", 2);  // new group available
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 2);

        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(healthyGroup("g1", 2));
        existing.addGroup(healthyGroup("g2", 2));  // g2 is now dead

        when(metadataStore.getShardPlannedAllocation("idx.1/0"))
                .thenReturn(Optional.of(existing));

        Map<String, LppGroup> liveGroups = new LinkedHashMap<>();
        liveGroups.put("g1", g1);
        liveGroups.put("g3", g3);

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(liveGroups, List.of(index));

        // Should have re-allocated and written a new PA
        verify(metadataStore).putShardPlannedAllocation(any());
        // Result should have 2 groups (scale satisfied with live groups)
        assertThat(result.get("idx.1/0").getAssignedGroupIds()).hasSize(2);
    }

    @Test
    void picksUpNewReplicaJoiningExistingGroup() {
        // g1 had 2 nodes when PA was written; a 3rd replica joined the group
        LppGroup g1Live = healthyGroup("g1", 3);  // now has 3 nodes
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(healthyGroup("g1", 2));  // old PA only had 2 nodes

        when(metadataStore.getShardPlannedAllocation("idx.1/0"))
                .thenReturn(Optional.of(existing));

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", g1Live), List.of(index));

        // New replica should be in the updated PA
        verify(metadataStore).putShardPlannedAllocation(any());
        assertThat(result.get("idx.1/0").getAssignedNodeNames()).hasSize(3);
    }

    // ---- stable allocation ----

    @Test
    void keepsStableAllocationWithoutRewriting() {
        LppGroup group = healthyGroup("g1", 2);
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        LppShardEntry entry = new LppShardEntry("grocery", "idx", "idx.1", 0);
        LppShardPlannedAllocation existing = new LppShardPlannedAllocation(entry);
        existing.addGroup(group);

        when(metadataStore.getShardPlannedAllocation("idx.1/0"))
                .thenReturn(Optional.of(existing));

        Map<String, LppShardPlannedAllocation> result = allocator.allocate(
                Map.of("g1", group), List.of(index));

        verify(metadataStore, never()).putShardPlannedAllocation(any());
        assertThat(result).containsKey("idx.1/0");
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

        when(metadataStore.getShardPlannedAllocation("idx.1/0"))
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

        assertThat(result.get("idx.1/0").getAssignedNodeNames()).hasSize(3);
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

    // ---- role-aware planning with separate ingest/search groups ----

    @Test
    void roleAwarePlanningPopulatesIngesterAndSearcherNodeListsSeparately() {
        Map<String, LppGroup> ingestGroups = Map.of("g1", ingesterGroup("g1", 2));
        Map<String, LppGroup> searchGroups = Map.of("g1", searcherGroup("g1", 2));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        Map<String, LppShardPlannedAllocation> result =
                allocator.allocate(ingestGroups, searchGroups, List.of(index));

        LppShardPlannedAllocation alloc = result.get("idx.1/0");
        assertThat(alloc.getAssignedIngesterNodeNames()).hasSize(2);
        assertThat(alloc.getAssignedSearcherNodeNames()).hasSize(2);
        // Ingester and searcher nodes should be distinct
        assertThat(alloc.getAssignedIngesterNodeNames()).noneMatch(
                n -> alloc.getAssignedSearcherNodeNames().contains(n));
    }

    @Test
    void roleAwarePlanningAssignedNodeNamesIsUnionOfBothRoles() {
        Map<String, LppGroup> ingestGroups = Map.of("g1", ingesterGroup("g1", 2));
        Map<String, LppGroup> searchGroups = Map.of("g1", searcherGroup("g1", 3));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        Map<String, LppShardPlannedAllocation> result =
                allocator.allocate(ingestGroups, searchGroups, List.of(index));

        LppShardPlannedAllocation alloc = result.get("idx.1/0");
        assertThat(alloc.getAssignedNodeNames()).hasSize(5); // 2 ingesters + 3 searchers
    }

    @Test
    void roleAwarePlanningGroupIdsMirrorBetweenIngestAndSearch() {
        Map<String, LppGroup> ingestGroups = Map.of(
                "g1", ingesterGroup("g1", 1),
                "g2", ingesterGroup("g2", 1));
        Map<String, LppGroup> searchGroups = Map.of(
                "g1", searcherGroup("g1", 1),
                "g2", searcherGroup("g2", 1));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 2);

        // Use uniform strategy for deterministic group selection
        allocator = new LppShardAllocator(metadataStore, new UniformAllocationStrategy());
        Map<String, LppShardPlannedAllocation> result =
                allocator.allocate(ingestGroups, searchGroups, List.of(index));

        LppShardPlannedAllocation alloc = result.get("idx.1/0");
        // Same group IDs for ingest and search
        assertThat(alloc.getIngestGroupIds()).containsExactlyInAnyOrderElementsOf(alloc.getSearchGroupIds());
    }

    @Test
    void roleAwarePlanningErrorsWhenSelectedIngesterGroupMissingFromSearcherPool() {
        // g1 exists for ingesters but NOT for searchers → shard should be skipped
        Map<String, LppGroup> ingestGroups = Map.of("g1", ingesterGroup("g1", 2));
        Map<String, LppGroup> searchGroups = Map.of("g2", searcherGroup("g2", 2)); // g1 missing!
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 1, 1);

        // Strategy that always picks g1 (via uniform, which picks smallest id first)
        allocator = new LppShardAllocator(metadataStore, new UniformAllocationStrategy());
        Map<String, LppShardPlannedAllocation> result =
                allocator.allocate(ingestGroups, searchGroups, List.of(index));

        // Shard must be skipped — no partial assignment allowed
        assertThat(result).doesNotContainKey("idx.1/0");
        verify(metadataStore, never()).putShardPlannedAllocation(any());
    }

    @Test
    void roleAwarePlanningProducesOneAllocationPerShardAcrossMultipleShards() {
        Map<String, LppGroup> ingestGroups = Map.of(
                "g1", ingesterGroup("g1", 2),
                "g2", ingesterGroup("g2", 2));
        Map<String, LppGroup> searchGroups = Map.of(
                "g1", searcherGroup("g1", 2),
                "g2", searcherGroup("g2", 2));
        LppIndexDefinition index = new LppIndexDefinition("grocery", "idx", "idx.1", 4, 1);

        Map<String, LppShardPlannedAllocation> result =
                allocator.allocate(ingestGroups, searchGroups, List.of(index));

        assertThat(result).hasSize(4);
    }

    // ---- helpers ----

    private LppGroup ingesterGroup(String groupId, int numNodes) {
        LppGroup group = new LppGroup(groupId, "zone-a", "ingester");
        for (int i = 0; i < numNodes; i++) {
            LppNode node = new LppNode("node-" + groupId + "-ingester-" + i, "odin", groupId, "ingester", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return group;
    }

    private LppGroup searcherGroup(String groupId, int numNodes) {
        LppGroup group = new LppGroup(groupId, "zone-a", "searcher");
        for (int i = 0; i < numNodes; i++) {
            LppNode node = new LppNode("node-" + groupId + "-searcher-" + i, "odin", groupId, "searcher", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        return group;
    }

    // ---- existing test helpers ----

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
