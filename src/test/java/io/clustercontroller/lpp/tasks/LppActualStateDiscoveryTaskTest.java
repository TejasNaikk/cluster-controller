package io.clustercontroller.lpp.tasks;

import io.clustercontroller.lpp.allocation.LppShardAllocator;
import io.clustercontroller.lpp.discovery.LppDiscovery;
import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.orchestration.LppGoalStateOrchestrator;
import io.clustercontroller.lpp.orchestration.LppRoutingTableOrchestrator;
import io.clustercontroller.lpp.orchestration.LppStateAggregator;
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
class LppActualStateDiscoveryTaskTest {

    @Mock private LppDiscovery discovery;
    @Mock private LppShardAllocator allocator;
    @Mock private LppGoalStateOrchestrator orchestrator;
    @Mock private LppRoutingTableOrchestrator routingTableOrchestrator;
    @Mock private LppStateAggregator aggregator;
    @Mock private LppMetadataStore metadataStore;

    private LppTaskContext ctx;

    @BeforeEach
    void setUp() {
        ctx = new LppTaskContext(discovery, allocator, orchestrator, routingTableOrchestrator,
                aggregator, metadataStore, "delivery-grocery", "local");
        when(metadataStore.getAllShardPlannedAllocations()).thenReturn(Map.of());
    }

    // ---- basic discovery ----

    @Test
    void discoversIngesterAndSearcherGroupsSeparately() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "node1-ingester", liveState("node1-ingester", "ingester", "group1"),
                "node1-searcher", liveState("node1-searcher", "searcher", "group1"),
                "node2-ingester", liveState("node2-ingester", "ingester", "group2"),
                "node2-searcher", liveState("node2-searcher", "searcher", "group2")
        ));

        String result = new LppActualStateDiscoveryTask(ctx).execute();

        assertThat(result).isEqualTo("SUCCESS");
        assertThat(ctx.getCurrentIngestGroups()).containsKeys("group1", "group2");
        assertThat(ctx.getCurrentSearchGroups()).containsKeys("group1", "group2");
        assertThat(ctx.getCurrentIngestGroups().get("group1").getNodes())
                .extracting(LppNode::getNodeName).containsExactly("node1-ingester");
        assertThat(ctx.getCurrentSearchGroups().get("group1").getNodes())
                .extracting(LppNode::getNodeName).containsExactly("node1-searcher");
    }

    @Test
    void multipleNodesInSameGroupAreGroupedTogether() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "node-a-ingester", liveState("node-a-ingester", "ingester", "group1"),
                "node-b-ingester", liveState("node-b-ingester", "ingester", "group1"),
                "node-c-ingester", liveState("node-c-ingester", "ingester", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        assertThat(ctx.getCurrentIngestGroups().get("group1").getNodes()).hasSize(3);
    }

    @Test
    void nodesWithUnknownRoleAreSkipped() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "node-unknown", liveState("node-unknown", "UNKNOWN_ROLE", "group1"),
                "node-ingester", liveState("node-ingester", "ingester", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        assertThat(ctx.getCurrentIngestGroups().get("group1").getNodes())
                .extracting(LppNode::getNodeName).containsExactly("node-ingester");
        // node-unknown not added to either group
    }

    @Test
    void nodesWithNullGroupIdAreSkipped() {
        LppNodeActualState noGroup = liveState("node-no-group", "ingester", null);
        noGroup.setGroupId(null);
        noGroup.setGrailShardId(null); // no fallback either
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of("node-no-group", noGroup));

        new LppActualStateDiscoveryTask(ctx).execute();

        assertThat(ctx.getCurrentIngestGroups()).isEmpty();
    }

    @Test
    void fallsBackToGrailShardIdWhenGroupIdAbsent() {
        LppNodeActualState state = liveState("node1-ingester", "ingester", null);
        state.setGroupId(null);
        state.setGrailShardId("grail-shard-42"); // fallback
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of("node1-ingester", state));

        new LppActualStateDiscoveryTask(ctx).execute();

        // Should be grouped under grailShardId as fallback
        assertThat(ctx.getCurrentIngestGroups()).containsKey("grail-shard-42");
    }

    @Test
    void returnsSuccessWithEmptyStateWhenNoNodes() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of());

        String result = new LppActualStateDiscoveryTask(ctx).execute();

        assertThat(result).isEqualTo("SUCCESS");
        assertThat(ctx.getCurrentIngestGroups()).isEmpty();
        assertThat(ctx.getCurrentSearchGroups()).isEmpty();
    }

    @Test
    void returnsFailedOnException() {
        when(metadataStore.getAllNodeActualStates()).thenThrow(new RuntimeException("etcd down"));

        String result = new LppActualStateDiscoveryTask(ctx).execute();
        assertThat(result).isEqualTo("FAILED");
    }

    // ---- stale node cleanup ----

    @Test
    void staleNodeIsDeletedFromEtcd() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "stale-node", staleState("stale-node", "ingester", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        verify(metadataStore).deleteNodeActualState("stale-node");
        verify(metadataStore).deleteNodeGoalState("stale-node");
    }

    @Test
    void staleNodeIsNotAddedToGroupTopology() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "stale-node", staleState("stale-node", "ingester", "group1"),
                "live-node",  liveState("live-node", "ingester", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        assertThat(ctx.getCurrentIngestGroups().get("group1").getNodes())
                .extracting(LppNode::getNodeName).containsExactly("live-node");
    }

    @Test
    void staleNodeRemovedFromPlannedAllocations() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "stale-node", staleState("stale-node", "ingester", "group1")
        ));

        // PA still references the stale node
        LppShardEntry entry = new LppShardEntry("col", "idx", "idx.1", 0);
        LppShardPlannedAllocation pa = new LppShardPlannedAllocation(entry);
        pa.getAssignedNodeNames().add("stale-node");
        pa.getAssignedIngesterNodeNames().add("stale-node");

        when(metadataStore.getAllShardPlannedAllocations()).thenReturn(
                Map.of("col.idx.1.0", pa));

        new LppActualStateDiscoveryTask(ctx).execute();

        // PA should have been updated to remove stale-node
        ArgumentCaptor<LppShardPlannedAllocation> captor =
                ArgumentCaptor.forClass(LppShardPlannedAllocation.class);
        verify(metadataStore).putShardPlannedAllocation(captor.capture());
        assertThat(captor.getValue().getAssignedNodeNames()).doesNotContain("stale-node");
        assertThat(captor.getValue().getAssignedIngesterNodeNames()).doesNotContain("stale-node");
    }

    @Test
    void liveNodeNotDeletedFromEtcd() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "live-node", liveState("live-node", "ingester", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        verify(metadataStore, never()).deleteNodeActualState(any());
        verify(metadataStore, never()).deleteNodeGoalState(any());
    }

    @Test
    void mixOfLiveAndStaleNodesHandledCorrectly() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "live-ingester",  liveState("live-ingester",  "ingester", "group1"),
                "stale-ingester", staleState("stale-ingester", "ingester", "group1"),
                "live-searcher",  liveState("live-searcher",  "searcher", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        // Only live nodes in topology
        assertThat(ctx.getCurrentIngestGroups().get("group1").getNodes())
                .extracting(LppNode::getNodeName).containsExactly("live-ingester");
        assertThat(ctx.getCurrentSearchGroups().get("group1").getNodes())
                .extracting(LppNode::getNodeName).containsExactly("live-searcher");

        // Stale node cleaned up
        verify(metadataStore).deleteNodeActualState("stale-ingester");
        verify(metadataStore).deleteNodeGoalState("stale-ingester");
        verify(metadataStore, never()).deleteNodeActualState("live-ingester");
    }

    // ---- ingester/searcher peer validation ----

    @Test
    void searcherWithNoIngesterPeerIsExcluded() {
        // 1 ingester (node 0) + 2 searchers (node 0 and node 1) — searcher-1 has no ingester-1 peer
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "node-g1-ingester-0", liveState("node-g1-ingester-0", "ingester", "group1"),
                "node-g1-searcher-0", liveState("node-g1-searcher-0", "searcher", "group1"),
                "node-g1-searcher-1", liveState("node-g1-searcher-1", "searcher", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        // searcher-0 has peer ingester-0 → kept
        // searcher-1 has no peer ingester-1 → excluded
        assertThat(ctx.getCurrentSearchGroups().get("group1").getNodes())
                .extracting(LppNode::getNodeName)
                .containsExactly("node-g1-searcher-0")
                .doesNotContain("node-g1-searcher-1");
    }

    @Test
    void equalIngesterAndSearcherCountAllKept() {
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                "node-g1-ingester-0", liveState("node-g1-ingester-0", "ingester", "group1"),
                "node-g1-ingester-1", liveState("node-g1-ingester-1", "ingester", "group1"),
                "node-g1-searcher-0", liveState("node-g1-searcher-0", "searcher", "group1"),
                "node-g1-searcher-1", liveState("node-g1-searcher-1", "searcher", "group1")
        ));

        new LppActualStateDiscoveryTask(ctx).execute();

        assertThat(ctx.getCurrentIngestGroups().get("group1").getNodes()).hasSize(2);
        assertThat(ctx.getCurrentSearchGroups().get("group1").getNodes()).hasSize(2);
    }

    // -------------------------------------------------------------------------

    private LppNodeActualState liveState(String nodeName, String role, String groupId) {
        LppNodeActualState state = new LppNodeActualState();
        state.setNodeName(nodeName);
        state.setRole(role);
        state.setGroupId(groupId);
        state.setZone("zone-a");
        state.setHeartbeatTimestampMs(System.currentTimeMillis()); // fresh
        return state;
    }

    private LppNodeActualState staleState(String nodeName, String role, String groupId) {
        LppNodeActualState state = liveState(nodeName, role, groupId);
        // Set heartbeat way in the past (> 10 min)
        state.setHeartbeatTimestampMs(System.currentTimeMillis() - 15 * 60 * 1000L);
        return state;
    }
}
