package io.clustercontroller.lpp.discovery;

import io.clustercontroller.lpp.models.LppGroup;
import io.clustercontroller.lpp.models.LppNodeActualState;
import io.clustercontroller.lpp.store.LppMetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LppDiscoveryTest {

    @Mock private LppMetadataStore metadataStore;

    private InMemoryGrailClient grailClient;
    private LppDiscovery discovery;

    // All test nodes belong to the same Odin instance (= same LPP cluster).
    // Groups are differentiated by grailShardId within that instance.
    private static final String ODIN_INSTANCE = "delivery-grocery-1";

    @BeforeEach
    void setUp() {
        grailClient = new InMemoryGrailClient();
        discovery = new LppDiscovery(grailClient, metadataStore, "delivery-grocery");

        // No stale nodes by default
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of());
    }

    @Test
    void discoverReturnsEmptyGroupsWhenNoNodes() {
        Map<String, LppGroup> groups = discovery.discover();
        assertThat(groups).isEmpty();
    }

    @Test
    void discoverBuildsGroupFromGrailShardId() {
        // grailShardId "shard-0" is the group key, not odinInstance
        LppNodeActualState node = new LppNodeActualState("node-001", ODIN_INSTANCE, "shard-0", "INGEST", "zone-a");
        grailClient.addNode("delivery-grocery", node);

        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of("node-001", node));

        Map<String, LppGroup> groups = discovery.discover();

        assertThat(groups).containsKey("shard-0");
        assertThat(groups.get("shard-0").getNodes()).hasSize(1);
        assertThat(groups.get("shard-0").getNodes().get(0).getNodeName()).isEqualTo("node-001");
    }

    @Test
    void discoverGroupsMultipleReplicasUnderSameShardId() {
        // 3 nodes with the same grailShardId = one replica group
        LppNodeActualState n1 = new LppNodeActualState("node-001", ODIN_INSTANCE, "shard-0", "INGEST", "zone-a");
        LppNodeActualState n2 = new LppNodeActualState("node-002", ODIN_INSTANCE, "shard-0", "INGEST", "zone-b");
        LppNodeActualState n3 = new LppNodeActualState("node-003", ODIN_INSTANCE, "shard-0", "INGEST", "zone-c");
        grailClient.addNode("delivery-grocery", n1);
        grailClient.addNode("delivery-grocery", n2);
        grailClient.addNode("delivery-grocery", n3);

        when(metadataStore.getAllNodeActualStates()).thenReturn(
                Map.of("node-001", n1, "node-002", n2, "node-003", n3));

        Map<String, LppGroup> groups = discovery.discover();

        // All three replicas collapse into one group keyed by grailShardId
        assertThat(groups).hasSize(1);
        assertThat(groups.get("shard-0").getNodes()).hasSize(3);
    }

    @Test
    void discoverCreatesSeparateGroupsForDifferentShardIds() {
        // Two nodes with the same odinInstance but different grailShardIds = two groups
        LppNodeActualState n1 = new LppNodeActualState("node-001", ODIN_INSTANCE, "shard-0", "INGEST", "zone-a");
        LppNodeActualState n2 = new LppNodeActualState("node-002", ODIN_INSTANCE, "shard-1", "INGEST", "zone-b");
        grailClient.addNode("delivery-grocery", n1);
        grailClient.addNode("delivery-grocery", n2);

        when(metadataStore.getAllNodeActualStates()).thenReturn(
                Map.of("node-001", n1, "node-002", n2));

        Map<String, LppGroup> groups = discovery.discover();

        assertThat(groups).hasSize(2);
        assertThat(groups).containsKeys("shard-0", "shard-1");
    }

    @Test
    void discoverMixedReplicaCountsAcrossGroups() {
        // shard-0 has 3 replicas, shard-1 has 1 — still two distinct groups
        LppNodeActualState n1 = new LppNodeActualState("node-001", ODIN_INSTANCE, "shard-0", "INGEST", "zone-a");
        LppNodeActualState n2 = new LppNodeActualState("node-002", ODIN_INSTANCE, "shard-0", "INGEST", "zone-b");
        LppNodeActualState n3 = new LppNodeActualState("node-003", ODIN_INSTANCE, "shard-0", "INGEST", "zone-c");
        LppNodeActualState n4 = new LppNodeActualState("node-004", ODIN_INSTANCE, "shard-1", "INGEST", "zone-a");
        grailClient.addNode("delivery-grocery", n1);
        grailClient.addNode("delivery-grocery", n2);
        grailClient.addNode("delivery-grocery", n3);
        grailClient.addNode("delivery-grocery", n4);

        when(metadataStore.getAllNodeActualStates()).thenReturn(
                Map.of("node-001", n1, "node-002", n2, "node-003", n3, "node-004", n4));

        Map<String, LppGroup> groups = discovery.discover();

        assertThat(groups).hasSize(2);
        assertThat(groups.get("shard-0").getNodes()).hasSize(3);
        assertThat(groups.get("shard-1").getNodes()).hasSize(1);
    }

    @Test
    void discoverPrunesStaleNodes() {
        LppNodeActualState stale = new LppNodeActualState("stale-node", ODIN_INSTANCE, "shard-0", "INGEST", "zone-a");
        stale.setHeartbeatTimestampMs(System.currentTimeMillis() - 20 * 60 * 1000L); // 20 min ago

        when(metadataStore.getAllNodeActualStates())
                .thenReturn(Map.of("stale-node", stale))
                .thenReturn(Map.of()); // after pruning

        Map<String, LppGroup> groups = discovery.discover();

        verify(metadataStore).deleteNodeActualState("stale-node");
        assertThat(groups).isEmpty();
    }

    @Test
    void discoverWritesGrailNodesToEtcd() {
        LppNodeActualState node = new LppNodeActualState("node-001", ODIN_INSTANCE, "shard-0", "INGEST", "zone-a");
        grailClient.addNode("delivery-grocery", node);
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of("node-001", node));

        discovery.discover();

        verify(metadataStore).putNodeActualState(any(LppNodeActualState.class));
    }

    @Test
    void discoverSkipsNodesWithNoGrailShardId() {
        // odinInstance is set but grailShardId is null → no group can be formed
        LppNodeActualState bad = new LppNodeActualState();
        bad.setNodeName("node-bad");
        bad.setOdinInstance(ODIN_INSTANCE);
        // grailShardId is null

        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of("node-bad", bad));

        Map<String, LppGroup> groups = discovery.discover();
        assertThat(groups).isEmpty();
    }
}
