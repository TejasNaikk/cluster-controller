package io.clustercontroller.lpp.discovery;

import io.clustercontroller.lpp.models.LppGroup;
import io.clustercontroller.lpp.models.LppNodeActualState;
import io.clustercontroller.lpp.store.LppEtcdPathResolver;
import io.clustercontroller.lpp.store.LppMetadataStore;
import io.etcd.jetcd.Client;
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
    void discoverBuildsGroupFromOdinInstance() {
        LppNodeActualState node = new LppNodeActualState("node-001", "delivery-grocery-1", "shard-0", "INGEST", "zone-a");
        grailClient.addNode("delivery-grocery", node);

        when(metadataStore.getAllNodeActualStates()).thenReturn(
                Map.of("node-001", node));

        Map<String, LppGroup> groups = discovery.discover();

        assertThat(groups).containsKey("delivery-grocery-1");
        assertThat(groups.get("delivery-grocery-1").getNodes()).hasSize(1);
        assertThat(groups.get("delivery-grocery-1").getNodes().get(0).getNodeName()).isEqualTo("node-001");
    }

    @Test
    void discoverGroupsMultipleNodesUnderSameOdinInstance() {
        LppNodeActualState n1 = new LppNodeActualState("node-001", "delivery-grocery-1", "shard-0", "INGEST", "zone-a");
        LppNodeActualState n2 = new LppNodeActualState("node-002", "delivery-grocery-1", "shard-0", "INGEST", "zone-b");
        grailClient.addNode("delivery-grocery", n1);
        grailClient.addNode("delivery-grocery", n2);

        when(metadataStore.getAllNodeActualStates()).thenReturn(
                Map.of("node-001", n1, "node-002", n2));

        Map<String, LppGroup> groups = discovery.discover();

        assertThat(groups).hasSize(1);
        assertThat(groups.get("delivery-grocery-1").getNodes()).hasSize(2);
    }

    @Test
    void discoverCreatesSeparateGroupsForDifferentOdinInstances() {
        LppNodeActualState n1 = new LppNodeActualState("node-001", "odin-instance-A", "shard-0", "INGEST", "zone-a");
        LppNodeActualState n2 = new LppNodeActualState("node-002", "odin-instance-B", "shard-0", "INGEST", "zone-b");
        grailClient.addNode("delivery-grocery", n1);
        grailClient.addNode("delivery-grocery", n2);

        when(metadataStore.getAllNodeActualStates()).thenReturn(
                Map.of("node-001", n1, "node-002", n2));

        Map<String, LppGroup> groups = discovery.discover();

        assertThat(groups).hasSize(2);
        assertThat(groups).containsKeys("odin-instance-A", "odin-instance-B");
    }

    @Test
    void discoverPrunesStaleNodes() {
        LppNodeActualState stale = new LppNodeActualState("stale-node", "odin-instance-A", "shard-0", "INGEST", "zone-a");
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
        LppNodeActualState node = new LppNodeActualState("node-001", "odin-A", "shard-0", "INGEST", "zone-a");
        grailClient.addNode("delivery-grocery", node);
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of("node-001", node));

        discovery.discover();

        verify(metadataStore).putNodeActualState(any(LppNodeActualState.class));
    }

    @Test
    void discoverSkipsNodesWithNoOdinInstance() {
        LppNodeActualState bad = new LppNodeActualState();
        bad.setNodeName("node-bad");
        // odinInstance is null

        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of("node-bad", bad));

        Map<String, LppGroup> groups = discovery.discover();
        assertThat(groups).isEmpty();
    }
}
