package io.clustercontroller.lpp.orchestration;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
class LppRoutingTableOrchestratorTest {

    @Mock private LppMetadataStore metadataStore;

    private LppRoutingTableOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        orchestrator = new LppRoutingTableOrchestrator(metadataStore);
        when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of());
        when(metadataStore.getRoutingTable()).thenReturn(Optional.empty());
    }

    // =========================================================================
    // buildRoutingTable — pure logic, no etcd writes
    // =========================================================================

    @Nested
    class BuildRoutingTable {

        @Test
        void emptyPlannedAllocationsProducesEmptyTable() {
            LppRoutingTable table = orchestrator.buildRoutingTable(Map.of(), Map.of(), 0);
            assertThat(table.getShardRoutes()).isEmpty();
        }

        @Test
        void primaryPathNodeInPaAndActive() {
            // PA has node-1; node-1's actual state has shard ACTIVE
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1");
            Map<String, LppNodeActualState> aa = Map.of("node-1", activeNode("node-1", "g1", shardKey));

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            assertThat(routes).hasSize(1);
            assertThat(routes.get(0).getNodeName()).isEqualTo("node-1");
        }

        @Test
        void primaryPathMultipleNodesInPaAllActive() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1", "node-2", "node-3");
            Map<String, LppNodeActualState> aa = Map.of(
                    "node-1", activeNode("node-1", "g1", shardKey),
                    "node-2", activeNode("node-2", "g1", shardKey),
                    "node-3", activeNode("node-3", "g1", shardKey)
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            assertThat(routes).hasSize(3);
            assertThat(routes).extracting(LppShardRoute::getNodeName)
                    .containsExactlyInAnyOrder("node-1", "node-2", "node-3");
        }

        @Test
        void primaryPathExcludesNodeNotYetActive() {
            // node-1 in PA and ACTIVE, node-2 in PA but PENDING (not yet active)
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1", "node-2");
            Map<String, LppNodeActualState> aa = Map.of(
                    "node-1", activeNode("node-1", "g1", shardKey),
                    "node-2", nodeWithShardState("node-2", "g1", shardKey, "PENDING")
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            assertThat(routes).hasSize(1);
            assertThat(routes.get(0).getNodeName()).isEqualTo("node-1");
        }

        @Test
        void primaryPathExcludesNodeWithNoActualState() {
            // node-1 in PA but no actual state in etcd (not yet discovered)
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1");
            Map<String, LppNodeActualState> aa = Map.of(); // node-1 not present

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            // Falls through to fallback — still empty because no node is ACTIVE anywhere
            assertThat(routes).isEmpty();
        }

        @Test
        void fallbackActivatesWhenPaIntersectionEmpty() {
            // PA has node-1 (not yet active), but node-2 (not in PA) is already ACTIVE
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1");
            Map<String, LppNodeActualState> aa = Map.of(
                    "node-1", nodeWithShardState("node-1", "g1", shardKey, "DOWNLOADING"),
                    "node-2", activeNode("node-2", "g2", shardKey)  // not in PA
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            assertThat(routes).hasSize(1);
            assertThat(routes.get(0).getNodeName()).isEqualTo("node-2");
        }

        @Test
        void fallbackPicksMultipleActiveNodesNotInPa() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-new");
            // node-new is in PA but not yet active; old nodes have shard active
            Map<String, LppNodeActualState> aa = Map.of(
                    "node-new", nodeWithShardState("node-new", "g2", shardKey, "DOWNLOADING"),
                    "node-old-1", activeNode("node-old-1", "g1", shardKey),
                    "node-old-2", activeNode("node-old-2", "g1", shardKey)
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            assertThat(routes).hasSize(2);
            assertThat(routes).extracting(LppShardRoute::getNodeName)
                    .containsExactlyInAnyOrder("node-old-1", "node-old-2");
        }

        @Test
        void staleNodesExcludedFromPrimaryPath() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-stale");
            LppNodeActualState stale = activeNode("node-stale", "g1", shardKey);
            stale.setHeartbeatTimestampMs(0L); // very stale

            Map<String, LppNodeActualState> aa = Map.of("node-stale", stale);

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            // Stale node excluded from primary and fallback
            assertThat(routes).isEmpty();
        }

        @Test
        void staleNodesExcludedFromFallback() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-planned");
            // node-planned not active; node-stale active but stale heartbeat
            LppNodeActualState stale = activeNode("node-stale", "g2", shardKey);
            stale.setHeartbeatTimestampMs(0L);

            Map<String, LppNodeActualState> aa = Map.of(
                    "node-planned", nodeWithShardState("node-planned", "g1", shardKey, "PENDING"),
                    "node-stale", stale
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey);
            assertThat(routes).isEmpty();
        }

        @Test
        void nodeWithDifferentShardActiveDoesNotCountForThisShard() {
            String shardKey0 = "grocery.deals.v1.0";
            String shardKey1 = "grocery.deals.v1.1";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey0, "node-1");
            // node-1 has shard 1 ACTIVE, but not shard 0
            Map<String, LppNodeActualState> aa = Map.of("node-1", activeNode("node-1", "g1", shardKey1));

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<LppShardRoute> routes = table.getShardRoutes().get(shardKey0);
            assertThat(routes).isEmpty();
        }

        @Test
        void routeCarriesHostAndPort() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1");
            LppNodeActualState state = activeNode("node-1", "g1", shardKey);
            state.setHost("lpp-host-42");
            state.setPort(25752);

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, Map.of("node-1", state), 0);

            LppShardRoute route = table.getShardRoutes().get(shardKey).get(0);
            assertThat(route.getHost()).isEqualTo("lpp-host-42");
            assertThat(route.getPort()).isEqualTo(25752);
            assertThat(route.getGroupId()).isEqualTo("g1");
        }

        @Test
        void multipleShardsMappedIndependently() {
            String sk0 = "grocery.deals.v1.0";
            String sk1 = "grocery.deals.v1.1";
            Map<String, LppShardPlannedAllocation> pa = new LinkedHashMap<>();
            pa.putAll(paWithNodes(sk0, "node-a"));
            pa.putAll(paWithNodes(sk1, "node-b"));

            Map<String, LppNodeActualState> aa = Map.of(
                    "node-a", activeNode("node-a", "g1", sk0),
                    "node-b", activeNode("node-b", "g2", sk1)
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            assertThat(table.getShardRoutes()).containsKeys(sk0, sk1);
            assertThat(table.getShardRoutes().get(sk0).get(0).getNodeName()).isEqualTo("node-a");
            assertThat(table.getShardRoutes().get(sk1).get(0).getNodeName()).isEqualTo("node-b");
        }
    }

    // =========================================================================
    // hasChanged
    // =========================================================================

    @Nested
    class HasChanged {

        @Test
        void nullExistingWithNonEmptyNewIsChanged() {
            LppRoutingTable newTable = new LppRoutingTable(0);
            newTable.setRoutes("sk", List.of(new LppShardRoute("n1", "h", 9000, "g1")));
            assertThat(orchestrator.hasChanged(null, newTable)).isTrue();
        }

        @Test
        void nullExistingWithEmptyNewIsNotChanged() {
            assertThat(orchestrator.hasChanged(null, new LppRoutingTable(0))).isFalse();
        }

        @Test
        void identicalTablesAreNotChanged() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setRoutes("sk", List.of(new LppShardRoute("n1", "h", 9000, "g1")));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setRoutes("sk", List.of(new LppShardRoute("n1", "h", 9000, "g1")));
            assertThat(orchestrator.hasChanged(t1, t2)).isFalse();
        }

        @Test
        void addedShardIsDetectedAsChange() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setRoutes("sk0", List.of(new LppShardRoute("n1", "h", 9000, "g1")));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setRoutes("sk0", List.of(new LppShardRoute("n1", "h", 9000, "g1")));
            t2.setRoutes("sk1", List.of(new LppShardRoute("n2", "h", 9000, "g1")));
            assertThat(orchestrator.hasChanged(t1, t2)).isTrue();
        }

        @Test
        void removedNodeInShardIsDetectedAsChange() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setRoutes("sk", List.of(
                    new LppShardRoute("n1", "h", 9000, "g1"),
                    new LppShardRoute("n2", "h", 9000, "g1")));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setRoutes("sk", List.of(new LppShardRoute("n1", "h", 9000, "g1")));
            assertThat(orchestrator.hasChanged(t1, t2)).isTrue();
        }

        @Test
        void replacedNodeInShardIsDetectedAsChange() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setRoutes("sk", List.of(new LppShardRoute("n1", "h", 9000, "g1")));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setRoutes("sk", List.of(new LppShardRoute("n2", "h", 9000, "g1")));
            assertThat(orchestrator.hasChanged(t1, t2)).isTrue();
        }

        @Test
        void orderChangeWithSameNodesIsNotChanged() {
            // hasChanged is set-based, order doesn't matter
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setRoutes("sk", List.of(
                    new LppShardRoute("n1", "h", 9000, "g1"),
                    new LppShardRoute("n2", "h", 9000, "g1")));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setRoutes("sk", List.of(
                    new LppShardRoute("n2", "h", 9000, "g1"),
                    new LppShardRoute("n1", "h", 9000, "g1")));
            assertThat(orchestrator.hasChanged(t1, t2)).isFalse();
        }

        @Test
        void emptyRouteListIsNotChanged() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setRoutes("sk", List.of());
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setRoutes("sk", List.of());
            assertThat(orchestrator.hasChanged(t1, t2)).isFalse();
        }
    }

    // =========================================================================
    // computeAndPersist — etcd interaction
    // =========================================================================

    @Nested
    class ComputeAndPersist {

        @Test
        void writesToEtcdOnFirstRun() {
            String shardKey = "grocery.deals.v1.0";
            when(metadataStore.getAllNodeActualStates())
                    .thenReturn(Map.of("node-1", activeNode("node-1", "g1", shardKey)));

            LppRoutingTable result = orchestrator.computeAndPersist(paWithNodes(shardKey, "node-1"));

            verify(metadataStore).putRoutingTable(any());
            assertThat(result.getVersion()).isEqualTo(1);
        }

        @Test
        void versionIncrementsWhenTableChanges() {
            String shardKey = "grocery.deals.v1.0";

            LppRoutingTable existing = new LppRoutingTable(5);
            existing.setRoutes(shardKey, List.of(new LppShardRoute("old-node", "h", 9000, "g1")));
            when(metadataStore.getRoutingTable()).thenReturn(Optional.of(existing));
            when(metadataStore.getAllNodeActualStates())
                    .thenReturn(Map.of("node-1", activeNode("node-1", "g1", shardKey)));

            LppRoutingTable result = orchestrator.computeAndPersist(paWithNodes(shardKey, "node-1"));

            verify(metadataStore).putRoutingTable(any());
            assertThat(result.getVersion()).isEqualTo(6);
        }

        @Test
        void noWriteWhenTableUnchanged() {
            String shardKey = "grocery.deals.v1.0";

            LppRoutingTable existing = new LppRoutingTable(3);
            existing.setRoutes(shardKey, List.of(new LppShardRoute("node-1", "h", 9000, "g1")));
            when(metadataStore.getRoutingTable()).thenReturn(Optional.of(existing));
            when(metadataStore.getAllNodeActualStates())
                    .thenReturn(Map.of("node-1", activeNode("node-1", "g1", shardKey)));

            orchestrator.computeAndPersist(paWithNodes(shardKey, "node-1"));

            verify(metadataStore, never()).putRoutingTable(any());
        }

        @Test
        void skipsWhenNoAllocations() {
            orchestrator.computeAndPersist(Map.of());
            verify(metadataStore, never()).putRoutingTable(any());
            verify(metadataStore, never()).getAllNodeActualStates();
        }

        @Test
        void writtenTableHasLastUpdatedMs() {
            String shardKey = "grocery.deals.v1.0";
            when(metadataStore.getAllNodeActualStates())
                    .thenReturn(Map.of("node-1", activeNode("node-1", "g1", shardKey)));

            long before = System.currentTimeMillis();
            orchestrator.computeAndPersist(paWithNodes(shardKey, "node-1"));
            long after = System.currentTimeMillis();

            ArgumentCaptor<LppRoutingTable> captor = ArgumentCaptor.forClass(LppRoutingTable.class);
            verify(metadataStore).putRoutingTable(captor.capture());
            assertThat(captor.getValue().getLastUpdatedMs()).isBetween(before, after);
        }

        @Test
        void fallbackUsedWhenPaNodesNotActive() {
            String shardKey = "grocery.deals.v1.0";
            // PA has node-planned, but node-fallback (not in PA) is the one that's active
            when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                    "node-planned", nodeWithShardState("node-planned", "g1", shardKey, "DOWNLOADING"),
                    "node-fallback", activeNode("node-fallback", "g2", shardKey)
            ));

            orchestrator.computeAndPersist(paWithNodes(shardKey, "node-planned"));

            ArgumentCaptor<LppRoutingTable> captor = ArgumentCaptor.forClass(LppRoutingTable.class);
            verify(metadataStore).putRoutingTable(captor.capture());
            List<LppShardRoute> routes = captor.getValue().getShardRoutes().get(shardKey);
            assertThat(routes).hasSize(1);
            assertThat(routes.get(0).getNodeName()).isEqualTo("node-fallback");
        }
    }

    // =========================================================================
    // resolveRoutes — edge cases
    // =========================================================================

    @Nested
    class ResolveRoutes {

        @Test
        void emptyPaAndEmptyAaProducesEmptyRoutes() {
            LppShardPlannedAllocation alloc = allocWithNodes("sk", List.of());
            List<LppShardRoute> routes = orchestrator.resolveRoutes("sk", alloc, Map.of());
            assertThat(routes).isEmpty();
        }

        @Test
        void onlyPaNodeNotInAaProducesEmptyRoutes() {
            LppShardPlannedAllocation alloc = allocWithNodes("sk", List.of("node-1"));
            List<LppShardRoute> routes = orchestrator.resolveRoutes("sk", alloc, Map.of());
            assertThat(routes).isEmpty();
        }

        @Test
        void paNodeActiveForDifferentShardDoesNotRoute() {
            LppShardPlannedAllocation alloc = allocWithNodes("sk.0", List.of("node-1"));
            // node-1 is ACTIVE for shard 1, not shard 0
            LppNodeActualState state = activeNode("node-1", "g1", "sk.1");
            List<LppShardRoute> routes = orchestrator.resolveRoutes("sk.0", alloc, Map.of("node-1", state));
            assertThat(routes).isEmpty();
        }

        @Test
        void nodeWithMultipleShardStatesOnlyMatchesCorrectShard() {
            String sk0 = "grocery.idx.0";
            String sk1 = "grocery.idx.1";
            LppShardPlannedAllocation alloc = allocWithNodes(sk0, List.of("node-multi"));

            LppNodeActualState state = new LppNodeActualState("node-multi", "oi", "g1", "INGEST", "zone-a");
            state.setHeartbeatTimestampMs(System.currentTimeMillis());
            state.setShardStates(List.of(
                    shardState(sk0, "ACTIVE"),
                    shardState(sk1, "ACTIVE")
            ));

            List<LppShardRoute> routes = orchestrator.resolveRoutes(sk0, alloc, Map.of("node-multi", state));
            assertThat(routes).hasSize(1);
            assertThat(routes.get(0).getNodeName()).isEqualTo("node-multi");
        }

        @Test
        void failedShardStateDoesNotRoute() {
            String shardKey = "grocery.idx.0";
            LppShardPlannedAllocation alloc = allocWithNodes(shardKey, List.of("node-failed"));
            LppNodeActualState state = nodeWithShardState("node-failed", "g1", shardKey, "FAILED");

            List<LppShardRoute> routes = orchestrator.resolveRoutes(shardKey, alloc, Map.of("node-failed", state));
            assertThat(routes).isEmpty();
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Build a PA map with a single shard key and the given node names. */
    private Map<String, LppShardPlannedAllocation> paWithNodes(String shardKey, String... nodeNames) {
        LppShardPlannedAllocation alloc = allocWithNodes(shardKey, List.of(nodeNames));
        return Map.of(shardKey, alloc);
    }

    private LppShardPlannedAllocation allocWithNodes(String shardKey, List<String> nodeNames) {
        LppShardEntry entry = shardEntryFromKey(shardKey);
        LppShardPlannedAllocation alloc = new LppShardPlannedAllocation(entry);
        alloc.getAssignedNodeNames().addAll(nodeNames);
        return alloc;
    }

    /** Parse a shardKey of the form "collection.fullIndex.shardId" */
    private LppShardEntry shardEntryFromKey(String shardKey) {
        String[] parts = shardKey.split("\\.");
        if (parts.length >= 3) {
            int shardId = Integer.parseInt(parts[parts.length - 1]);
            String collection = parts[0];
            String fullIndex = String.join(".", Arrays.copyOfRange(parts, 1, parts.length - 1));
            return new LppShardEntry(collection, fullIndex, fullIndex, shardId);
        }
        return new LppShardEntry("col", shardKey, shardKey, 0);
    }

    /** Fresh, ACTIVE node with the given shard key marked ACTIVE. */
    private LppNodeActualState activeNode(String nodeName, String grailShardId, String shardKey) {
        LppNodeActualState state = new LppNodeActualState(nodeName, "oi", grailShardId, "INGEST", "zone-a");
        state.setHeartbeatTimestampMs(System.currentTimeMillis());
        state.setShardStates(List.of(shardState(shardKey, "ACTIVE")));
        return state;
    }

    /** Fresh node with a single shard in the given state. */
    private LppNodeActualState nodeWithShardState(String nodeName, String grailShardId, String shardKey, String state) {
        LppNodeActualState as = new LppNodeActualState(nodeName, "oi", grailShardId, "INGEST", "zone-a");
        as.setHeartbeatTimestampMs(System.currentTimeMillis());
        as.setShardStates(List.of(shardState(shardKey, state)));
        return as;
    }

    private LppShardActualState shardState(String shardKey, String state) {
        LppShardActualState ss = new LppShardActualState();
        ss.setShardKey(shardKey);
        ss.setState(state);
        ss.setLastStateChangeMs(System.currentTimeMillis());
        return ss;
    }
}
