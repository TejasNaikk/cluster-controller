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
            assertThat(table.getRoutes()).isEmpty();
        }

        @Test
        void primaryPathNodeInPaAndActive() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1");
            Map<String, LppNodeActualState> aa = Map.of("node-1", activeNode("node-1", "g1", shardKey));

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).containsExactly("node-1");
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

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).containsExactlyInAnyOrder("node-1", "node-2", "node-3");
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

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).containsExactly("node-1");
        }

        @Test
        void primaryPathExcludesNodeWithNoActualState() {
            // node-1 in PA but no actual state in etcd (not yet discovered)
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1");
            Map<String, LppNodeActualState> aa = Map.of();

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            // Falls through to fallback — empty because no node is ACTIVE anywhere
            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).isEmpty();
        }

        @Test
        void fallbackActivatesWhenPaIntersectionEmpty() {
            // PA has node-1 (not yet active), but node-2 (not in PA) is already ACTIVE
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-1");
            Map<String, LppNodeActualState> aa = Map.of(
                    "node-1", nodeWithShardState("node-1", "g1", shardKey, "DOWNLOADING"),
                    "node-2", activeNode("node-2", "g2", shardKey) // not in PA
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).containsExactly("node-2");
        }

        @Test
        void fallbackPicksMultipleActiveNodesNotInPa() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-new");
            Map<String, LppNodeActualState> aa = Map.of(
                    "node-new", nodeWithShardState("node-new", "g2", shardKey, "DOWNLOADING"),
                    "node-old-1", activeNode("node-old-1", "g1", shardKey),
                    "node-old-2", activeNode("node-old-2", "g1", shardKey)
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).containsExactlyInAnyOrder("node-old-1", "node-old-2");
        }

        @Test
        void staleNodesExcludedFromPrimaryPath() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-stale");
            LppNodeActualState stale = activeNode("node-stale", "g1", shardKey);
            stale.setHeartbeatTimestampMs(0L);

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, Map.of("node-stale", stale), 0);

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).isEmpty();
        }

        @Test
        void staleNodesExcludedFromFallback() {
            String shardKey = "grocery.deals.v1.0";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey, "node-planned");
            LppNodeActualState stale = activeNode("node-stale", "g2", shardKey);
            stale.setHeartbeatTimestampMs(0L);

            Map<String, LppNodeActualState> aa = Map.of(
                    "node-planned", nodeWithShardState("node-planned", "g1", shardKey, "PENDING"),
                    "node-stale", stale
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).isEmpty();
        }

        @Test
        void nodeWithDifferentShardActiveDoesNotCountForThisShard() {
            String shardKey0 = "grocery.deals.v1.0";
            String shardKey1 = "grocery.deals.v1.1";
            Map<String, LppShardPlannedAllocation> pa = paWithNodes(shardKey0, "node-1");
            // node-1 has shard 1 ACTIVE, but not shard 0
            Map<String, LppNodeActualState> aa = Map.of("node-1", activeNode("node-1", "g1", shardKey1));

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            List<String> nodes = table.getRoutes().get("deals.v1").get("0");
            assertThat(nodes).isEmpty();
        }

        @Test
        void multipleShardsOfSameIndexMappedIndependently() {
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

            Map<String, List<String>> shards = table.getRoutes().get("deals.v1");
            assertThat(shards).containsKeys("0", "1");
            assertThat(shards.get("0")).containsExactly("node-a");
            assertThat(shards.get("1")).containsExactly("node-b");
        }

        @Test
        void multipleIndicesMappedSeparately() {
            String sk0 = "grocery.deals.v1.0";
            String sk1 = "grocery.local.v2.0";
            Map<String, LppShardPlannedAllocation> pa = new LinkedHashMap<>();
            pa.putAll(paWithNodes(sk0, "node-a"));
            pa.putAll(paWithNodes(sk1, "node-b"));

            Map<String, LppNodeActualState> aa = Map.of(
                    "node-a", activeNode("node-a", "g1", sk0),
                    "node-b", activeNode("node-b", "g2", sk1)
            );

            LppRoutingTable table = orchestrator.buildRoutingTable(pa, aa, 0);

            assertThat(table.getRoutes()).containsKeys("deals.v1", "local.v2");
            assertThat(table.getRoutes().get("deals.v1").get("0")).containsExactly("node-a");
            assertThat(table.getRoutes().get("local.v2").get("0")).containsExactly("node-b");
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
            newTable.setShardNodes("deals.v1", 0, List.of("n1"));
            assertThat(orchestrator.hasChanged(null, newTable)).isTrue();
        }

        @Test
        void nullExistingWithEmptyNewIsNotChanged() {
            assertThat(orchestrator.hasChanged(null, new LppRoutingTable(0))).isFalse();
        }

        @Test
        void identicalTablesAreNotChanged() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setShardNodes("deals.v1", 0, List.of("n1"));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setShardNodes("deals.v1", 0, List.of("n1"));
            assertThat(orchestrator.hasChanged(t1, t2)).isFalse();
        }

        @Test
        void addedShardIsDetectedAsChange() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setShardNodes("deals.v1", 0, List.of("n1"));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setShardNodes("deals.v1", 0, List.of("n1"));
            t2.setShardNodes("deals.v1", 1, List.of("n2"));
            assertThat(orchestrator.hasChanged(t1, t2)).isTrue();
        }

        @Test
        void addedIndexIsDetectedAsChange() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setShardNodes("deals.v1", 0, List.of("n1"));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setShardNodes("deals.v1", 0, List.of("n1"));
            t2.setShardNodes("local.v2", 0, List.of("n2"));
            assertThat(orchestrator.hasChanged(t1, t2)).isTrue();
        }

        @Test
        void removedNodeInShardIsDetectedAsChange() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setShardNodes("deals.v1", 0, List.of("n1", "n2"));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setShardNodes("deals.v1", 0, List.of("n1"));
            assertThat(orchestrator.hasChanged(t1, t2)).isTrue();
        }

        @Test
        void replacedNodeInShardIsDetectedAsChange() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setShardNodes("deals.v1", 0, List.of("n1"));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setShardNodes("deals.v1", 0, List.of("n2"));
            assertThat(orchestrator.hasChanged(t1, t2)).isTrue();
        }

        @Test
        void orderChangeWithSameNodesIsNotChanged() {
            // hasChanged is set-based, order doesn't matter
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setShardNodes("deals.v1", 0, List.of("n1", "n2"));
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setShardNodes("deals.v1", 0, List.of("n2", "n1"));
            assertThat(orchestrator.hasChanged(t1, t2)).isFalse();
        }

        @Test
        void emptyNodeListIsNotChanged() {
            LppRoutingTable t1 = new LppRoutingTable(1);
            t1.setShardNodes("deals.v1", 0, List.of());
            LppRoutingTable t2 = new LppRoutingTable(1);
            t2.setShardNodes("deals.v1", 0, List.of());
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
            existing.setShardNodes("deals.v1", 0, List.of("old-node"));
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
            existing.setShardNodes("deals.v1", 0, List.of("node-1"));
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
            when(metadataStore.getAllNodeActualStates()).thenReturn(Map.of(
                    "node-planned", nodeWithShardState("node-planned", "g1", shardKey, "DOWNLOADING"),
                    "node-fallback", activeNode("node-fallback", "g2", shardKey)
            ));

            orchestrator.computeAndPersist(paWithNodes(shardKey, "node-planned"));

            ArgumentCaptor<LppRoutingTable> captor = ArgumentCaptor.forClass(LppRoutingTable.class);
            verify(metadataStore).putRoutingTable(captor.capture());
            List<String> nodes = captor.getValue().getRoutes().get("deals.v1").get("0");
            assertThat(nodes).containsExactly("node-fallback");
        }
    }

    // =========================================================================
    // resolveNodes — edge cases
    // =========================================================================

    @Nested
    class ResolveNodes {

        @Test
        void emptyPaAndEmptyAaProducesEmptyList() {
            LppShardPlannedAllocation alloc = allocWithNodes("sk", List.of());
            List<String> nodes = orchestrator.resolveNodes("sk", alloc, Map.of());
            assertThat(nodes).isEmpty();
        }

        @Test
        void onlyPaNodeNotInAaProducesEmptyList() {
            LppShardPlannedAllocation alloc = allocWithNodes("sk", List.of("node-1"));
            List<String> nodes = orchestrator.resolveNodes("sk", alloc, Map.of());
            assertThat(nodes).isEmpty();
        }

        @Test
        void paNodeActiveForDifferentShardDoesNotRoute() {
            LppShardPlannedAllocation alloc = allocWithNodes("sk.0", List.of("node-1"));
            LppNodeActualState state = activeNode("node-1", "g1", "sk.1");
            List<String> nodes = orchestrator.resolveNodes("sk.0", alloc, Map.of("node-1", state));
            assertThat(nodes).isEmpty();
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

            List<String> nodes = orchestrator.resolveNodes(sk0, alloc, Map.of("node-multi", state));
            assertThat(nodes).containsExactly("node-multi");
        }

        @Test
        void failedShardStateDoesNotRoute() {
            String shardKey = "grocery.idx.0";
            LppShardPlannedAllocation alloc = allocWithNodes(shardKey, List.of("node-failed"));
            LppNodeActualState state = nodeWithShardState("node-failed", "g1", shardKey, "FAILED");

            List<String> nodes = orchestrator.resolveNodes(shardKey, alloc, Map.of("node-failed", state));
            assertThat(nodes).isEmpty();
        }

        @Test
        void primaryReturnsOnlyNodeNamesNotMetadata() {
            String shardKey = "grocery.deals.v1.0";
            LppShardPlannedAllocation alloc = allocWithNodes(shardKey, List.of("node-1", "node-2"));
            LppNodeActualState s1 = activeNode("node-1", "g1", shardKey);
            LppNodeActualState s2 = activeNode("node-2", "g1", shardKey);

            List<String> nodes = orchestrator.resolveNodes(shardKey, alloc, Map.of("node-1", s1, "node-2", s2));
            assertThat(nodes).containsExactlyInAnyOrder("node-1", "node-2");
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

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

    private LppNodeActualState activeNode(String nodeName, String grailShardId, String shardKey) {
        LppNodeActualState state = new LppNodeActualState(nodeName, "oi", grailShardId, "INGEST", "zone-a");
        state.setHeartbeatTimestampMs(System.currentTimeMillis());
        state.setShardStates(List.of(shardState(shardKey, "ACTIVE")));
        return state;
    }

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
