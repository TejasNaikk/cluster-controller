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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LppTasksTest {

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
    }

    // ---- Discovery task ----

    @Test
    void discoveryTaskSucceeds() {
        when(discovery.discover()).thenReturn(Map.of("g1", new LppGroup("g1", "z", "INGEST")));

        LppDiscoveryTask task = new LppDiscoveryTask(ctx);
        String result = task.execute();

        assertThat(result).isEqualTo("SUCCESS");
        assertThat(ctx.getCurrentGroups()).containsKey("g1");
    }

    @Test
    void discoveryTaskReturnsFailed() {
        when(discovery.discover()).thenThrow(new RuntimeException("Grail down"));

        String result = new LppDiscoveryTask(ctx).execute();
        assertThat(result).isEqualTo("FAILED");
    }

    // ---- Allocation task ----

    @Test
    void allocationTaskSkipsWhenNoGroups() {
        // groups are empty by default in ctx
        String result = new LppAllocationTask(ctx).execute();
        assertThat(result).isEqualTo("SKIPPED");
        verify(allocator, never()).allocate(any(), any());
    }

    @Test
    void allocationTaskSkipsWhenNoIndices() {
        ctx.setCurrentGroups(Map.of("g1", new LppGroup("g1", "z", "INGEST")));
        when(metadataStore.getAllIndexDefinitions()).thenReturn(List.of());

        String result = new LppAllocationTask(ctx).execute();
        assertThat(result).isEqualTo("SKIPPED");
    }

    @Test
    void allocationTaskSucceedsAndStoresAllocations() {
        ctx.setCurrentGroups(Map.of("g1", new LppGroup("g1", "z", "INGEST")));
        when(metadataStore.getAllIndexDefinitions()).thenReturn(
                List.of(new LppIndexDefinition("grocery", "idx", "idx.1", 2, 1)));

        LppShardPlannedAllocation a0 = allocationFor("grocery", "idx", 0);
        LppShardPlannedAllocation a1 = allocationFor("grocery", "idx", 1);
        when(allocator.allocate(any(), any())).thenReturn(
                Map.of("grocery.idx.0", a0, "grocery.idx.1", a1));

        String result = new LppAllocationTask(ctx).execute();

        assertThat(result).isEqualTo("SUCCESS");
        assertThat(ctx.getCurrentAllocations()).hasSize(2);
    }

    // ---- Orchestration task ----

    @Test
    void orchestrationTaskSkipsWhenNoAllocations() {
        String result = new LppOrchestrationTask(ctx).execute();
        assertThat(result).isEqualTo("SKIPPED");
        verify(orchestrator, never()).orchestrate(any(), any(), any());
    }

    @Test
    void orchestrationTaskReturnsConvergedWhenAllNodesInSync() {
        ctx.setCurrentAllocations(Map.of("k", allocationFor("grocery", "idx", 0)));
        when(orchestrator.orchestrate(any(), any(), any())).thenReturn(List.of());

        String result = new LppOrchestrationTask(ctx).execute();
        assertThat(result).isEqualTo("CONVERGED");
    }

    @Test
    void orchestrationTaskReturnsSuccessWhenNodeUpdated() {
        ctx.setCurrentAllocations(Map.of("k", allocationFor("grocery", "idx", 0)));
        when(orchestrator.orchestrate(any(), any(), any())).thenReturn(List.of("node-1"));

        String result = new LppOrchestrationTask(ctx).execute();
        assertThat(result).isEqualTo("SUCCESS");
    }

    // ---- State aggregation task ----

    @Test
    void stateAggregationSucceeds() {
        when(aggregator.aggregate()).thenReturn(
                new LppStateAggregator.AggregationResult(5, List.of(), List.of()));

        String result = new LppStateAggregationTask(ctx).execute();
        assertThat(result).isEqualTo("SUCCESS");
    }

    @Test
    void stateAggregationReturnsFailedOnException() {
        when(aggregator.aggregate()).thenThrow(new RuntimeException("etcd failure"));

        String result = new LppStateAggregationTask(ctx).execute();
        assertThat(result).isEqualTo("FAILED");
    }

    // -------------------------------------------------------------------------

    private LppShardPlannedAllocation allocationFor(String collection, String index, int shardId) {
        LppShardEntry entry = new LppShardEntry(collection, index, index + ".1", shardId);
        return new LppShardPlannedAllocation(entry);
    }
}
