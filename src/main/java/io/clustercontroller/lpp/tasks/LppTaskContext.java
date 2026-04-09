package io.clustercontroller.lpp.tasks;

import io.clustercontroller.lpp.allocation.LppShardAllocator;
import io.clustercontroller.lpp.discovery.LppDiscovery;
import io.clustercontroller.lpp.models.LppGroup;
import io.clustercontroller.lpp.models.LppIndexDefinition;
import io.clustercontroller.lpp.models.LppShardPlannedAllocation;
import io.clustercontroller.lpp.orchestration.LppGoalStateOrchestrator;
import io.clustercontroller.lpp.orchestration.LppRoutingTableOrchestrator;
import io.clustercontroller.lpp.orchestration.LppStateAggregator;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * Shared context for all LPP tasks — equivalent to OS controller's {@link io.clustercontroller.tasks.TaskContext}.
 *
 * <p>Holds stateless (or cluster-agnostic) LPP services. The namespace/region are
 * the LPP equivalents of clusterId and region.
 */
@Getter
@Slf4j
public class LppTaskContext {

    private final LppDiscovery discovery;
    private final LppShardAllocator allocator;
    private final LppGoalStateOrchestrator orchestrator;
    private final LppRoutingTableOrchestrator routingTableOrchestrator;
    private final LppStateAggregator stateAggregator;
    private final LppMetadataStore metadataStore;
    private final String namespace;
    private final String region;

    // In-memory state shared across tasks within a single cycle
    private volatile Map<String, LppGroup> currentIngestGroups = Map.of();
    private volatile Map<String, LppGroup> currentSearchGroups = Map.of();
    private volatile Map<String, LppShardPlannedAllocation> currentAllocations = Map.of();

    public LppTaskContext(
            LppDiscovery discovery,
            LppShardAllocator allocator,
            LppGoalStateOrchestrator orchestrator,
            LppRoutingTableOrchestrator routingTableOrchestrator,
            LppStateAggregator stateAggregator,
            LppMetadataStore metadataStore,
            String namespace,
            String region) {
        this.discovery = discovery;
        this.allocator = allocator;
        this.orchestrator = orchestrator;
        this.routingTableOrchestrator = routingTableOrchestrator;
        this.stateAggregator = stateAggregator;
        this.metadataStore = metadataStore;
        this.namespace = namespace;
        this.region = region;
    }

    public void setCurrentIngestGroups(Map<String, LppGroup> groups) {
        this.currentIngestGroups = Map.copyOf(groups);
    }

    public void setCurrentSearchGroups(Map<String, LppGroup> groups) {
        this.currentSearchGroups = Map.copyOf(groups);
    }

    public void setCurrentAllocations(Map<String, LppShardPlannedAllocation> allocations) {
        this.currentAllocations = Map.copyOf(allocations);
    }

    public List<LppIndexDefinition> loadIndexDefinitions() {
        return metadataStore.getAllIndexDefinitions();
    }
}
