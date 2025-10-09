package io.clustercontroller.allocation;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.enums.ShardState;
import io.clustercontroller.models.*;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for ActualAllocationUpdater.
 */
class ActualAllocationUpdaterTest {
    
    @Mock
    private MetadataStore metadataStore;
    
    private ActualAllocationUpdater actualAllocationUpdater;
    private String clusterId = "test-cluster";
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        actualAllocationUpdater = new ActualAllocationUpdater(metadataStore);
    }
    
    @Test
    void testUpdateActualAllocations_NoSearchUnits() throws Exception {
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(Collections.emptyList());
        
        // Should not throw exception
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        verify(metadataStore).getAllSearchUnits(clusterId);
        verifyNoMoreInteractions(metadataStore);
    }
    
    @Test
    void testUpdateActualAllocations_SingleSearchUnit() throws Exception {
        // Setup test data
        SearchUnit searchUnit = createSearchUnit("node1", NodeRole.REPLICA.getValue());
        SearchUnitActualState actualState = createActualState("node1", "test-index", 0, ShardState.STARTED, false);
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(searchUnit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        when(metadataStore.getSearchUnit(clusterId, "node1")).thenReturn(Optional.of(searchUnit));
        when(metadataStore.getActualAllocation(clusterId, "test-index", "0")).thenReturn(null);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        verify(metadataStore).getAllSearchUnits(clusterId);
        verify(metadataStore).getSearchUnitActualState(clusterId, "node1");
        verify(metadataStore).setActualAllocation(eq(clusterId), eq("test-index"), eq("0"), any(ShardAllocation.class));
    }
    
    @Test
    void testUpdateActualAllocations_MultipleSearchUnits() throws Exception {
        // Setup test data - one primary, one replica
        SearchUnit primaryUnit = createSearchUnit("primary-node", NodeRole.PRIMARY.getValue());
        SearchUnit replicaUnit = createSearchUnit("replica-node", NodeRole.REPLICA.getValue());
        
        SearchUnitActualState primaryState = createActualState("primary-node", "test-index", 0, ShardState.STARTED, true);
        SearchUnitActualState replicaState = createActualState("replica-node", "test-index", 0, ShardState.STARTED, false);
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(primaryUnit, replicaUnit));
        when(metadataStore.getSearchUnitActualState(clusterId, "primary-node")).thenReturn(primaryState);
        when(metadataStore.getSearchUnitActualState(clusterId, "replica-node")).thenReturn(replicaState);
        when(metadataStore.getSearchUnit(clusterId, "primary-node")).thenReturn(Optional.of(primaryUnit));
        when(metadataStore.getSearchUnit(clusterId, "replica-node")).thenReturn(Optional.of(replicaUnit));
        when(metadataStore.getActualAllocation(clusterId, "test-index", "0")).thenReturn(null);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        verify(metadataStore).setActualAllocation(eq(clusterId), eq("test-index"), eq("0"), argThat(allocation -> 
            allocation.getIngestSUs().contains("primary-node") && 
            allocation.getSearchSUs().contains("replica-node")
        ));
    }
    
    @Test
    void testUpdateActualAllocations_SkipCoordinatorNodes() throws Exception {
        // Setup test data - coordinator node should be skipped
        SearchUnit coordinatorUnit = createSearchUnit("coordinator-node", NodeRole.COORDINATOR.getValue());
        SearchUnit dataUnit = createSearchUnit("data-node", NodeRole.REPLICA.getValue());
        
        SearchUnitActualState dataState = createActualState("data-node", "test-index", 0, ShardState.STARTED, false);
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(coordinatorUnit, dataUnit));
        when(metadataStore.getSearchUnitActualState(clusterId, "data-node")).thenReturn(dataState);
        when(metadataStore.getSearchUnit(clusterId, "data-node")).thenReturn(Optional.of(dataUnit));
        when(metadataStore.getActualAllocation(clusterId, "test-index", "0")).thenReturn(null);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        // Coordinator node should not be queried for actual state
        verify(metadataStore, never()).getSearchUnitActualState(clusterId, "coordinator-node");
        verify(metadataStore).getSearchUnitActualState(clusterId, "data-node");
    }
    
    @Test
    void testUpdateActualAllocations_SkipNonStartedShards() throws Exception {
        // Setup test data - shard is INITIALIZING, should be skipped
        SearchUnit searchUnit = createSearchUnit("node1", NodeRole.REPLICA.getValue());
        SearchUnitActualState actualState = createActualState("node1", "test-index", 0, ShardState.INITIALIZING, false);
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(searchUnit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        // Should not create actual allocation for non-STARTED shards
        verify(metadataStore, never()).setActualAllocation(anyString(), anyString(), anyString(), any(ShardAllocation.class));
    }
    
    @Test
    void testUpdateActualAllocations_SkipStaleHeartbeat() throws Exception {
        // Setup test data - stale timestamp
        SearchUnit searchUnit = createSearchUnit("node1", NodeRole.REPLICA.getValue());
        SearchUnitActualState actualState = createActualState("node1", "test-index", 0, ShardState.STARTED, false);
        
        // Set timestamp to 2 minutes ago (stale)
        actualState.setTimestamp(System.currentTimeMillis() - (2 * 60 * 1000));
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(searchUnit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        // Should not create actual allocation for stale nodes
        verify(metadataStore, never()).setActualAllocation(anyString(), anyString(), anyString(), any(ShardAllocation.class));
    }
    
    @Test
    void testUpdateActualAllocations_NoChangeNeeded() throws Exception {
        // Setup test data
        SearchUnit searchUnit = createSearchUnit("node1", NodeRole.REPLICA.getValue());
        SearchUnitActualState actualState = createActualState("node1", "test-index", 0, ShardState.STARTED, false);
        
        // Existing actual allocation matches current state
        ShardAllocation existingAllocation = new ShardAllocation("0", "test-index");
        existingAllocation.setSearchSUs(List.of("node1"));
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(searchUnit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        when(metadataStore.getSearchUnit(clusterId, "node1")).thenReturn(Optional.of(searchUnit));
        when(metadataStore.getActualAllocation(clusterId, "test-index", "0")).thenReturn(existingAllocation);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        // Should not update if allocation hasn't changed
        verify(metadataStore, never()).setActualAllocation(anyString(), anyString(), anyString(), any(ShardAllocation.class));
    }
    
    @Test
    void testUpdateActualAllocations_CleanupStaleAllocations() throws Exception {
        // Setup test data
        SearchUnit searchUnit = createSearchUnit("node1", NodeRole.REPLICA.getValue());
        SearchUnitActualState actualState = createActualState("node1", "test-index", 0, ShardState.STARTED, false);
        
        // Index config with 2 shards
        Index indexConfig = createIndexConfig("test-index", 2);
        
        // Existing stale allocation for shard 1 (not on any node anymore)
        ShardAllocation staleAllocation = new ShardAllocation("1", "test-index");
        staleAllocation.setSearchSUs(List.of("old-node"));
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(searchUnit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        when(metadataStore.getSearchUnit(clusterId, "node1")).thenReturn(Optional.of(searchUnit));
        when(metadataStore.getActualAllocation(clusterId, "test-index", "0")).thenReturn(null);
        when(metadataStore.getActualAllocation(clusterId, "test-index", "1")).thenReturn(staleAllocation);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(List.of(indexConfig));
        
        actualAllocationUpdater.updateActualAllocations(clusterId);
        
        // Should clean up stale allocation for shard 1
        verify(metadataStore).setActualAllocation(eq(clusterId), eq("test-index"), eq("1"), argThat(allocation ->
            allocation.getIngestSUs().isEmpty() && allocation.getSearchSUs().isEmpty()
        ));
    }
    
    // Helper methods
    
    private SearchUnit createSearchUnit(String name, String role) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole(role);
        return unit;
    }
    
    private SearchUnitActualState createActualState(String nodeName, String indexName, int shardId, ShardState state, boolean primary) {
        SearchUnitActualState actualState = new SearchUnitActualState();
        actualState.setNodeName(nodeName);
        actualState.setTimestamp(System.currentTimeMillis()); // Recent timestamp
        
        // Create shard routing info
        SearchUnitActualState.ShardRoutingInfo shardInfo = new SearchUnitActualState.ShardRoutingInfo();
        shardInfo.setShardId(shardId);
        shardInfo.setPrimary(primary);
        shardInfo.setState(state);
        
        // Add to node routing
        Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = new HashMap<>();
        nodeRouting.put(indexName, List.of(shardInfo));
        actualState.setNodeRouting(nodeRouting);
        
        return actualState;
    }
    
    private Index createIndexConfig(String indexName, int numberOfShards) {
        Index index = new Index();
        index.setIndexName(indexName);
        
        IndexSettings settings = new IndexSettings();
        settings.setNumberOfShards(numberOfShards);
        settings.setShardReplicaCount(Collections.nCopies(numberOfShards, 1));
        index.setSettings(settings);
        
        return index;
    }
}

