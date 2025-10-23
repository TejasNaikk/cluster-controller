package io.clustercontroller.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.ShardState;
import io.clustercontroller.models.ClusterHealthInfo;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class ClusterHealthManagerTest {

    @Mock
    private MetadataStore metadataStore;

    private ClusterHealthManager clusterHealthManager;
    
    private final String testClusterId = "test-cluster";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        clusterHealthManager = new ClusterHealthManager(metadataStore);
    }

    @Test
    void testGetClusterHealth_EmptyCluster() throws Exception {
        // Given - empty cluster
        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(Collections.emptyMap());
        when(metadataStore.getAllIndexConfigs(testClusterId))
            .thenReturn(Collections.emptyList());

        // When
        String healthJson = clusterHealthManager.getClusterHealth(testClusterId, "cluster");
        ClusterHealthInfo health = objectMapper.readValue(healthJson, ClusterHealthInfo.class);

        // Then
        assertThat(health.getClusterName()).isEqualTo(testClusterId);
        assertThat(health.getStatus()).isEqualTo(HealthState.RED); // No active nodes = RED
        assertThat(health.getNumberOfNodes()).isEqualTo(0);
        assertThat(health.getNumberOfDataNodes()).isEqualTo(0);
        assertThat(health.getActiveNodes()).isEqualTo(0);
        assertThat(health.getNumberOfIndices()).isEqualTo(0);
        assertThat(health.getTotalShards()).isEqualTo(0);
    }

    @Test
    void testGetClusterHealth_WithHealthyNodes() throws Exception {
        // Given - healthy cluster with nodes
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createHealthyDataNode("node1");
        actualStates.put("node1", node1);

        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getAllIndexConfigs(testClusterId))
            .thenReturn(Collections.emptyList());

        // When
        String healthJson = clusterHealthManager.getClusterHealth(testClusterId, "cluster");
        ClusterHealthInfo health = objectMapper.readValue(healthJson, ClusterHealthInfo.class);

        // Then
        assertThat(health.getClusterName()).isEqualTo(testClusterId);
        assertThat(health.getStatus()).isEqualTo(HealthState.GREEN);
        assertThat(health.getNumberOfNodes()).isEqualTo(1);
        assertThat(health.getNumberOfDataNodes()).isEqualTo(1);
        assertThat(health.getActiveNodes()).isEqualTo(1);
    }

    @Test
    void testGetClusterHealth_WithIndicesLevel() throws Exception {
        // Given - cluster with index
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createNodeWithShards("node1", "test-index", 0, true, ShardState.STARTED);
        actualStates.put("node1", node1);

        Index index = createIndex("test-index", 1, Arrays.asList(1));
        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getAllIndexConfigs(testClusterId))
            .thenReturn(Arrays.asList(index));

        // When
        String healthJson = clusterHealthManager.getClusterHealth(testClusterId, "indices");
        ClusterHealthInfo health = objectMapper.readValue(healthJson, ClusterHealthInfo.class);

        // Then
        assertThat(health.getIndices()).isNotNull();
        assertThat(health.getIndices()).containsKey("test-index");
        assertThat(health.getIndices().get("test-index").getShards()).isNull(); // shards not included at 'indices' level
    }

    @Test
    void testGetClusterHealth_WithShardsLevel() throws Exception {
        // Given - cluster with shards
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createNodeWithShards("node1", "test-index", 0, true, ShardState.STARTED);
        actualStates.put("node1", node1);

        Index index = createIndex("test-index", 1, Arrays.asList(1));
        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getAllIndexConfigs(testClusterId))
            .thenReturn(Arrays.asList(index));

        // When
        String healthJson = clusterHealthManager.getClusterHealth(testClusterId, "shards");
        ClusterHealthInfo health = objectMapper.readValue(healthJson, ClusterHealthInfo.class);

        // Then
        assertThat(health.getIndices()).isNotNull();
        assertThat(health.getIndices()).containsKey("test-index");
        assertThat(health.getIndices().get("test-index").getShards()).isNotNull();
        assertThat(health.getIndices().get("test-index").getShards()).isNotEmpty();
    }

    @Test
    void testGetIndexHealth_Success() throws Exception {
        // Given
        String indexName = "test-index";
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createNodeWithShards("node1", indexName, 0, true, ShardState.STARTED);
        actualStates.put("node1", node1);

        Index index = createIndex(indexName, 1, Arrays.asList(1));
        String indexJson = objectMapper.writeValueAsString(index);

        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getIndexConfig(testClusterId, indexName))
            .thenReturn(Optional.of(indexJson));

        // When
        String healthJson = clusterHealthManager.getIndexHealth(testClusterId, indexName, "indices");

        // Then
        assertThat(healthJson).isNotNull();
    }

    @Test
    void testGetIndexHealth_IndexNotFound() throws Exception {
        // Given
        String indexName = "non-existent-index";
        when(metadataStore.getIndexConfig(testClusterId, indexName))
            .thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(Collections.emptyMap());

        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.getIndexHealth(testClusterId, indexName, "indices"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Index '" + indexName + "' not found");
    }

    @Test
    void testGetClusterStats_NotImplemented() {
        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.getClusterStats(testClusterId))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster stats not yet implemented");
    }

    // Helper methods to create test data
    private SearchUnitActualState createHealthyDataNode(String nodeName) {
        SearchUnitActualState state = new SearchUnitActualState();
        state.setNodeName(nodeName);
        state.setRole("REPLICA");
        // Set resource metrics that make isHealthy() return true
        // isHealthy() checks: memoryUsedPercent < 90 && diskAvailableMB > 1024
        state.setMemoryUsedMB(1000);
        state.setMemoryMaxMB(4000);
        state.setMemoryUsedPercent(25); // 25% < 90%
        state.setHeapUsedMB(500);
        state.setHeapMaxMB(2000);
        state.setHeapUsedPercent(25);
        state.setDiskTotalMB(10000);
        state.setDiskAvailableMB(5000); // 5000 > 1024
        state.setCpuUsedPercent(30);
        return state;
    }

    private SearchUnitActualState createNodeWithShards(String nodeName, String indexName, 
                                                       int shardId, boolean isPrimary, ShardState state) {
        SearchUnitActualState node = createHealthyDataNode(nodeName);
        
        SearchUnitActualState.ShardRoutingInfo shardInfo = new SearchUnitActualState.ShardRoutingInfo();
        shardInfo.setShardId(shardId);
        shardInfo.setPrimary(isPrimary);
        shardInfo.setState(state);
        
        Map<String, List<SearchUnitActualState.ShardRoutingInfo>> routing = new HashMap<>();
        routing.put(indexName, Arrays.asList(shardInfo));
        node.setNodeRouting(routing);
        
        return node;
    }

    private Index createIndex(String name, int numShards, List<Integer> replicaCounts) {
        Index index = new Index();
        index.setIndexName(name);
        
        IndexSettings settings = new IndexSettings();
        settings.setNumberOfShards(numShards);
        settings.setShardReplicaCount(replicaCounts);
        index.setSettings(settings);
        
        return index;
    }
}