package io.clustercontroller.indices;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.Index;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.ShardData;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.store.MetadataStore;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages index lifecycle operations.
 * Internal component used by TaskManager.
 */
@Slf4j
public class IndexManager {
    
    private final MetadataStore metadataStore;
    private final ObjectMapper objectMapper;
    
    public IndexManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.objectMapper = new ObjectMapper();
    }
    
    public void createIndex(String indexConfig) throws Exception {
        log.info("Creating index with config: {}", indexConfig);
        
        // Parse the JSON input to extract index configuration
        CreateIndexRequest request = parseCreateIndexRequest(indexConfig);
        
        log.info("CreateIndex - Parsed index name: {}", request.getIndexName());
        
        // Validate the parsed input
        if (request.getIndexName() == null || request.getIndexName().isEmpty()) {
            throw new Exception("Index name cannot be null or empty");
        }
        
        // Check if index already exists
        if (metadataStore.getIndexConfig(request.getIndexName()).isPresent()) {
            log.info("CreateIndex - Index '{}' already exists, skipping creation", request.getIndexName());
            return;
        }
        
        // Get all available search units for allocation planning
        List<SearchUnit> availableUnits = metadataStore.getAllSearchUnits();
        
        if (availableUnits.isEmpty()) {
            throw new Exception("No search units available for index allocation");
        }
        
        // TODO: Determine number of shards from settings, adding default 1 shard for now
        int numberOfShards = 1;
        
        // TODO: Calculate maximum replicas per shard based on available search units
        List<Integer> shardReplicaCount = new ArrayList<>();
        shardReplicaCount.add(1);
        
        log.info("CreateIndex - Using {} shards with replica count: {}", numberOfShards, shardReplicaCount);
        
        // TODO: Create allocation plan
        List<ShardData> allocationPlan = new ArrayList<>();
        
        // Create the new Index configuration
        Index newIndex = new Index();
        newIndex.setIndexName(request.getIndexName());
        newIndex.setShardReplicaCount(shardReplicaCount);
        newIndex.setAllocationPlan(allocationPlan);
        
        // Store the index configuration
        String documentId = metadataStore.createIndexConfig(newIndex);
        log.info("CreateIndex - Successfully created index configuration for '{}' with document ID: {}", 
            newIndex.getIndexName(), documentId);
        
        // Store mappings if provided
        if (request.getMappings() != null && !request.getMappings().trim().isEmpty()) {
            metadataStore.setIndexMappings(request.getIndexName(), request.getMappings());
            log.info("CreateIndex - Set mappings for index '{}'", request.getIndexName());
        }
        
        // Store settings if provided
        if (request.getSettings() != null && !request.getSettings().trim().isEmpty()) {
            metadataStore.setIndexSettings(request.getIndexName(), request.getSettings());
            log.info("CreateIndex - Set settings for index '{}'", request.getIndexName());
        }
        
    }
    
    public void deleteIndex(String indexConfig) throws Exception {
        log.info("Deleting index with config: {}", indexConfig);
        // TODO: Implement index deletion logic
    }
    
    public void planShardAllocation() throws Exception {
        log.info("Planning shard allocation");
        // TODO: Implement shard allocation planning logic
    }

    private CreateIndexRequest parseCreateIndexRequest(String input) throws Exception {
        return objectMapper.readValue(input, CreateIndexRequest.class);
    }

    /**
     * Data class to hold parsed create index request
     */
    @Data
    @NoArgsConstructor
    private static class CreateIndexRequest {
        @JsonProperty("index_name")
        private String indexName;

        @JsonProperty("mappings")
        private String mappings; // Optional mappings JSON
        
        @JsonProperty("settings")
        private String settings; // Optional settings JSON
    }
}