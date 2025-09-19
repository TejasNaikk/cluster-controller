package io.clustercontroller.indices;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.Index;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.ShardData;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.store.MetadataStore;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    
    public void deleteIndex(String input) throws Exception {
        log.info("IndexShard - Deleting index from input: {}", input);
        
        // Parse the JSON input to extract delete index configuration
        DeleteIndexRequest request = parseDeleteIndexRequest(input);
        
        log.info("DeleteIndex - Parsed index name: {}", request.getIndexName());
        
        // Validate the parsed input
        if (request.getIndexName() == null || request.getIndexName().isEmpty()) {
            throw new Exception("Index name cannot be null or empty");
        }
        
        String indexName = request.getIndexName();
        
        // Check if index exists
        List<Index> allIndices = metadataStore.getAllIndexConfigs();
        Index targetIndex = allIndices.stream()
            .filter(index -> indexName.equals(index.getIndexName()))
            .findFirst()
            .orElse(null);
            
        if (targetIndex == null) {
            log.warn("DeleteIndex - Index '{}' not found, nothing to delete", indexName);
            return;
        }
        
        // Step 1: Remove all planned allocations for this index
        deleteAllPlannedAllocationsFromIndex(indexName);
        
        // Step 2: Delete the index configuration from etcd
        metadataStore.deleteIndexConfig(indexName);
        log.info("DeleteIndex - Successfully deleted index configuration for '{}'", indexName);
        
        // Step 3: Clean up goal states for this deleted index
        cleanupGoalStatesForDeletedIndex(indexName);
        
        log.info("DeleteIndex - Index '{}' deletion completed.", indexName);
    }
    
    public void planShardAllocation() throws Exception {
        log.info("Planning shard allocation");
        // TODO: Implement shard allocation planning logic
    }

    private CreateIndexRequest parseCreateIndexRequest(String input) throws Exception {
        return objectMapper.readValue(input, CreateIndexRequest.class);
    }
    
    private DeleteIndexRequest parseDeleteIndexRequest(String input) throws Exception {
        return objectMapper.readValue(input, DeleteIndexRequest.class);
    }
    
    /**
     * Delete all planned allocations from an index
     */
    private void deleteAllPlannedAllocationsFromIndex(String indexName) throws Exception {
        log.info("DeleteIndex - Cleaning up ALL planned allocations from index '{}'", indexName);
        
        try {
            List<ShardAllocation> plannedAllocations = metadataStore.getAllPlannedAllocations(indexName);
            
            for (ShardAllocation allocation : plannedAllocations) {
                try {
                    metadataStore.deletePlannedAllocation(indexName, allocation.getShardId());
                } catch (Exception e) {
                    log.error("DeleteIndex - Failed to delete planned allocation {}/{}: {}", 
                        indexName, allocation.getShardId(), e.getMessage());
                }
            }
            
            log.info("DeleteIndex - Cleaned up {} planned allocations from index '{}'", 
                plannedAllocations.size(), indexName);
                
        } catch (Exception e) {
            log.error("DeleteIndex - Failed to cleanup planned allocations from index '{}': {}", 
                indexName, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Cleans up goal states (local shards) for an index from all search units
     */
    private void cleanupGoalStatesForDeletedIndex(String deletedIndexName) throws Exception {
        log.info("DeleteIndex - Starting immediate goal state cleanup for deleted index '{}'", deletedIndexName);
        
        try {
            // Get all search units to check their goal states
            List<SearchUnit> allSearchUnits = metadataStore.getAllSearchUnits();
            
            for (SearchUnit searchUnit : allSearchUnits) {
                String unitName = searchUnit.getName();
                
                try {
                    // Get current goal state for this unit
                    Optional<SearchUnitGoalState> goalStateOpt = metadataStore.getSearchUnitGoalState(unitName);
                    
                    if (!goalStateOpt.isPresent()) {
                        log.debug("DeleteIndex - No goal state found for unit '{}'", unitName);
                        continue;
                    }
                    
                    SearchUnitGoalState goalState = goalStateOpt.get();
                    
                    // Check if this unit has the deleted index in its goal state
                    if (!goalState.getLocalShards().containsKey(deletedIndexName)) {
                        log.debug("DeleteIndex - Unit '{}' does not have deleted index '{}' in goal state", 
                            unitName, deletedIndexName);
                        continue;
                    }
                    
                    // Remove the deleted index from goal state
                    Map<String, Map<String, String>> localShards = goalState.getLocalShards();
                    localShards.remove(deletedIndexName);
                    
                    // Save updated goal state
                    metadataStore.updateSearchUnitGoalState(unitName, goalState);
                    
                    log.info("DeleteIndex - Removed deleted index '{}' from goal state of unit '{}'", 
                        deletedIndexName, unitName);
                        
                } catch (Exception e) {
                    log.error("DeleteIndex - Failed to cleanup goal state for unit '{}': {}", 
                        unitName, e.getMessage(), e);
                }
            }
            
        } catch (Exception e) {
            log.error("DeleteIndex - Failed to get search units for goal state cleanup: {}", 
                e.getMessage(), e);
            throw e;
        }
        
        log.info("DeleteIndex - Completed immediate goal state cleanup for deleted index '{}'", 
            deletedIndexName);
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
    
    /**
     * Data class to hold parsed delete index request
     */
    @Data
    @NoArgsConstructor
    private static class DeleteIndexRequest {
        @JsonProperty("index_name")
        private String indexName;
    }
}