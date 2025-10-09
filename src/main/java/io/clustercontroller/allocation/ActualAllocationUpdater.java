package io.clustercontroller.allocation;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.enums.ShardState;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * ActualAllocationUpdater - Responsible for aggregating search unit actual states into index shard actual allocations
 * 
 * This component:
 * 1. Reads actual states from /search-units/<su-name>/actual-state
 * 2. Aggregates this information by index and shard
 * 3. Updates actual allocations at /indices/<index-name>/<shard_id>/actual-allocation
 * 4. Enables convergence logic between planned and actual states for routing decisions
 * 
 * Convergence Strategy:
 * - PA[n3,n4], AA[n1,n2] → routing uses fallback [n1,n2] (old nodes still serving)
 * - PA[n3,n4], AA[n1,n2,n3,n4] → routing uses intersection [n3,n4] (new nodes ready)
 * - PA[n3,n4], AA[n3,n4] → routing optimal, no query failures
 */
@Slf4j
public class ActualAllocationUpdater {
    
    private final MetadataStore metadataStore;
    
    // Timeout for considering a search unit's heartbeat stale (1 minute)
    private static final long STALE_HEARTBEAT_TIMEOUT_MS = 60 * 1000;
    
    public ActualAllocationUpdater(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
    
    /**
     * Main entry point for actual allocation update task
     * Aggregates all search unit actual states and updates actual allocations in etcd
     * 
     * @param clusterId the cluster ID to update actual allocations for
     */
    public void updateActualAllocations(String clusterId) {
        log.info("Starting actual allocation update process for cluster: {}", clusterId);
        
        try {
            // Get all search units to read their actual states
            List<SearchUnit> searchUnits = metadataStore.getAllSearchUnits(clusterId);
            if (searchUnits.isEmpty()) {
                log.info("No search units found for cluster: {}", clusterId);
                return;
            }
            
            // Collect actual state information from all search units
            Map<String, Map<String, Set<String>>> actualAllocations = collectActualAllocations(clusterId, searchUnits);
            
            // Update actual allocation records for each index/shard combination
            int totalUpdates = updateActualAllocationRecords(clusterId, actualAllocations);
            
            // Clean up stale actual allocations that are no longer valid
            cleanupStaleActualAllocations(clusterId, actualAllocations);
            
            log.info("Completed actual allocation update with {} updates for cluster: {}", totalUpdates, clusterId);
            
        } catch (Exception e) {
            log.error("Failed to update actual allocations for cluster {}: {}", clusterId, e.getMessage(), e);
        }
    }
    
    /**
     * Collect actual allocations from all search units
     * Returns: Map<indexName, Map<shardId, Set<unitNames>>>
     * 
     * @param clusterId the cluster ID
     * @param searchUnits list of search units to process
     */
    private Map<String, Map<String, Set<String>>> collectActualAllocations(String clusterId, List<SearchUnit> searchUnits) throws Exception {
        Map<String, Map<String, Set<String>>> actualAllocations = new HashMap<>();
        
        for (SearchUnit searchUnit : searchUnits) {
            String unitName = searchUnit.getName();
            
            try {
                // Skip coordinator nodes - they don't host shards
                if (NodeRole.COORDINATOR.getValue().equalsIgnoreCase(searchUnit.getRole())) {
                    log.debug("Skipping coordinator node: {}", unitName);
                    continue;
                }
                
                // Get actual state for this search unit
                SearchUnitActualState actualState = metadataStore.getSearchUnitActualState(clusterId, unitName);
                if (actualState == null) {
                    log.debug("No actual state found for search unit: {}", unitName);
                    continue;
                }
                
                // Check if timestamp is recent (basic health check)
                if (!isNodeTimestampRecent(actualState, unitName)) {
                    continue;
                }
                
                // Process each index on this search unit using the nodeRouting structure
                Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = actualState.getNodeRouting();
                if (nodeRouting != null) {
                    for (Map.Entry<String, List<SearchUnitActualState.ShardRoutingInfo>> indexEntry : nodeRouting.entrySet()) {
                        String indexName = indexEntry.getKey();
                        List<SearchUnitActualState.ShardRoutingInfo> shards = indexEntry.getValue();
                        
                        // Process each shard for this index
                        if (shards != null) {
                            for (SearchUnitActualState.ShardRoutingInfo shard : shards) {
                                // Only include shards that are in STARTED state
                                if (!ShardState.STARTED.equals(shard.getState())) {
                                    log.debug("Skipping non-STARTED shard {}/{} on unit {} (state: {})", 
                                        indexName, shard.getShardId(), unitName, shard.getState());
                                    continue;
                                }
                                
                                // Use shard ID as string
                                String shardId = String.valueOf(shard.getShardId());
                                actualAllocations
                                    .computeIfAbsent(indexName, k -> new HashMap<>())
                                    .computeIfAbsent(shardId, k -> new HashSet<>())
                                    .add(unitName);
                                
                                log.debug("Found STARTED shard {}/{} (primary: {}) on unit: {}", 
                                    indexName, shardId, shard.isPrimary(), unitName);
                            }
                        }
                    }
                }
                
            } catch (Exception e) {
                log.error("Error processing search unit {}: {}", unitName, e.getMessage(), e);
                // Continue with other search units
            }
        }
        
        log.info("Collected actual allocations for {} indices", actualAllocations.size());
        return actualAllocations;
    }
    
    /**
     * Update actual allocation records based on collected data
     * 
     * @param clusterId the cluster ID
     * @param actualAllocations collected actual allocations
     */
    private int updateActualAllocationRecords(String clusterId, Map<String, Map<String, Set<String>>> actualAllocations) throws Exception {
        int totalUpdates = 0;
        
        for (Map.Entry<String, Map<String, Set<String>>> indexEntry : actualAllocations.entrySet()) {
            String indexName = indexEntry.getKey();
            
            for (Map.Entry<String, Set<String>> shardEntry : indexEntry.getValue().entrySet()) {
                String shardId = shardEntry.getKey();
                Set<String> allocatedUnits = shardEntry.getValue();
                
                try {
                    if (updateShardActualAllocation(clusterId, indexName, shardId, allocatedUnits)) {
                        totalUpdates++;
                    }
                } catch (Exception e) {
                    log.error("Error updating actual allocation for {}/{}: {}", 
                        indexName, shardId, e.getMessage(), e);
                    // Continue with other shards
                }
            }
        }
        
        return totalUpdates;
    }
    
    /**
     * Update actual allocation for a specific shard
     * 
     * @param clusterId the cluster ID
     * @param indexName the index name
     * @param shardId the shard ID
     * @param allocatedUnits set of unit names that have this shard
     */
    private boolean updateShardActualAllocation(String clusterId, String indexName, String shardId, Set<String> allocatedUnits) throws Exception {
        // Get current actual allocation
        ShardAllocation currentActual = metadataStore.getActualAllocation(clusterId, indexName, shardId);
        
        // Separate units by role
        List<String> ingestSUs = new ArrayList<>();
        List<String> searchSUs = new ArrayList<>();
        
        for (String unitName : allocatedUnits) {
            try {
                Optional<SearchUnit> searchUnitOpt = metadataStore.getSearchUnit(clusterId, unitName);
                if (searchUnitOpt.isPresent()) {
                    SearchUnit searchUnit = searchUnitOpt.get();
                    String role = searchUnit.getRole() != null ? searchUnit.getRole().toUpperCase() : "UNKNOWN";
                    
                    if (NodeRole.PRIMARY.getValue().equals(role)) {
                        ingestSUs.add(unitName);
                    } else if (NodeRole.REPLICA.getValue().equals(role)) {
                        searchSUs.add(unitName);
                    } else {
                        log.warn("Unknown role '{}' for search unit: {}, defaulting to REPLICA", role, unitName);
                        searchSUs.add(unitName); // Default to search
                    }
                } else {
                    log.warn("Search unit {} not found in configuration, defaulting to REPLICA", unitName);
                    searchSUs.add(unitName); // Default to search
                }
            } catch (Exception e) {
                log.error("Error determining role for unit {}: {}", unitName, e.getMessage());
                searchSUs.add(unitName); // Default to search on error
            }
        }
        
        // Check if allocation has changed
        if (currentActual != null) {
            Set<String> currentIngestSUs = new HashSet<>(currentActual.getIngestSUs());
            Set<String> currentSearchSUs = new HashSet<>(currentActual.getSearchSUs());
            Set<String> newIngestSUs = new HashSet<>(ingestSUs);
            Set<String> newSearchSUs = new HashSet<>(searchSUs);
            
            if (currentIngestSUs.equals(newIngestSUs) && currentSearchSUs.equals(newSearchSUs)) {
                // No change needed
                log.debug("No change needed for actual allocation {}/{}", indexName, shardId);
                return false;
            }
        }
        
        // Create or update actual allocation
        ShardAllocation actualAllocation = new ShardAllocation(shardId, indexName);
        actualAllocation.setIngestSUs(ingestSUs);
        actualAllocation.setSearchSUs(searchSUs);
        actualAllocation.setAllocationTimestamp(System.currentTimeMillis());
        
        metadataStore.setActualAllocation(clusterId, indexName, shardId, actualAllocation);
        
        log.info("Updated actual allocation for {}/{}: ingest={}, search={}", 
            indexName, shardId, ingestSUs, searchSUs);
        return true;
    }
    
    /**
     * Clean up actual allocations for shards that no longer exist on any search unit
     * Uses the already-collected actual allocation data for efficiency
     * 
     * @param clusterId the cluster ID
     * @param currentActualAllocations collected actual allocations
     */
    private void cleanupStaleActualAllocations(String clusterId, Map<String, Map<String, Set<String>>> currentActualAllocations) throws Exception {
        log.info("Starting cleanup of stale actual allocations for cluster: {}", clusterId);
        
        // Get all index configurations to know what indices exist
        List<Index> indexConfigs = metadataStore.getAllIndexConfigs(clusterId);
        int totalCleanups = 0;
        
        for (Index indexConfig : indexConfigs) {
            String indexName = indexConfig.getIndexName();
            int numberOfShards = indexConfig.getSettings().getNumberOfShards();
            
            try {
                // Check each shard for this index
                for (int shardIndex = 0; shardIndex < numberOfShards; shardIndex++) {
                    String shardId = String.valueOf(shardIndex);
                    
                    // Check if this shard is still actually allocated according to current search unit states
                    boolean stillAllocated = currentActualAllocations.containsKey(indexName) && 
                                           currentActualAllocations.get(indexName).containsKey(shardId) &&
                                           !currentActualAllocations.get(indexName).get(shardId).isEmpty();
                    
                    if (!stillAllocated) {
                        // Check if there's an existing actual allocation record
                        ShardAllocation existingActual = metadataStore.getActualAllocation(clusterId, indexName, shardId);
                        if (existingActual != null && 
                            (!existingActual.getIngestSUs().isEmpty() || !existingActual.getSearchSUs().isEmpty())) {
                            
                            // Clean up stale actual allocation by setting empty lists
                            ShardAllocation emptyAllocation = new ShardAllocation(shardId, indexName);
                            emptyAllocation.setIngestSUs(new ArrayList<>());
                            emptyAllocation.setSearchSUs(new ArrayList<>());
                            emptyAllocation.setAllocationTimestamp(System.currentTimeMillis());
                            
                            metadataStore.setActualAllocation(clusterId, indexName, shardId, emptyAllocation);
                            totalCleanups++;
                            
                            log.info("Cleaned up stale actual allocation for {}/{}", indexName, shardId);
                        }
                    }
                }
                
            } catch (Exception e) {
                log.error("Error cleaning up actual allocations for index {}: {}", 
                    indexName, e.getMessage(), e);
                // Continue with other indexes
            }
        }
        
        log.info("Completed cleanup, removed {} stale actual allocations", totalCleanups);
    }
    
    /**
     * Check if a node has a recent timestamp (< 1 minute old heartbeat)
     */
    private boolean isNodeTimestampRecent(SearchUnitActualState actualState, String unitName) {
        long currentTime = System.currentTimeMillis();
        long nodeTimestamp = actualState.getTimestamp();
        long timeDiff = currentTime - nodeTimestamp;
        
        if (timeDiff > STALE_HEARTBEAT_TIMEOUT_MS) {
            log.debug("Skipping stale search unit: {} (timestamp: {}, age: {}ms)", 
                unitName, nodeTimestamp, timeDiff);
            return false;
        }
        
        return true;
    }
    
    /**
     * Sync coordinator state with search units (placeholder for future implementation)
     * 
     * @param clusterId the cluster ID
     */
    public void syncCoordinatorState(String clusterId) {
        log.info("Syncing coordinator state with search units for cluster: {}", clusterId);
        // TODO: Implement coordinator synchronization logic
        // This would update coordinator routing tables based on actual allocations
    }
    
    /**
     * Handle allocation drift detection and correction (placeholder for future implementation)
     * 
     * @param clusterId the cluster ID
     */
    public void handleAllocationDrift(String clusterId) {
        log.info("Handling allocation drift detection and correction for cluster: {}", clusterId);
        // TODO: Implement allocation drift handling logic
        // This would detect when actual state drifts too far from planned state
        // and trigger corrective actions
    }
}
