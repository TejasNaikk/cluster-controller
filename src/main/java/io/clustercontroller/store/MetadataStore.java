package io.clustercontroller.store;

import io.clustercontroller.models.Index;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.SearchUnitGoalState;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstraction layer for metadata storage supporting different backends (etcd, redis, etc.)
 * This is the main interface for all cluster metadata operations.
 */
public interface MetadataStore {
    
    // =================================================================
    // CONTROLLER TASKS OPERATIONS
    // =================================================================
    
    /**
     * Get all controller tasks sorted by priority
     */
    List<TaskMetadata> getAllTasks() throws Exception;
    
    /**
     * Get task by name
     */
    Optional<TaskMetadata> getTask(String taskName) throws Exception;
    
    /**
     * Create new task
     */
    String createTask(TaskMetadata task) throws Exception;
    
    /**
     * Update existing task
     */
    void updateTask(TaskMetadata task) throws Exception;
    
    /**
     * Delete task
     */
    void deleteTask(String taskName) throws Exception;
    
    /**
     * Delete old completed tasks (cleanup)
     */
    void deleteOldTasks(long olderThanTimestamp) throws Exception;
    
    // =================================================================
    // SEARCH UNITS OPERATIONS
    // =================================================================
    
    /**
     * Get all search units
     */
    List<SearchUnit> getAllSearchUnits() throws Exception;
    
    /**
     * Get search unit by name
     */
    Optional<SearchUnit> getSearchUnit(String unitName) throws Exception;
    
    /**
     * Create or update search unit
     */
    void upsertSearchUnit(String unitName, SearchUnit searchUnit) throws Exception;
    
    /**
     * Update search unit
     */
    void updateSearchUnit(SearchUnit searchUnit) throws Exception;
    
    /**
     * Delete search unit
     */
    void deleteSearchUnit(String unitName) throws Exception;
    
    // =================================================================
    // SEARCH UNIT STATE OPERATIONS (for discovery)
    // =================================================================
    
    /**
     * Get all search unit actual states (for discovery)
     */
    Map<String, SearchUnitActualState> getAllSearchUnitActualStates() throws Exception;
    
    /**
     * Get search unit goal state
     */
    Optional<SearchUnitGoalState> getSearchUnitGoalState(String unitName) throws Exception;
    
    /**
     * Get search unit actual state
     */
    Optional<SearchUnitActualState> getSearchUnitActualState(String unitName) throws Exception;
    
    /**
     * Update search unit goal state
     */
    void updateSearchUnitGoalState(String unitName, SearchUnitGoalState goalState) throws Exception;
    // =================================================================
    // INDEX CONFIGURATIONS OPERATIONS
    // =================================================================
    
    /**
     * Get all index configurations
     */
    List<Index> getAllIndexConfigs() throws Exception;
    
    /**
     * Get index configuration by name
     */
    Optional<String> getIndexConfig(String indexName) throws Exception;
    
    /**
     * Create new index configuration
     */
    String createIndexConfig(Index index) throws Exception;
    
    /**
     * Update index configuration
     */
    void updateIndexConfig(Index index) throws Exception;
    
    /**
     * Delete index configuration
     */
    void deleteIndexConfig(String indexName) throws Exception;
    
    /**
     * Set index mappings
     */
    void setIndexMappings(String indexName, String mappings) throws Exception;
    
    /**
     * Set index settings
     */
    void setIndexSettings(String indexName, String settings) throws Exception;
    
    // =================================================================
    // SHARD ALLOCATION OPERATIONS
    // =================================================================
    
    /**
     * Get all planned allocations for an index
     */
    List<ShardAllocation> getAllPlannedAllocations(String indexName) throws Exception;
    
    /**
     * Delete a planned allocation for a specific shard
     */
    void deletePlannedAllocation(String indexName, String shardId) throws Exception;
    
    // =================================================================
    // CLUSTER OPERATIONS
    // =================================================================
    
    /**
     * Initialize/setup the metadata store
     */
    void initialize() throws Exception;
    
    /**
     * Close/cleanup the metadata store
     */
    void close() throws Exception;
    
    /**
     * Get the cluster name this metadata store is connected to
     */
    String getClusterName();
}
