package io.clustercontroller.api.handlers;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.*;
import io.clustercontroller.store.MetadataStore;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * REST controller providing visualizer endpoints for the cluster dashboard.
 * Multi-cluster aware - accepts clusterId as path parameter.
 */
@RestController
@RequestMapping("/visualizer")
@Component
public class VisualizerHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(VisualizerHandler.class);
    private final MetadataStore metadataStore;
    private final String defaultClusterId;
    
    public VisualizerHandler(MetadataStore metadataStore, io.clustercontroller.config.ClusterControllerConfig config) {
        this.metadataStore = metadataStore;
        this.defaultClusterId = config.getClusterName();
    }
    
    /**
     * Get all available clusters
     */
    @GetMapping("/clusters")
    public ClusterList getClusters() {
        try {
            ClusterList result = new ClusterList();
            result.clusters = metadataStore.getAllClusters();
            result.defaultCluster = defaultClusterId;
            return result;
        } catch (Exception e) {
            logger.error("Error getting clusters: {}", e.getMessage(), e);
            ClusterList errorResult = new ClusterList();
            errorResult.clusters = List.of(defaultClusterId);
            errorResult.defaultCluster = defaultClusterId;
            return errorResult;
        }
    }
    
    /**
     * Get complete cluster overview
     */
    @GetMapping("/{clusterId}/cluster")
    public ClusterOverview getClusterOverview(@PathVariable("clusterId") String clusterId) {
        try {
            ClusterOverview overview = new ClusterOverview();
            overview.clusterName = clusterId;
            
            // Get search units and indices
            List<SearchUnit> searchUnits = metadataStore.getAllSearchUnits(clusterId);
            List<Index> indices = metadataStore.getAllIndexConfigs(clusterId);
            
            // Convert to view models
            overview.searchUnits = searchUnits.stream()
                .map(this::createSearchUnitView)
                .collect(Collectors.toList());
            
            overview.indices = indices.stream()
                .map(this::createIndexView)
                .collect(Collectors.toList());
            
            // Calculate stats
            overview.stats = calculateClusterStats(searchUnits, indices);
            
            return overview;
            
        } catch (Exception e) {
            logger.error("Error getting cluster overview: {}", e.getMessage(), e);
            ClusterOverview errorOverview = new ClusterOverview();
            errorOverview.clusterName = "unknown";
            errorOverview.stats = new ClusterStats();
            errorOverview.searchUnits = new ArrayList<>();
            errorOverview.indices = new ArrayList<>();
            return errorOverview;
        }
    }
    
    /**
     * Get allocation matrix
     */
    @GetMapping("/{clusterId}/allocation-matrix")
    public AllocationMatrix getAllocationMatrix(@PathVariable("clusterId") String clusterId) {
        try {
            AllocationMatrix matrix = new AllocationMatrix();
            
            List<SearchUnit> searchUnits = metadataStore.getAllSearchUnits(clusterId);
            List<Index> indices = metadataStore.getAllIndexConfigs(clusterId);
            
            matrix.searchUnits = searchUnits.stream()
                .map(SearchUnit::getName)
                .collect(Collectors.toList());
            
            matrix.indices = indices.stream()
                .map(Index::getIndexName)
                .collect(Collectors.toList());
            
            matrix.allocations = new HashMap<>();
            
            // Build allocation matrix
            for (Index index : indices) {
                String indexName = index.getIndexName();
                Map<String, List<String>> indexAllocations = new HashMap<>();
                
                int numberOfShards = index.getSettings().getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    String shardId = String.valueOf(i);
                    
                    try {
                        ShardAllocation planned = metadataStore.getPlannedAllocation(clusterId, indexName, shardId);
                        if (planned != null) {
                            // Add ingest SUs
                            for (String unitName : planned.getIngestSUs()) {
                                indexAllocations.computeIfAbsent(unitName, k -> new ArrayList<>())
                                    .add(shardId + "p");
                            }
                            // Add search SUs
                            for (String unitName : planned.getSearchSUs()) {
                                indexAllocations.computeIfAbsent(unitName, k -> new ArrayList<>())
                                    .add(shardId + "r");
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Error getting allocation for {}/{}: {}", indexName, shardId, e.getMessage());
                    }
                }
                
                matrix.allocations.put(indexName, indexAllocations);
            }
            
            return matrix;
            
        } catch (Exception e) {
            logger.error("Error getting allocation matrix: {}", e.getMessage(), e);
            AllocationMatrix errorMatrix = new AllocationMatrix();
            errorMatrix.searchUnits = new ArrayList<>();
            errorMatrix.indices = new ArrayList<>();
            errorMatrix.allocations = new HashMap<>();
            return errorMatrix;
        }
    }
    
    /**
     * Get search unit detail
     */
    @GetMapping("/{clusterId}/search-unit/{unitName}")
    public SearchUnitDetail getSearchUnitDetail(
            @PathVariable("clusterId") String clusterId,
            @PathVariable("unitName") String unitName) {
        try {
            SearchUnitDetail detail = new SearchUnitDetail();
            detail.name = unitName;
            
            // Get configuration
            Optional<SearchUnit> unitOpt = metadataStore.getSearchUnit(clusterId, unitName);
            if (unitOpt.isPresent()) {
                SearchUnit unit = unitOpt.get();
                detail.configuration = unit;
            }
            
            // Get goal state
            try {
                SearchUnitGoalState goalState = metadataStore.getSearchUnitGoalState(clusterId, unitName);
                detail.goalState = goalState;
            } catch (Exception e) {
                logger.warn("Error getting goal state for {}: {}", unitName, e.getMessage());
            }
            
            // Get actual state
            try {
                SearchUnitActualState actualState = metadataStore.getSearchUnitActualState(clusterId, unitName);
                detail.actualState = actualState;
            } catch (Exception e) {
                logger.warn("Error getting actual state for {}: {}", unitName, e.getMessage());
            }
            
            return detail;
            
        } catch (Exception e) {
            logger.error("Error getting search unit detail for {}: {}", unitName, e.getMessage(), e);
            SearchUnitDetail errorDetail = new SearchUnitDetail();
            errorDetail.name = unitName;
            return errorDetail;
        }
    }
    
    /**
     * Get index detail
     */
    @GetMapping("/{clusterId}/index/{indexName}")
    public IndexDetail getIndexDetail(
            @PathVariable("clusterId") String clusterId,
            @PathVariable("indexName") String indexName) {
        try {
            IndexDetail detail = new IndexDetail();
            detail.name = indexName;
            
            // Get index config
            Optional<String> configOpt = metadataStore.getIndexConfig(clusterId, indexName);
            if (configOpt.isPresent()) {
                // Parse and set config
                detail.configuration = configOpt.get();
            }
            
            // Get all planned allocations for this index
            List<Index> indices = metadataStore.getAllIndexConfigs(clusterId);
            Optional<Index> indexOpt = indices.stream()
                .filter(idx -> idx.getIndexName().equals(indexName))
                .findFirst();
            
            if (indexOpt.isPresent()) {
                Index index = indexOpt.get();
                int numberOfShards = index.getSettings().getNumberOfShards();
                
                detail.plannedAllocations = new ArrayList<>();
                detail.actualAllocations = new ArrayList<>();
                
                for (int i = 0; i < numberOfShards; i++) {
                    String shardId = String.valueOf(i);
                    
                    try {
                        ShardAllocation planned = metadataStore.getPlannedAllocation(clusterId, indexName, shardId);
                        if (planned != null) {
                            detail.plannedAllocations.add(planned);
                        }
                    } catch (Exception e) {
                        logger.warn("Error getting planned allocation: {}", e.getMessage());
                    }
                    
                    try {
                        ShardAllocation actual = metadataStore.getActualAllocation(clusterId, indexName, shardId);
                        if (actual != null) {
                            detail.actualAllocations.add(actual);
                        }
                    } catch (Exception e) {
                        logger.warn("Error getting actual allocation: {}", e.getMessage());
                    }
                }
            }
            
            return detail;
            
        } catch (Exception e) {
            logger.error("Error getting index detail for {}: {}", indexName, e.getMessage(), e);
            IndexDetail errorDetail = new IndexDetail();
            errorDetail.name = indexName;
            return errorDetail;
        }
    }
    
    /**
     * Get controller tasks
     */
    @GetMapping("/{clusterId}/tasks")
    public List<TaskView> getTasks(@PathVariable("clusterId") String clusterId) {
        try {
            List<TaskMetadata> tasks = metadataStore.getAllTasks(clusterId);
            
            return tasks.stream()
                .map(this::createTaskView)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("Error getting tasks: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }
    
    // Helper methods
    
    private SearchUnitView createSearchUnitView(SearchUnit unit) {
        SearchUnitView view = new SearchUnitView();
        view.name = unit.getName();
        view.host = unit.getHost();
        view.role = unit.getRole();
        view.stateAdmin = unit.getStateAdmin();
        view.statePulled = unit.getStatePulled() != null ? unit.getStatePulled().name() : "UNKNOWN";
        view.shardId = unit.getShardId();
        view.zone = unit.getZone();
        
        // Create ports info
        Map<String, Integer> ports = new HashMap<>();
        ports.put("http", unit.getPortHttp());
        ports.put("transport", unit.getPortTransport());
        view.ports = ports;
        
        return view;
    }
    
    private IndexView createIndexView(Index index) {
        IndexView view = new IndexView();
        view.name = index.getIndexName();
        view.shardCount = index.getSettings().getNumberOfShards();
        view.shardReplicaCount = index.getSettings().getShardReplicaCount();
        view.health = "GREEN"; // Default, should be calculated
        view.allocatedShards = 0; // Should be calculated
        return view;
    }
    
    private TaskView createTaskView(TaskMetadata task) {
        TaskView view = new TaskView();
        view.name = task.getName();
        view.priority = task.getPriority();
        view.status = task.getStatus();
        view.schedule = task.getSchedule();
        view.lastUpdated = task.getLastUpdated() != null ? task.getLastUpdated().toString() : null;
        view.input = task.getInput();
        return view;
    }
    
    private ClusterStats calculateClusterStats(List<SearchUnit> searchUnits, List<Index> indices) {
        ClusterStats stats = new ClusterStats();
        stats.totalSearchUnits = searchUnits.size();
        stats.totalIndices = indices.size();
        stats.totalShards = indices.stream()
            .mapToInt(idx -> idx.getSettings().getNumberOfShards())
            .sum();
        
        // Count by state
        stats.searchUnitsByState = searchUnits.stream()
            .collect(Collectors.groupingBy(
                unit -> unit.getStateAdmin() != null ? unit.getStateAdmin() : "UNKNOWN",
                Collectors.counting()
            ));
        
        return stats;
    }
    
    // View models
    
    @Data
    public static class ClusterList {
        @JsonProperty("clusters")
        public List<String> clusters;
        
        @JsonProperty("default_cluster")
        public String defaultCluster;
    }
    
    @Data
    public static class ClusterOverview {
        @JsonProperty("cluster_name")
        public String clusterName;
        
        @JsonProperty("search_units")
        public List<SearchUnitView> searchUnits;
        
        @JsonProperty("indices")
        public List<IndexView> indices;
        
        @JsonProperty("stats")
        public ClusterStats stats;
    }
    
    @Data
    public static class ClusterStats {
        @JsonProperty("total_search_units")
        public int totalSearchUnits;
        
        @JsonProperty("total_indices")
        public int totalIndices;
        
        @JsonProperty("total_shards")
        public int totalShards;
        
        @JsonProperty("search_units_by_state")
        public Map<String, Long> searchUnitsByState;
    }
    
    @Data
    public static class SearchUnitView {
        @JsonProperty("name")
        public String name;
        
        @JsonProperty("host")
        public String host;
        
        @JsonProperty("role")
        public String role;
        
        @JsonProperty("state_admin")
        public String stateAdmin;
        
        @JsonProperty("state_pulled")
        public String statePulled;
        
        @JsonProperty("node_state")
        public String nodeState;
        
        @JsonProperty("shard_id")
        public String shardId;
        
        @JsonProperty("zone")
        public String zone;
        
        @JsonProperty("ports")
        public Map<String, Integer> ports;
        
        @JsonProperty("index_count")
        public int indexCount;
        
        @JsonProperty("shard_count")
        public int shardCount;
    }
    
    @Data
    public static class IndexView {
        @JsonProperty("name")
        public String name;
        
        @JsonProperty("shard_count")
        public int shardCount;
        
        @JsonProperty("shard_replica_count")
        public List<Integer> shardReplicaCount;
        
        @JsonProperty("health")
        public String health;
        
        @JsonProperty("allocated_shards")
        public int allocatedShards;
    }
    
    @Data
    public static class TaskView {
        @JsonProperty("name")
        public String name;
        
        @JsonProperty("priority")
        public int priority;
        
        @JsonProperty("status")
        public String status;
        
        @JsonProperty("schedule")
        public String schedule;
        
        @JsonProperty("last_updated")
        public String lastUpdated;
        
        @JsonProperty("input")
        public String input;
    }
    
    @Data
    public static class AllocationMatrix {
        @JsonProperty("search_units")
        public List<String> searchUnits;
        
        @JsonProperty("indices")
        public List<String> indices;
        
        @JsonProperty("allocations")
        public Map<String, Map<String, List<String>>> allocations;
    }
    
    @Data
    public static class SearchUnitDetail {
        @JsonProperty("name")
        public String name;
        
        @JsonProperty("configuration")
        public SearchUnit configuration;
        
        @JsonProperty("goal_state")
        public SearchUnitGoalState goalState;
        
        @JsonProperty("actual_state")
        public SearchUnitActualState actualState;
        
        @JsonProperty("convergence_status")
        public Map<String, Object> convergenceStatus;
    }
    
    @Data
    public static class IndexDetail {
        @JsonProperty("name")
        public String name;
        
        @JsonProperty("configuration")
        public String configuration;
        
        @JsonProperty("planned_allocations")
        public List<ShardAllocation> plannedAllocations;
        
        @JsonProperty("actual_allocations")
        public List<ShardAllocation> actualAllocations;
    }
}

