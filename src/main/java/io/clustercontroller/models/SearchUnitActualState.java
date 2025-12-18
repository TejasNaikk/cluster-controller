package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.config.Constants;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.ShardState;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchUnitActualState {

    @JsonProperty("nodeName")
    private String nodeName;
    
    @JsonProperty("address") 
    private String address;
    
    @JsonProperty("httpPort")
    private int httpPort;
    
    @JsonProperty("transportPort")
    private int transportPort;
    
    @JsonProperty("nodeId")
    private String nodeId;
    
    @JsonProperty("ephemeralId")
    private String ephemeralId;
    
    // Resource usage metrics
    @JsonProperty("memoryUsedMB")
    private long memoryUsedMB;
    
    @JsonProperty("memoryMaxMB")
    private long memoryMaxMB;
    
    @JsonProperty("memoryUsedPercent")
    private int memoryUsedPercent;
    
    @JsonProperty("heapUsedMB")
    private long heapUsedMB;
    
    @JsonProperty("heapMaxMB")
    private long heapMaxMB;
    
    @JsonProperty("heapUsedPercent")
    private int heapUsedPercent;
    
    @JsonProperty("diskTotalMB")
    private long diskTotalMB;
    
    @JsonProperty("diskAvailableMB")
    private long diskAvailableMB;
    
    @JsonProperty("cpuUsedPercent")
    private int cpuUsedPercent;
    
    // Heartbeat and timing
    @JsonProperty("heartbeatIntervalMillis")
    private long heartbeatIntervalMillis;
    
    @JsonProperty("timestamp")
    private long timestamp;
    
    // Shard routing information - the key part for controller logic
    @JsonProperty("nodeRouting")
    private Map<String, List<ShardRoutingInfo>> nodeRouting; // index-name -> list of shard routing info
    
    // Node role and shard information (populated by worker)
    @JsonProperty("clusterlessRole")
    private String role; // "PRIMARY", "SEARCH_REPLICA", "COORDINATOR"
    
    @JsonProperty("clusterlessShardId")
    private String shardId; // "shard-1", "shard-2", etc.
    
    @JsonProperty("cluster_name")
    private String clusterName; // "search-cluster", "analytics-cluster", etc.
    
    // Stats reported by the dataplane (doc counts, etc.)
    @JsonProperty("stats")
    private IndicesStats stats;
    
    public SearchUnitActualState() {
        this.nodeRouting = new HashMap<>();
    }
    
    // ========== UTILITY METHODS ==========
    
    /**
     * Determine if the search unit is healthy based on node state
     */
    public boolean isHealthy() {
        // Consider node healthy if memory and disk usage are reasonable
        // TODO: come up with more comprehensive health check logic
        return memoryUsedPercent < Constants.HEALTH_CHECK_MEMORY_THRESHOLD_PERCENT 
            && diskAvailableMB > Constants.HEALTH_CHECK_DISK_THRESHOLD_MB;
    }
    
    /**
     * Determines the admin state of this search unit based on its health status.
     * 
     * @return NORMAL if the unit is healthy, DRAIN if unhealthy
     */
    public String deriveAdminState() {
        return isHealthy() ? Constants.ADMIN_STATE_NORMAL : Constants.ADMIN_STATE_DRAIN;
    }
    
    /**
     * Derive node state directly as the final status representation
     * Determined by the health of the node AND the presence of active shards
     * TODO: Determine if we should report RED/YELLOW/GREEN from os directly instead of deriving it from the node state
     * Returns: GREEN (healthy+active), YELLOW (healthy+inactive), RED (unhealthy)
     */
    public HealthState deriveNodeState() {
        // First check if node is healthy based on resource usage
        if (!isHealthy()) {
            return HealthState.RED;
        }
        
        // Then check routing info for active shards
        if (nodeRouting != null && !nodeRouting.isEmpty()) {
            boolean hasActiveShards = nodeRouting.values().stream()
                    .flatMap(List::stream)
                    .anyMatch(routing -> ShardState.STARTED.equals(routing.getState()));
            return hasActiveShards ? HealthState.GREEN : HealthState.YELLOW;
        }
        
        // If healthy but no routing info (e.g., coordinator nodes), consider it green/active
        return HealthState.GREEN;
    }

    
    /**
     * Shard routing information for a single shard on this node
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShardRoutingInfo {
        @JsonProperty("shardId")
        private int shardId;
        
        @JsonProperty("role")
        private String role; // "primary", "search_replica", "replica"
        
        @JsonProperty("state")
        private ShardState state; // e.g., STARTED, INITIALIZING, RELOCATING
        
        @JsonProperty("relocating")
        private boolean relocating;
        
        @JsonProperty("relocatingNodeId")
        private String relocatingNodeId; // Target node ID when relocating
        
        @JsonProperty("allocationId")
        private String allocationId;
        
        @JsonProperty("currentNodeId")
        private String currentNodeId;
        
        @JsonProperty("currentNodeName")
        private String currentNodeName;
        
        public ShardRoutingInfo() {}
        
        public ShardRoutingInfo(int shardId, String role, ShardState state) {
            this.shardId = shardId;
            this.role = role;
            this.state = state;
            this.relocating = false;
        }
        
        /**
         * Check if this shard is a primary shard
         * @return true if role is "primary", false otherwise
         */
        public boolean isPrimary() {
            return role != null && Constants.ROLE_PRIMARY.equalsIgnoreCase(role);
        }
    }
    
    /**
     * Stats reported by the dataplane containing index and shard level metrics
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IndicesStats {
        @JsonProperty("indices")
        private IndicesContainer indices;
    }
    
    /**
     * Container for index-level stats
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IndicesContainer {
        @JsonProperty("docs")
        private DocsStats docs;
        
        @JsonProperty("shards")
        private Map<String, List<Map<String, ShardLevelStats>>> shards; // indexName -> list of shard stats
    }
    
    /**
     * Document count statistics
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DocsStats {
        @JsonProperty("count")
        private long count;
        
        @JsonProperty("deleted")
        private long deleted;
    }
    
    /**
     * Shard-level statistics including docs and sequence numbers
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShardLevelStats {
        @JsonProperty("docs")
        private DocsStats docs;
        
        @JsonProperty("seq_no")
        private SeqNoStats seqNo;
    }
    
    /**
     * Sequence number stats for tracking replication progress
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SeqNoStats {
        @JsonProperty("max_seq_no")
        private long maxSeqNo;
        
        @JsonProperty("local_checkpoint")
        private long localCheckpoint;
        
        @JsonProperty("global_checkpoint")
        private long globalCheckpoint;
    }
    
    // ========== UTILITY METHODS FOR STATS ==========
    
    /**
     * Get the document count for a specific index and shard from the stats.
     * 
     * @param indexName the name of the index
     * @param shardId the shard ID
     * @return the document count, or -1 if not found
     */
    public long getShardDocCount(String indexName, int shardId) {
        if (stats == null || stats.getIndices() == null || stats.getIndices().getShards() == null) {
            return -1;
        }
        
        List<Map<String, ShardLevelStats>> indexShards = stats.getIndices().getShards().get(indexName);
        if (indexShards == null) {
            return -1;
        }
        
        String shardIdStr = String.valueOf(shardId);
        for (Map<String, ShardLevelStats> shardMap : indexShards) {
            ShardLevelStats shardStats = shardMap.get(shardIdStr);
            if (shardStats != null && shardStats.getDocs() != null) {
                return shardStats.getDocs().getCount();
            }
        }
        
        return -1;
    }
    
    /**
     * Check if a shard's data is fully replicated (global checkpoint equals local checkpoint).
     * 
     * @param indexName the name of the index
     * @param shardId the shard ID
     * @return true if replicated, false if not or if data not available
     */
    public boolean isShardReplicated(String indexName, int shardId) {
        if (stats == null || stats.getIndices() == null || stats.getIndices().getShards() == null) {
            return false;
        }
        
        List<Map<String, ShardLevelStats>> indexShards = stats.getIndices().getShards().get(indexName);
        if (indexShards == null) {
            return false;
        }
        
        String shardIdStr = String.valueOf(shardId);
        for (Map<String, ShardLevelStats> shardMap : indexShards) {
            ShardLevelStats shardStats = shardMap.get(shardIdStr);
            if (shardStats != null && shardStats.getSeqNo() != null) {
                SeqNoStats seqNo = shardStats.getSeqNo();
                return seqNo.getGlobalCheckpoint() == seqNo.getLocalCheckpoint();
            }
        }
        
        return false;
    }
} 