package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Goal state for data node search units.
 * Specifies which shards should be assigned to this node and their roles.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchUnitGoalState {
    
    /**
     * Data node local shards: index-name -> shard-id -> role
     * Role is a simple string: "PRIMARY" or "SEARCH_REPLICA"
     */
    @JsonProperty("local_shards")
    private Map<String, Map<String, String>> localShards;
    
    @JsonProperty("last_updated")
    private String lastUpdated;
    
    @JsonProperty("version")
    private long version;
    
    public SearchUnitGoalState() {
        this.localShards = new HashMap<>();
        this.version = 1;
    }
    
    public Map<String, Map<String, String>> getLocalShards() {
        return localShards;
    }
    
    public void setLocalShards(Map<String, Map<String, String>> localShards) {
        this.localShards = localShards != null ? localShards : new HashMap<>();
    }
    
    public String getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public long getVersion() {
        return version;
    }
    
    public void setVersion(long version) {
        this.version = version;
    }
    
    // ========== UTILITY METHODS ==========
    
    /**
     * Check if this goal state contains a specific index
     */
    public boolean hasIndex(String indexName) {
        return localShards != null && localShards.containsKey(indexName);
    }
    
    /**
     * Get list of shard IDs for a specific index
     * @return List of shard IDs, or empty list if index not found
     */
    public java.util.List<String> getShardsForIndex(String indexName) {
        if (!hasIndex(indexName)) {
            return new java.util.ArrayList<>();
        }
        Map<String, String> shards = localShards.get(indexName);
        return shards != null ? new java.util.ArrayList<>(shards.keySet()) : new java.util.ArrayList<>();
    }
    
    /**
     * Get the role for a specific shard in an index
     * @param indexName The index name
     * @param shardId The shard ID
     * @return The role ("PRIMARY" or "SEARCH_REPLICA"), or null if not found
     */
    public String getShardRole(String indexName, String shardId) {
        if (!hasIndex(indexName)) {
            return null;
        }
        Map<String, String> shards = localShards.get(indexName);
        return shards != null ? shards.get(shardId) : null;
    }
    
    @Override
    public String toString() {
        return "SearchUnitGoalState{" +
                "localShards=" + localShards +
                ", lastUpdated='" + lastUpdated + '\'' +
                ", version=" + version +
                '}';
    }
    
    /**
     * Compares goal states based on localShards content only.
     * lastUpdated and version are metadata fields that don't affect the actual goal state.
     * This is used to avoid unnecessary writes to etcd when the goal state hasn't actually changed.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchUnitGoalState that = (SearchUnitGoalState) o;
        
        // Compare localShards content - this is the actual goal state
        if (localShards == null && that.localShards == null) return true;
        if (localShards == null || that.localShards == null) return false;
        
        return localShards.equals(that.localShards);
    }
    
    @Override
    public int hashCode() {
        return localShards != null ? localShards.hashCode() : 0;
    }
    
    /**
     * Check if a specific index/shard with role already exists in the goal state.
     * This is used to determine if an update is actually needed.
     */
    public boolean hasShardWithRole(String indexName, String shardId, String role) {
        if (localShards == null) return false;
        Map<String, String> shards = localShards.get(indexName);
        if (shards == null) return false;
        String existingRole = shards.get(shardId);
        return role.equals(existingRole);
    }
}
