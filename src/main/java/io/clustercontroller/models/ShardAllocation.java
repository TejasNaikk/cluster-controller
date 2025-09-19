package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a planned shard allocation for a specific shard
 */
@Data
@NoArgsConstructor
public class ShardAllocation {
    
    @JsonProperty("index_name")
    private String indexName;
    
    @JsonProperty("shard_id")
    private String shardId;
    
    @JsonProperty("target_nodes")
    private List<String> targetNodes = new ArrayList<>();
    
    @JsonProperty("allocation_type")
    private String allocationType; // e.g., "PRIMARY", "REPLICA"
    
    @JsonProperty("status")
    private String status; // e.g., "PLANNED", "IN_PROGRESS", "COMPLETED"
    
    @JsonProperty("created_at")
    private String createdAt = java.time.OffsetDateTime.now().toString();
    
    @JsonProperty("updated_at")
    private String updatedAt = java.time.OffsetDateTime.now().toString();
    
    public ShardAllocation(String indexName, String shardId) {
        this.indexName = indexName;
        this.shardId = shardId;
    }
    
    // Custom setter to maintain null safety
    public void setTargetNodes(List<String> targetNodes) {
        this.targetNodes = targetNodes != null ? targetNodes : new ArrayList<>();
    }
}
