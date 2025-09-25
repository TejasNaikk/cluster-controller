package io.clustercontroller.models;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Simple model for planned shard allocation.
 */
@Data
@NoArgsConstructor
public class ShardAllocation {

    private String shardId;
    private String indexName;
    private List<String> searchUnitNames;

    public ShardAllocation(String shardId, String indexName, List<String> searchUnitNames) {
        this.shardId = shardId;
        this.indexName = indexName;
        this.searchUnitNames = searchUnitNames;
    }
}