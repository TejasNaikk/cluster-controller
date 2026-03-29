package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Planned allocation for a single shard — which groups (and thus nodes) should serve it.
 *
 * <p>Stored in etcd at /lpp/{env}/shards/{shardKey}/planned-allocation.
 * Written by LppShardAllocator, read by LppGoalStateOrchestrator to build per-node manifests.
 */
@Data
@NoArgsConstructor
public class LppShardPlannedAllocation {

    @JsonProperty("shard_key")
    private String shardKey;

    @JsonProperty("collection")
    private String collection;

    @JsonProperty("index_name")
    private String indexName;

    @JsonProperty("full_index_name")
    private String fullIndexName;

    @JsonProperty("shard_id")
    private int shardId;

    /** Group IDs assigned to serve this shard. */
    @JsonProperty("assigned_group_ids")
    private List<String> assignedGroupIds = new ArrayList<>();

    /** Node names (flattened from groups) assigned to serve this shard. */
    @JsonProperty("assigned_node_names")
    private List<String> assignedNodeNames = new ArrayList<>();

    @JsonProperty("last_updated_ms")
    private long lastUpdatedMs;

    public LppShardPlannedAllocation(LppShardEntry entry) {
        this.shardKey = entry.getKey();
        this.collection = entry.getCollection();
        this.indexName = entry.getIndexName();
        this.fullIndexName = entry.getFullIndexName();
        this.shardId = entry.getShardId();
        this.lastUpdatedMs = System.currentTimeMillis();
    }

    public void addGroup(LppGroup group) {
        if (!assignedGroupIds.contains(group.getGroupId())) {
            assignedGroupIds.add(group.getGroupId());
        }
        group.getNodes().forEach(n -> {
            if (!assignedNodeNames.contains(n.getNodeName())) {
                assignedNodeNames.add(n.getNodeName());
            }
        });
    }
}
