package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * A replica group in LPP — a set of nodes that all serve the same set of shards.
 *
 * <p>Mapped 1:1 to an Odin instance. Adding a new group = adding a new Odin instance
 * with the same shard assignment, which is the autoscaling unit.
 *
 * <p>Default replica count is 3 (matches Odin cluster default).
 */
@Data
@NoArgsConstructor
public class LppGroup {

    /**
     * Group ID = Odin instance name (e.g., "delivery-grocery-ingester-1-production").
     */
    @JsonProperty("group_id")
    private String groupId;

    @JsonProperty("zone")
    private String zone;

    @JsonProperty("role")
    private String role;

    @JsonProperty("nodes")
    private List<LppNode> nodes = new ArrayList<>();

    /** Shard keys this group is assigned to serve. Set by the allocator. */
    @JsonProperty("assigned_shard_keys")
    private List<String> assignedShardKeys = new ArrayList<>();

    /** Target number of replicas in this group. Default 3. */
    @JsonProperty("replica_count")
    private int replicaCount = 3;

    public LppGroup(String groupId, String zone, String role) {
        this.groupId = groupId;
        this.zone = zone;
        this.role = role;
    }

    public void addNode(LppNode node) {
        nodes.add(node);
    }

    public int size() {
        return nodes.size();
    }

    public boolean isFull() {
        return nodes.size() >= replicaCount;
    }

    /** True if every node in the group is healthy and not drained. */
    public boolean isHealthy() {
        if (nodes.isEmpty()) return false;
        return nodes.stream().allMatch(n -> n.isHealthy() && !n.isDrained());
    }
}
