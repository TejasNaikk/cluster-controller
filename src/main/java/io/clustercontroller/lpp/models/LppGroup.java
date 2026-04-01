package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * A replica group in LPP — a set of nodes that all serve the same set of shards.
 *
 * <p>Mapped 1:1 to an Odin instance. Adding a new group = adding a new Odin instance
 * with the same shard assignment, which is the autoscaling unit.
 *
 * <p>Default replica count is 3 (matches Odin cluster default).
 *
 * <p>{@link GroupType} tracks which traffic types a group handles. Today all groups
 * handle both INGEST and SEARCH. The type set is the infrastructure hook for introducing
 * dedicated ingest-only or search-only groups in the future without changing the allocator.
 */
@Data
@NoArgsConstructor
public class LppGroup {

    /**
     * Traffic type a group can serve. A group can handle one or both types.
     * Default: {INGEST, SEARCH} — all current LPP groups do both.
     */
    public enum GroupType { INGEST, SEARCH }

    /**
     * Group ID = Odin instance name (e.g., "delivery-grocery-ingester-1-production").
     */
    @JsonProperty("group_id")
    private String groupId;

    @JsonProperty("zone")
    private String zone;

    @JsonProperty("role")
    private String role;

    /**
     * Traffic types this group can serve. All groups default to both INGEST and SEARCH
     * since LPP does not yet have dedicated groups. Future dedicated groups will have
     * only one type in this set.
     */
    @JsonProperty("group_types")
    private Set<GroupType> groupTypes = EnumSet.of(GroupType.INGEST, GroupType.SEARCH);

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
        // Default: can serve both — dedicated groups override this after construction
        this.groupTypes = EnumSet.of(GroupType.INGEST, GroupType.SEARCH);
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

    /**
     * True if every node in the group is healthy and not drained.
     * Used for observability / metrics — stricter than eligibility.
     */
    public boolean isHealthy() {
        if (nodes.isEmpty()) return false;
        return nodes.stream().allMatch(n -> n.isHealthy() && !n.isDrained());
    }

    /**
     * True if the group has at least one node that is not being drained.
     *
     * <p>Used by the allocator for shard assignment eligibility. A group with 2/3 nodes
     * healthy is still fully capable of serving traffic — removing it from allocation
     * would trigger unnecessary resharding. We only exclude a group if every node in it
     * is being drained (decommission in progress) or the group is empty.
     */
    public boolean hasEligibleNodes() {
        if (nodes.isEmpty()) return false;
        return nodes.stream().anyMatch(n -> !n.isDrained());
    }

    /**
     * True if this group can serve the given traffic type.
     * All groups default to serving both INGEST and SEARCH.
     */
    public boolean canServe(GroupType type) {
        return groupTypes.contains(type);
    }
}
