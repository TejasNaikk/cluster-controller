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
 *
 * <h3>Separate ingest / search group sets</h3>
 * <p>Today all groups handle both ingest and search so {@code ingestGroupIds} and
 * {@code searchGroupIds} will usually be identical. The separation exists so the
 * allocator can plan dedicated groups in the future without schema changes.
 *
 * <h3>Safe handoff (add-before-remove)</h3>
 * <p>When a shard needs to move to a different group (reallocation, upscale), the allocator
 * uses a two-phase handoff:
 * <ol>
 *   <li>{@code status = HANDOFF}: both old and new groups are in {@code assignedGroupIds}
 *       (union of {@code ingestGroupIds} and {@code searchGroupIds}). The orchestrator
 *       pushes the shard to the new groups and waits for them to become ACTIVE.</li>
 *   <li>Once new groups are ACTIVE, the shard is removed from old groups and
 *       {@code status} reverts to STABLE.</li>
 * </ol>
 */
@Data
@NoArgsConstructor
public class LppShardPlannedAllocation {

    public enum HandoffStatus { STABLE, HANDOFF }

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

    /**
     * All group IDs currently assigned to serve this shard (ingest ∪ search, plus any
     * handoff transition groups). This is what the orchestrator iterates over to build
     * per-node goal states.
     */
    @JsonProperty("assigned_group_ids")
    private List<String> assignedGroupIds = new ArrayList<>();

    /** Node names (flattened from assignedGroupIds) assigned to serve this shard. */
    @JsonProperty("assigned_node_names")
    private List<String> assignedNodeNames = new ArrayList<>();

    /** Ingester node names assigned to serve this shard (ingest path). */
    @JsonProperty("assigned_ingester_node_names")
    private List<String> assignedIngesterNodeNames = new ArrayList<>();

    /** Searcher node names assigned to serve this shard (search path). */
    @JsonProperty("assigned_searcher_node_names")
    private List<String> assignedSearcherNodeNames = new ArrayList<>();

    /** Groups responsible for the ingest/write path. */
    @JsonProperty("ingest_group_ids")
    private List<String> ingestGroupIds = new ArrayList<>();

    /** Groups responsible for the search/read path. */
    @JsonProperty("search_group_ids")
    private List<String> searchGroupIds = new ArrayList<>();

    /**
     * STABLE = normal operation. HANDOFF = shard is migrating; both old and new groups
     * are in the assignment until new groups report ACTIVE.
     */
    @JsonProperty("status")
    private HandoffStatus status = HandoffStatus.STABLE;

    /**
     * During HANDOFF: the new groups that should take over from the current ingest/search
     * groups. These are included in {@code assignedGroupIds} so the orchestrator pushes
     * to them. Once ACTIVE, they replace the groups in ingestGroupIds / searchGroupIds.
     */
    @JsonProperty("target_ingest_group_ids")
    private List<String> targetIngestGroupIds = new ArrayList<>();

    @JsonProperty("target_search_group_ids")
    private List<String> targetSearchGroupIds = new ArrayList<>();

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

    /** Add a group to the ingest path. Also registers it in assignedGroupIds/Nodes. */
    public void addIngestGroup(LppGroup group) {
        addGroupInternal(group, assignedIngesterNodeNames);
        if (!ingestGroupIds.contains(group.getGroupId())) {
            ingestGroupIds.add(group.getGroupId());
        }
    }

    /** Add a group to the search path. Also registers it in assignedGroupIds/Nodes. */
    public void addSearchGroup(LppGroup group) {
        addGroupInternal(group, assignedSearcherNodeNames);
        if (!searchGroupIds.contains(group.getGroupId())) {
            searchGroupIds.add(group.getGroupId());
        }
    }

    /**
     * Convenience: add a group to both ingest and search paths.
     * Used when a group handles both (the current default).
     */
    public void addGroup(LppGroup group) {
        addIngestGroup(group);
        addSearchGroup(group);
    }

    private void addGroupInternal(LppGroup group, List<String> roleSpecificNodes) {
        if (!assignedGroupIds.contains(group.getGroupId())) {
            assignedGroupIds.add(group.getGroupId());
        }
        group.getNodes().forEach(n -> {
            if (!assignedNodeNames.contains(n.getNodeName())) {
                assignedNodeNames.add(n.getNodeName());
            }
            if (!roleSpecificNodes.contains(n.getNodeName())) {
                roleSpecificNodes.add(n.getNodeName());
            }
        });
    }
}
