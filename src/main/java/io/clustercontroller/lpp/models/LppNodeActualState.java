package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Actual state of an LPP node as reported to the controller.
 *
 * <p>Initially sourced from Grail (infrastructure topology) until LPP nodes publish
 * this directly to etcd at /lpp/{env}/nodes/{nodeName}/actual-state.
 *
 * <p>Per-shard states are published separately at
 * /lpp/{env}/nodes/{nodeName}/shards/{shardKey}.
 */
@Data
@NoArgsConstructor
public class LppNodeActualState {

    @JsonProperty("node_name")
    private String nodeName;

    @JsonProperty("host")
    private String host;

    @JsonProperty("port")
    private int port;

    /** Odin instance name — identifies the LPP multi-tenant cluster this node belongs to. */
    @JsonProperty("odin_instance")
    private String odinInstance;

    /** Shard ID from Grail topology. Used as the group key — nodes with the same grailShardId
     * form one replica group (3 replicas all serving identical shards). */
    @JsonProperty("grail_shard_id")
    private String grailShardId;

    /**
     * Group ID from node self-report. Primary group key — nodes with the same groupId
     * form one replica group. Replaces grailShardId for self-reporting nodes.
     */
    @JsonProperty("group_id")
    private String groupId;

    @JsonProperty("role")
    private String role;

    @JsonProperty("zone")
    private String zone;

    @JsonProperty("region")
    private String region;

    /** Epoch ms of last heartbeat. Used to detect dead nodes. */
    @JsonProperty("heartbeat_timestamp_ms")
    private long heartbeatTimestampMs;

    /** Number of active shard slots on this node (from node's own reporting). */
    @JsonProperty("shard_count")
    private int shardCount;

    /** Per-shard status entries. Populated when LPP nodes write actual-state. */
    @JsonProperty("shard_states")
    private List<LppShardActualState> shardStates = new ArrayList<>();

    public LppNodeActualState(String nodeName, String odinInstance, String grailShardId, String role, String zone) {
        this.nodeName = nodeName;
        this.odinInstance = odinInstance;
        this.grailShardId = grailShardId;
        this.role = role;
        this.zone = zone;
        this.heartbeatTimestampMs = System.currentTimeMillis();
    }

    public boolean isStale(long timeoutMs) {
        return System.currentTimeMillis() - heartbeatTimestampMs > timeoutMs;
    }
}
