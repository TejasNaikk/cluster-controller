package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single LucenePlus node as seen by the LPP controller.
 *
 * <p>A node belongs to exactly one Odin instance (= group). The grailShardId is the shard
 * identifier reported by Grail, used for grouping nodes until LPP nodes report their own
 * shard registry. It does NOT correspond to a Lucene shard — it is an infrastructure-level
 * identifier from the deployment topology.
 */
@Data
@NoArgsConstructor
public class LppNode {

    /** Unique node name (e.g., "delivery-grocery-ingester-1-production-phx-001"). */
    @JsonProperty("node_name")
    private String nodeName;

    /**
     * Odin instance this node belongs to — used as the group identifier.
     * One Odin instance = one replica group in the LPP model.
     */
    @JsonProperty("odin_instance")
    private String odinInstance;

    /**
     * Shard ID reported by Grail for this node. Used as a temporary group identifier
     * until LPP nodes expose their own shard registry via etcd.
     */
    @JsonProperty("grail_shard_id")
    private String grailShardId;

    /** Node role: INGEST or SEARCH. */
    @JsonProperty("role")
    private String role;

    /** Physical host or IP. */
    @JsonProperty("host")
    private String host;

    /** gRPC port for management service. */
    @JsonProperty("port")
    private int port = 9090;

    /** Availability zone for anti-affinity decisions. */
    @JsonProperty("zone")
    private String zone;

    /** Admin state: NORMAL or DRAIN. */
    @JsonProperty("admin_state")
    private String adminState = "NORMAL";

    /** Health state derived from heartbeat freshness and node metrics. */
    @JsonProperty("health_state")
    private String healthState = "UNKNOWN";

    /** Timestamp of last observed heartbeat (from Grail or etcd actual-state). */
    @JsonProperty("last_heartbeat_ms")
    private long lastHeartbeatMs;

    /**
     * Shard slots currently active on this node, as reported by the node itself.
     * Empty until LPP nodes begin publishing actual-state to etcd.
     */
    @JsonProperty("active_shard_keys")
    private List<String> activeShardKeys = new ArrayList<>();

    public LppNode(String nodeName, String odinInstance, String grailShardId, String role, String zone) {
        this.nodeName = nodeName;
        this.odinInstance = odinInstance;
        this.grailShardId = grailShardId;
        this.role = role;
        this.zone = zone;
        this.lastHeartbeatMs = System.currentTimeMillis();
    }

    public boolean isDrained() {
        return "DRAIN".equals(adminState);
    }

    public boolean isHealthy() {
        return "GREEN".equals(healthState) || "YELLOW".equals(healthState);
    }

    /** Key format used to look up this node in etcd and in maps. */
    @JsonIgnore
    public String getKey() {
        return nodeName;
    }
}
