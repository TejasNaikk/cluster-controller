package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Goal state for a single LPP node — this is the manifest the controller writes to etcd
 * at /lpp/{env}/nodes/{nodeName}/goal-state. The data-plane LPP node watches this key
 * and reconciles its shard slots accordingly.
 *
 * <p>Mirrors the LppManifest structure on the data plane (PR #12810).
 */
@Data
@NoArgsConstructor
public class LppNodeGoalState {

    @JsonProperty("node_name")
    private String nodeName;

    @JsonProperty("role")
    private String role = "INGEST";

    @JsonProperty("region")
    private String region = "local";

    @JsonProperty("shards")
    private List<LppShardEntry> shards = new ArrayList<>();

    /** Monotonically increasing version. Incremented on every controller-side update. */
    @JsonProperty("version")
    private long version;

    @JsonProperty("last_updated_ms")
    private long lastUpdatedMs;

    public LppNodeGoalState(String nodeName, String role, String region) {
        this.nodeName = nodeName;
        this.role = role;
        this.region = region;
        this.lastUpdatedMs = System.currentTimeMillis();
    }

    public void addShard(LppShardEntry entry) {
        shards.add(entry);
    }

    /** Bump version and timestamp. Call before writing to etcd. */
    public LppNodeGoalState bumpVersion() {
        this.version++;
        this.lastUpdatedMs = System.currentTimeMillis();
        return this;
    }
}
