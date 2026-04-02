package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-shard status as reported by an LPP node.
 * Published to etcd at /lpp/{env}/nodes/{nodeName}/shards/{shardKey}.
 */
@Data
@NoArgsConstructor
public class LppShardActualState {

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
     * State from data-plane LppShardSlotState:
     * PENDING, DOWNLOADING, LOADED, WARMING, ACTIVE, REMOVING, REMOVED, FAILED.
     */
    @JsonProperty("state")
    private String state;

    @JsonProperty("last_state_change_ms")
    private long lastStateChangeMs;

    @JsonIgnore
    public boolean isActive() {
        return "ACTIVE".equals(state);
    }

    @JsonIgnore
    public boolean isFailed() {
        return "FAILED".equals(state);
    }

    @JsonIgnore
    public boolean isStuck(long stuckThresholdMs) {
        if (isActive() || "REMOVED".equals(state)) {
            return false;
        }
        return System.currentTimeMillis() - lastStateChangeMs > stuckThresholdMs;
    }
}
