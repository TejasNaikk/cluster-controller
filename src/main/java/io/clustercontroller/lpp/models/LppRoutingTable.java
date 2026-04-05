package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Routing table goal state written by the controller and consumed by the LPP gateway.
 *
 * <p>Stored at /lpp/{env}/routing/goal-state.
 *
 * <p>The routing decision for each shard:
 * <ol>
 *   <li>PA ∩ AA: nodes that are both planned (in PA) AND actively serving the shard
 *       (shard state == ACTIVE, fresh heartbeat). PA is the source of truth.</li>
 *   <li>Fallback: if the intersection is empty (e.g. mid-migration), fall back to ALL
 *       nodes that have the shard ACTIVE with a fresh heartbeat, regardless of PA.
 *       This prevents a dark shard during handoff.</li>
 * </ol>
 *
 * <p>The {@code version} monotonically increments on each change to the table content.
 */
@Data
@NoArgsConstructor
public class LppRoutingTable {

    /** Monotonically increasing version, bumped each time the routing table content changes. */
    @JsonProperty("version")
    private long version;

    @JsonProperty("last_updated_ms")
    private long lastUpdatedMs;

    /**
     * Map from shardKey → list of routable nodes for that shard.
     * shardKey = "{collection}.{fullIndexName}.{shardId}"
     */
    @JsonProperty("shard_routes")
    private Map<String, List<LppShardRoute>> shardRoutes = new LinkedHashMap<>();

    public LppRoutingTable(long version) {
        this.version = version;
        this.lastUpdatedMs = System.currentTimeMillis();
    }

    public void addRoute(String shardKey, LppShardRoute route) {
        shardRoutes.computeIfAbsent(shardKey, k -> new ArrayList<>()).add(route);
    }

    public void setRoutes(String shardKey, List<LppShardRoute> routes) {
        shardRoutes.put(shardKey, routes);
    }
}
