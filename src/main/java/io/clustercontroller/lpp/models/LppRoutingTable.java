package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Routing table written by the controller and consumed by the LPP search gateway.
 *
 * <p>Stored at /lpp/search-gateway/{env}/{region}/routing-table.
 *
 * <p>Structure:
 * <pre>
 * {
 *   "version": 5,
 *   "last_updated_ms": 1234567890,
 *   "index_shard_routing": {
 *     "deals_index.v1": {
 *       "0": ["node-a", "node-b"],
 *       "1": ["node-c", "node-d"]
 *     },
 *     "local_index.v1": { ... }
 *   }
 * }
 * </pre>
 *
 * <p>Key is fullIndexName (e.g. "deals_index.v1"), sub-key is shardId as string,
 * value is the list of node names routable for that shard (PA ∩ AA, or fallback).
 */
@Data
@NoArgsConstructor
public class LppRoutingTable {

    /** Monotonically increasing version, bumped each time routing table content changes. */
    @JsonProperty("version")
    private long version;

    @JsonProperty("last_updated_ms")
    private long lastUpdatedMs;

    /**
     * routes: fullIndexName → { shardId → [nodeName, ...] }
     */
    @JsonProperty("index_shard_routing")
    private Map<String, Map<String, List<String>>> routes = new LinkedHashMap<>();

    public LppRoutingTable(long version) {
        this.version = version;
        this.lastUpdatedMs = System.currentTimeMillis();
    }

    public void addNode(String fullIndexName, int shardId, String nodeName) {
        routes.computeIfAbsent(fullIndexName, k -> new LinkedHashMap<>())
              .computeIfAbsent(String.valueOf(shardId), k -> new ArrayList<>())
              .add(nodeName);
    }

    public void setShardNodes(String fullIndexName, int shardId, List<String> nodes) {
        routes.computeIfAbsent(fullIndexName, k -> new LinkedHashMap<>())
              .put(String.valueOf(shardId), nodes);
    }
}
