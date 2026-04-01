package io.clustercontroller.lpp.discovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.lpp.models.LppNodeActualState;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Grail client that queries LPP node topology via the Cerberus local proxy using YQL.
 *
 * <h3>Setup</h3>
 * <p>Start the Cerberus proxy before using this client:
 * <pre>
 *   cerberus -s grail-deployment-storage-staging --no-status-page
 * </pre>
 * Then the proxy is available at {@code http://localhost:5436}.
 *
 * <h3>YQL query</h3>
 * <p>Queries {@code luceneplus::node:*} entities filtered by Odin instanceName (the LPP
 * namespace, e.g. "delivery-grocery"). The key field is {@code GoalState.shard_id} which
 * is the replica-group identifier within the Odin instance — nodes sharing the same
 * shard_id form one replica group and all serve the same LP shards.
 *
 * <h3>Configuration (application-lpp.yml)</h3>
 * <pre>
 * lpp:
 *   grail:
 *     enabled: true
 *     baseUrl: http://localhost:5436    # Cerberus proxy
 *     connectTimeoutMs: 3000
 *     readTimeoutMs: 10000
 * </pre>
 */
@Slf4j
public class GrailHttpClient implements GrailClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * YQL for LPP nodes. Filters by instanceName (Odin namespace), region (controller's region),
     * and ensureState=PRESENT. Region filter ensures each regional controller only sees its
     * own nodes — DCA controller sees DCA nodes, PHX controller sees PHX nodes.
     * GoalState.shard_id is the replica-group ID within the region.
     */
    private static final String YQL_TEMPLATE =
            "luceneplus::node:* (\n" +
            "    WHERE storage::StorageNodeGoalState.instanceName = \"%s\"\n" +
            "    WHERE storage::StorageNodeGoalState.region = \"%s\"\n" +
            "    WHERE storage::StorageNodeGoalState.ensureState = \"PRESENT\"\n" +
            "    FIELD storage::StorageNodeGoalState.name as Name\n" +
            "    FIELD storage::StorageNodeGoalState.instanceName as InstanceName\n" +
            "    FIELD storage::StorageNodeGoalState.zone as Zone\n" +
            "    FIELD storage::StorageNodeGoalState.region as Region\n" +
            "    FIELD storage::NodePlacement.hostName as HostName\n" +
            "    FIELD storage::NodePlacement.ports as Ports\n" +
            "    FIELD storage::StorageNodeActualState.runState as RunState\n" +
            "    FIELD luceneplus::GoalState as GoalState\n" +
            "    FIELD storage::Health.health as Health\n" +
            ")";

    private final String yqlEndpoint;
    private final String region;
    private final HttpClient httpClient;
    private final int readTimeoutMs;

    public GrailHttpClient(String baseUrl, String region, int connectTimeoutMs, int readTimeoutMs) {
        String base = baseUrl.replaceAll("/$", "");
        this.yqlEndpoint = base + "/yql";
        this.region = region;
        this.readTimeoutMs = readTimeoutMs;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .build();
        log.info("LPP GrailHttpClient initialized → yqlEndpoint={}, region={}", this.yqlEndpoint, region);
    }

    @Override
    public List<LppNodeActualState> fetchNodes(String namespace) {
        log.info("LPP Grail: fetching topology via YQL for namespace={}, region={}", namespace, region);

        String yql = String.format(YQL_TEMPLATE, namespace, region);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(yqlEndpoint))
                    .timeout(Duration.ofMillis(readTimeoutMs))
                    .header("Content-Type", "text/plain")
                    .header("Accept", "application/json")
                    .header("RPC-Caller", "cluster-controller")
                    .header("RPC-Service", "grail-deployment-storage")
                    .header("X-Uber-Source", "cluster-controller")
                    .POST(HttpRequest.BodyPublishers.ofString(yql))
                    .build();

            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("LPP Grail: HTTP {} from YQL endpoint: {}", response.statusCode(), response.body());
                return List.of();
            }

            return parseYqlResponse(response.body(), namespace);

        } catch (Exception e) {
            log.error("LPP Grail: failed to fetch nodes for namespace={}: {}", namespace, e.getMessage(), e);
            return List.of();
        }
    }

    private List<LppNodeActualState> parseYqlResponse(String body, String namespace) throws Exception {
        YqlResponse yqlResponse = MAPPER.readValue(body, YqlResponse.class);
        List<LppNodeActualState> result = new ArrayList<>();

        if (yqlResponse.result == null) {
            log.warn("LPP Grail: YQL response has no 'result' field for namespace={}", namespace);
            return result;
        }

        for (YqlEntity entity : yqlResponse.result) {
            // Only include nodes that are actually running
            if (!"STARTED".equalsIgnoreCase(entity.runState)) {
                log.debug("LPP Grail: skipping node {} (RunState={})", entity.name, entity.runState);
                continue;
            }

            if (entity.goalState == null) {
                log.warn("LPP Grail: node {} has no GoalState, skipping", entity.name);
                continue;
            }

            // shard_id is the replica-group identifier (maps to LppGroup.groupId)
            String grailShardId = String.valueOf(entity.goalState.shardId);
            int grpcPort = extractGrpcPort(entity.ports);

            LppNodeActualState state = new LppNodeActualState(
                    entity.name,
                    entity.instanceName != null ? entity.instanceName : namespace,
                    grailShardId,
                    "INGEST",   // all LPP nodes handle both ingest and search
                    entity.zone != null ? entity.zone : entity.region);  // Zone is null in prod; fall back to region
            state.setHost(entity.hostName);
            state.setPort(grpcPort);
            state.setRegion(entity.region);

            result.add(state);
            log.debug("LPP Grail: node {} → grailShardId={}, host={}:{}, zone={}",
                    entity.name, grailShardId, entity.hostName, grpcPort, entity.zone);
        }

        log.info("LPP Grail: {} STARTED nodes for namespace={}", result.size(), namespace);
        return result;
    }

    /** Extract gRPC port from the Ports map returned by Grail. Falls back to 9090. */
    private int extractGrpcPort(Map<String, Integer> ports) {
        if (ports == null || ports.isEmpty()) return 9090;
        // Try common gRPC port key names
        for (String key : List.of("grpc", "GRPC", "rpc")) {
            Integer p = ports.get(key);
            if (p != null && p > 0) return p;
        }
        // Return the first available port
        return ports.values().iterator().next();
    }

    // -------------------------------------------------------------------------
    // YQL response model
    // -------------------------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class YqlResponse {
        /** Top-level array of entity results from YQL — field is "result" (verified). */
        @JsonProperty("result")
        List<YqlEntity> result;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class YqlEntity {
        @JsonProperty("Name")
        String name;

        @JsonProperty("InstanceName")
        String instanceName;

        @JsonProperty("Zone")
        String zone;

        @JsonProperty("Region")
        String region;

        @JsonProperty("HostName")
        String hostName;

        /** Map of port name → port number, e.g. {"grpc": 9090, "http": 8080} */
        @JsonProperty("Ports")
        Map<String, Integer> ports;

        @JsonProperty("RunState")
        String runState;

        @JsonProperty("GoalState")
        LppGoalState goalState;

        @JsonProperty("Health")
        String health;
    }

    /**
     * Subset of luceneplus::GoalState. The shard_id field identifies which replica group
     * this node belongs to within the Odin instance. Nodes with the same shard_id form
     * one replica group and all serve the same LP index shards.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class LppGoalState {
        /** Replica-group identifier within the Odin instance. Becomes LppGroup.groupId. */
        @JsonProperty("shard_id")
        int shardId;

        @JsonProperty("collection")
        String collection;
    }
}
