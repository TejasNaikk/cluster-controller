package io.clustercontroller.lpp.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.lpp.models.*;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * etcd-backed metadata store for LPP controller state.
 *
 * <p>Wraps the jetcd client with typed read/write methods for LPP models.
 * All keys are resolved through {@link LppEtcdPathResolver}.
 */
@Slf4j
public class LppMetadataStore {

    private static final long TIMEOUT_SECONDS = 5;
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final Client etcdClient;
    private final LppEtcdPathResolver pathResolver;

    public LppMetadataStore(Client etcdClient, LppEtcdPathResolver pathResolver) {
        this.etcdClient = etcdClient;
        this.pathResolver = pathResolver;
    }

    // -------------------------------------------------------------------------
    // NODE ACTUAL STATE
    // -------------------------------------------------------------------------

    public void putNodeActualState(LppNodeActualState state) {
        put(pathResolver.nodeActualStatePath(state.getNodeName()), toJson(state));
    }

    public Optional<LppNodeActualState> getNodeActualState(String nodeName) {
        return get(pathResolver.nodeActualStatePath(nodeName))
                .map(v -> fromJson(v, LppNodeActualState.class));
    }

    /** Returns all node actual states. Key = nodeName. */
    public Map<String, LppNodeActualState> getAllNodeActualStates() {
        Map<String, String> raw = getWithPrefix(pathResolver.nodesPrefix());
        Map<String, LppNodeActualState> result = new LinkedHashMap<>();
        raw.forEach((key, value) -> {
            if (key.endsWith("/actual-state")) {
                LppNodeActualState state = fromJson(value, LppNodeActualState.class);
                if (state != null) {
                    result.put(state.getNodeName(), state);
                }
            }
        });
        return result;
    }

    public void deleteNodeActualState(String nodeName) {
        delete(pathResolver.nodeActualStatePath(nodeName));
    }

    public void deleteNodeGoalState(String nodeName) {
        delete(pathResolver.nodeGoalStatePath(nodeName));
    }

    // -------------------------------------------------------------------------
    // NODE GOAL STATE
    // -------------------------------------------------------------------------

    public void putNodeGoalState(LppNodeGoalState goalState) {
        put(pathResolver.nodeGoalStatePath(goalState.getNodeName()), toJson(goalState));
    }

    public Optional<LppNodeGoalState> getNodeGoalState(String nodeName) {
        return get(pathResolver.nodeGoalStatePath(nodeName))
                .map(v -> fromJson(v, LppNodeGoalState.class));
    }

    // -------------------------------------------------------------------------
    // SHARD PLANNED ALLOCATION
    // -------------------------------------------------------------------------

    public void putShardPlannedAllocation(LppShardPlannedAllocation allocation) {
        put(pathResolver.shardPlannedAllocationPath(allocation.getShardKey()), toJson(allocation));
    }

    public Optional<LppShardPlannedAllocation> getShardPlannedAllocation(String shardKey) {
        return get(pathResolver.shardPlannedAllocationPath(shardKey))
                .map(v -> fromJson(v, LppShardPlannedAllocation.class));
    }

    public Map<String, LppShardPlannedAllocation> getAllShardPlannedAllocations() {
        Map<String, String> raw = getWithPrefix(pathResolver.shardsPrefix());
        Map<String, LppShardPlannedAllocation> result = new LinkedHashMap<>();
        raw.forEach((key, value) -> {
            if (key.endsWith("/planned-allocation")) {
                LppShardPlannedAllocation alloc = fromJson(value, LppShardPlannedAllocation.class);
                if (alloc != null) {
                    result.put(alloc.getShardKey(), alloc);
                }
            }
        });
        return result;
    }

    // -------------------------------------------------------------------------
    // INDEX DEFINITIONS
    // -------------------------------------------------------------------------

    public void putIndexDefinition(LppIndexDefinition def) {
        put(pathResolver.indexConfPath(def.getKey()), toJson(def));
    }

    public Optional<LppIndexDefinition> getIndexDefinition(String indexKey) {
        return get(pathResolver.indexConfPath(indexKey))
                .map(v -> fromJson(v, LppIndexDefinition.class));
    }

    public List<LppIndexDefinition> getAllIndexDefinitions() {
        Map<String, String> raw = getWithPrefix(pathResolver.indicesPrefix());
        List<LppIndexDefinition> result = new ArrayList<>();
        raw.forEach((key, value) -> {
            if (key.endsWith("/conf")) {
                LppIndexDefinition def = fromJson(value, LppIndexDefinition.class);
                if (def != null) {
                    result.add(def);
                }
            }
        });
        return result;
    }

    // -------------------------------------------------------------------------
    // INTERNAL ETCD OPS
    // -------------------------------------------------------------------------

    private void put(String key, String value) {
        try {
            etcdClient.getKVClient()
                    .put(toBS(key), toBS(value))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.debug("LPP etcd put: {}", key);
        } catch (Exception e) {
            log.error("LPP etcd put failed for key {}: {}", key, e.getMessage());
        }
    }

    private Optional<String> get(String key) {
        try {
            GetResponse resp = etcdClient.getKVClient()
                    .get(toBS(key))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (resp.getKvs().isEmpty()) return Optional.empty();
            return Optional.of(resp.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("LPP etcd get failed for key {}: {}", key, e.getMessage());
            return Optional.empty();
        }
    }

    private Map<String, String> getWithPrefix(String prefix) {
        try {
            GetOption opt = GetOption.newBuilder().isPrefix(true).build();
            GetResponse resp = etcdClient.getKVClient()
                    .get(toBS(prefix), opt)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<String, String> result = new LinkedHashMap<>();
            for (KeyValue kv : resp.getKvs()) {
                result.put(
                        kv.getKey().toString(StandardCharsets.UTF_8),
                        kv.getValue().toString(StandardCharsets.UTF_8));
            }
            return result;
        } catch (Exception e) {
            log.error("LPP etcd prefix get failed for prefix {}: {}", prefix, e.getMessage());
            return Collections.emptyMap();
        }
    }

    private void delete(String key) {
        try {
            etcdClient.getKVClient()
                    .delete(toBS(key))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.debug("LPP etcd delete: {}", key);
        } catch (Exception e) {
            log.error("LPP etcd delete failed for key {}: {}", key, e.getMessage());
        }
    }

    private ByteSequence toBS(String s) {
        return ByteSequence.from(s, StandardCharsets.UTF_8);
    }

    private String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("LPP JSON serialization failed: {}", e.getMessage());
            return "{}";
        }
    }

    private <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            log.error("LPP JSON deserialization failed for type {}: {}", type.getSimpleName(), e.getMessage());
            return null;
        }
    }
}
