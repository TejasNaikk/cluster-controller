package io.clustercontroller.store;

import io.clustercontroller.models.Index;
import io.clustercontroller.models.ShardAllocation;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.util.EnvironmentUtils;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.LeaseOption;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.clustercontroller.config.Constants.PATH_DELIMITER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.clustercontroller.config.Constants.*;

/**
 * etcd-based implementation of MetadataStore.
 * Singleton to ensure single etcd client connection.
 */
@Slf4j
public class EtcdMetadataStore implements MetadataStore {
    
    // TODO: Make etcd timeout configurable via environment variable or config
    private static final long ETCD_OPERATION_TIMEOUT_SECONDS = 5;
    
    private static EtcdMetadataStore instance;
    
    private final String clusterName;
    private final String[] etcdEndpoints;
    private final Client etcdClient;
    private final KV kvClient;
    private final EtcdPathResolver pathResolver;
    private final ObjectMapper objectMapper;
    
    // Leader election fields
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final String nodeId;
    
    /**
     * Private constructor for singleton pattern
     */
    private EtcdMetadataStore(String clusterName, String[] etcdEndpoints) throws Exception {
        this.clusterName = clusterName;
        this.etcdEndpoints = etcdEndpoints;
        this.nodeId = EnvironmentUtils.getRequiredEnv("NODE_NAME");
        
        // Initialize Jackson ObjectMapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // Initialize etcd client
        this.etcdClient = Client.builder().endpoints(etcdEndpoints).build();
        this.kvClient = etcdClient.getKVClient();
        
        // Initialize path resolver
        this.pathResolver = new EtcdPathResolver(clusterName);
        
        log.info("EtcdMetadataStore initialized for cluster: {} with endpoints: {} and nodeId: {}", 
            clusterName, String.join(",", etcdEndpoints), nodeId);
    }
    
    // =================================================================
    // SINGLETON MANAGEMENT
    // =================================================================
    
    /**
     * Test constructor with injected dependencies
     */
    private EtcdMetadataStore(String clusterName, String[] etcdEndpoints, String nodeId, Client etcdClient, KV kvClient) {
        this.clusterName = clusterName;
        this.etcdEndpoints = etcdEndpoints;
        this.nodeId = nodeId;
        this.etcdClient = etcdClient;
        this.kvClient = kvClient;
        
        // Initialize Jackson ObjectMapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // Initialize path resolver
        this.pathResolver = new EtcdPathResolver(clusterName);
        
        log.info("EtcdMetadataStore initialized for testing with cluster: {} and nodeId: {}", clusterName, nodeId);
    }
    /**
     * Get singleton instance
     */
    public static synchronized EtcdMetadataStore getInstance(String clusterName, String[] etcdEndpoints) throws Exception {
        if (instance == null) {
            instance = new EtcdMetadataStore(clusterName, etcdEndpoints);
        }
        return instance;
    }
    
    /**
     * Get existing instance (throws if not initialized)
     */
    public static EtcdMetadataStore getInstance() {
        if (instance == null) {
            throw new IllegalStateException("EtcdMetadataStore not initialized. Call getInstance(clusterName, etcdEndpoints) first.");
        }
        return instance;
    }
    
    /**
     * Reset singleton instance (for testing only)
     */
    public static synchronized void resetInstance() {
        instance = null;
    }
    
    /**
     * Create test instance with mocked dependencies (for testing only)
     */
    public static synchronized EtcdMetadataStore createTestInstance(String clusterName, String[] etcdEndpoints, String nodeId, Client etcdClient, KV kvClient) {
        resetInstance();
        instance = new EtcdMetadataStore(clusterName, etcdEndpoints, nodeId, etcdClient, kvClient);
        return instance;
    }

    
    // =================================================================
    // CONTROLLER TASKS OPERATIONS
    // =================================================================
    
    @Override
    public List<TaskMetadata> getAllTasks() throws Exception {
        log.debug("Getting all tasks from etcd");
        
        try {
            String tasksPrefix = pathResolver.getControllerTasksPrefix();
            List<TaskMetadata> tasks = getAllObjectsByPrefix(tasksPrefix, TaskMetadata.class);
            
            // Sort by priority (0 = highest priority)
            tasks.sort((t1, t2) -> Integer.compare(t1.getPriority(), t2.getPriority()));
            
            log.debug("Retrieved {} tasks from etcd", tasks.size());
            return tasks;
            
        } catch (Exception e) {
            log.error("Failed to get all tasks from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve tasks from etcd", e);
        }
    }
    
    @Override
    public Optional<TaskMetadata> getTask(String taskName) throws Exception {
        log.debug("Getting task {} from etcd", taskName);
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(taskName);
            Optional<TaskMetadata> result = getObjectByPath(taskPath, TaskMetadata.class);
            
            if (result.isPresent()) {
                log.debug("Retrieved task {} from etcd", taskName);
            } else {
                log.debug("Task {} not found in etcd", taskName);
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get task {} from etcd: {}", taskName, e.getMessage(), e);
            throw new Exception("Failed to retrieve task from etcd", e);
        }
    }
    
    @Override
    public String createTask(TaskMetadata task) throws Exception {
        log.info("Creating task {} in etcd", task.getName());
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(task.getName());
            storeObjectAsJson(taskPath, task);
            
            log.info("Successfully created task {} in etcd", task.getName());
            return task.getName();
            
        } catch (Exception e) {
            log.error("Failed to create task {} in etcd: {}", task.getName(), e.getMessage(), e);
            throw new Exception("Failed to create task in etcd", e);
        }
    }
    
    @Override
    public void updateTask(TaskMetadata task) throws Exception {
        log.debug("Updating task {} in etcd", task.getName());
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(task.getName());
            storeObjectAsJson(taskPath, task);
            
            log.debug("Successfully updated task {} in etcd", task.getName());
            
        } catch (Exception e) {
            log.error("Failed to update task {} in etcd: {}", task.getName(), e.getMessage(), e);
            throw new Exception("Failed to update task in etcd", e);
        }
    }
    
    @Override
    public void deleteTask(String taskName) throws Exception {
        log.info("Deleting task {} from etcd", taskName);
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(taskName);
            executeEtcdDelete(taskPath);
            
            log.info("Successfully deleted task {} from etcd", taskName);
            
        } catch (Exception e) {
            log.error("Failed to delete task {} from etcd: {}", taskName, e.getMessage(), e);
            throw new Exception("Failed to delete task from etcd", e);
        }
    }
    
    @Override
    public void deleteOldTasks(long olderThanTimestamp) throws Exception {
        log.debug("Deleting old tasks from etcd older than {}", olderThanTimestamp);
        // TODO: Implement etcd cleanup for old tasks
    }
    
    // =================================================================
    // SEARCH UNITS OPERATIONS
    // =================================================================
    
    @Override
    public List<SearchUnit> getAllSearchUnits() throws Exception {
        log.debug("Getting all search units from etcd");
        
        try {
            String unitsPrefix = pathResolver.getSearchUnitsPrefix();
            List<SearchUnit> searchUnits = getAllObjectsByPrefix(unitsPrefix, SearchUnit.class);
            
            log.debug("Retrieved {} search units from etcd", searchUnits.size());
            return searchUnits;
            
        } catch (Exception e) {
            log.error("Failed to get all search units from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve search units from etcd", e);
        }
    }
    
    @Override
    public Optional<SearchUnit> getSearchUnit(String unitName) throws Exception {
        log.debug("Getting search unit {} from etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(unitName);
            Optional<SearchUnit> result = getObjectByPath(unitPath, SearchUnit.class);
            
            if (result.isPresent()) {
                log.debug("Retrieved search unit {} from etcd", unitName);
            } else {
                log.debug("Search unit {} not found in etcd", unitName);
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get search unit {} from etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to retrieve search unit from etcd", e);
        }
    }
    
    @Override
    public void upsertSearchUnit(String unitName, SearchUnit searchUnit) throws Exception {
        log.info("Upserting search unit {} in etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(unitName);
            storeObjectAsJson(unitPath, searchUnit);
            
            log.info("Successfully upserted search unit {} in etcd", unitName);
            
        } catch (Exception e) {
            log.error("Failed to upsert search unit {} in etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to upsert search unit in etcd", e);
        }
    }
    
    @Override
    public void updateSearchUnit(SearchUnit searchUnit) throws Exception {
        log.debug("Updating search unit {} in etcd", searchUnit.getName());
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(searchUnit.getName());
            storeObjectAsJson(unitPath, searchUnit);
            
            log.debug("Successfully updated search unit {} in etcd", searchUnit.getName());
            
        } catch (Exception e) {
            log.error("Failed to update search unit {} in etcd: {}", searchUnit.getName(), e.getMessage(), e);
            throw new Exception("Failed to update search unit in etcd", e);
        }
    }
    
    @Override
    public void deleteSearchUnit(String unitName) throws Exception {
        log.info("Deleting search unit {} from etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(unitName);
            executeEtcdDelete(unitPath);
            
            log.info("Successfully deleted search unit {} from etcd", unitName);
            
        } catch (Exception e) {
            log.error("Failed to delete search unit {} from etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to delete search unit from etcd", e);
        }
    }
    
    // =================================================================
    // SEARCH UNIT STATE OPERATIONS (for discovery)
    // =================================================================
    
    @Override
    public Map<String, SearchUnitActualState> getAllSearchUnitActualStates() throws Exception {
        String prefix = pathResolver.getSearchUnitsPrefix();
        GetOption option = GetOption.newBuilder()
                .withPrefix(ByteSequence.from(prefix, UTF_8))
                .build();
        
        CompletableFuture<GetResponse> getFuture = kvClient.get(
                ByteSequence.from(prefix, UTF_8), option);
        GetResponse response = getFuture.get();
        
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        
        for (KeyValue kv : response.getKvs()) {
            String key = kv.getKey().toString(UTF_8);
            String json = kv.getValue().toString(UTF_8);
            
            // Parse key to get unit name and check if it's an actual-state key
            String relativePath = key.substring(prefix.length());
            String[] parts = relativePath.split("/");
            if (parts.length >= 2 && "actual-state".equals(parts[1])) {
                String unitName = parts[0];
                try {
                    SearchUnitActualState actualState = objectMapper.readValue(json, SearchUnitActualState.class);
                    actualStates.put(unitName, actualState);
                } catch (Exception e) {
                    log.warn("Failed to parse actual state for unit {}: {}", unitName, e.getMessage());
                }
            }
        }
        
        log.debug("Retrieved {} search unit actual states from etcd", actualStates.size());
        return actualStates;
    }
    
    @Override
    public Optional<SearchUnitGoalState> getSearchUnitGoalState(String unitName) throws Exception {
        String key = pathResolver.getSearchUnitGoalStatePath(unitName);
        CompletableFuture<GetResponse> getFuture = kvClient.get(ByteSequence.from(key, UTF_8));
        GetResponse response = getFuture.get();
        
        if (response.getKvs().isEmpty()) {
            return Optional.empty();
        }
        
        String json = response.getKvs().get(0).getValue().toString(UTF_8);
        SearchUnitGoalState goalState = objectMapper.readValue(json, SearchUnitGoalState.class);
        
        return Optional.of(goalState);
    }
    
    @Override
    public void updateSearchUnitGoalState(String unitName, SearchUnitGoalState goalState) throws Exception {
        log.debug("Updating search unit goal state for unit: {}", unitName);
        
        try {
            String key = pathResolver.getSearchUnitGoalStatePath(unitName);
            String json = objectMapper.writeValueAsString(goalState);
            
            executeEtcdPut(key, json);
            
            log.debug("Successfully updated goal state for search unit: {}", unitName);
            
        } catch (Exception e) {
            log.error("Failed to update goal state for search unit {}: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to update search unit goal state in etcd", e);
        }
    }
    
    @Override
    public Optional<SearchUnitActualState> getSearchUnitActualState(String unitName) throws Exception {
        String key = pathResolver.getSearchUnitActualStatePath(unitName);
        CompletableFuture<GetResponse> getFuture = kvClient.get(ByteSequence.from(key, UTF_8));
        GetResponse response = getFuture.get();
        
        if (response.getKvs().isEmpty()) {
            return Optional.empty();
        }
        
        String json = response.getKvs().get(0).getValue().toString(UTF_8);
        SearchUnitActualState actualState = objectMapper.readValue(json, SearchUnitActualState.class);
        
        return Optional.of(actualState);
    }
    // =================================================================
    // INDEX CONFIGURATIONS OPERATIONS
    // =================================================================
    
    @Override
    public List<Index> getAllIndexConfigs() throws Exception {
        log.debug("Getting all index configs from etcd");
        
        try {
            String indicesPrefix = pathResolver.getIndicesPrefix();
            GetResponse response = executeEtcdPrefixQuery(indicesPrefix);
            
            List<Index> indices = new ArrayList<>();
            for (var kv : response.getKvs()) {
                String indexConfigJson = kv.getValue().toString(StandardCharsets.UTF_8);
                try {
                    Index index = objectMapper.readValue(indexConfigJson, Index.class);
                    indices.add(index);
                } catch (Exception e) {
                    log.warn("Failed to parse index config JSON: {}", e.getMessage());
                }
            }
            
            log.debug("Retrieved {} index configs from etcd", indices.size());
            return indices;
            
        } catch (Exception e) {
            log.error("Failed to get all index configs from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve index configs from etcd", e);
        }
    }
    
    @Override
    public Optional<String> getIndexConfig(String indexName) throws Exception {
        log.debug("Getting index config {} from etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(indexName);
            GetResponse response = executeEtcdGet(indexPath);
            
            if (response.getCount() == 0) {
                log.debug("Index config {} not found in etcd", indexName);
                return Optional.empty();
            }
            
            String indexConfigJson = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
            
            log.debug("Retrieved index config {} from etcd", indexName);
            return Optional.of(indexConfigJson);
            
        } catch (Exception e) {
            log.error("Failed to get index config {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to retrieve index config from etcd", e);
        }
    }
    
    @Override
    public String createIndexConfig(Index index) throws Exception {
        log.info("Creating index config {} in etcd", index.getIndexName());
        
        try {
            String indexPath = pathResolver.getIndexConfPath(index.getIndexName());
            executeEtcdPut(indexPath, objectMapper.writeValueAsString(index));
            
            log.info("Successfully created index config {} in etcd", index.getIndexName());
            return index.getIndexName();
            
        } catch (Exception e) {
            log.error("Failed to create index config {} in etcd: {}", index.getIndexName(), e.getMessage(), e);
            throw new Exception("Failed to create index config in etcd", e);
        }
    }
    
    @Override
    public void updateIndexConfig(Index index) throws Exception {
        log.debug("Updating index config {} in etcd", index.getIndexName());
        
        try {
            String indexPath = pathResolver.getIndexConfPath(index.getIndexName());
            executeEtcdPut(indexPath, objectMapper.writeValueAsString(index));
            
            log.debug("Successfully updated index config {} in etcd", index.getIndexName());
            
        } catch (Exception e) {
            log.error("Failed to update index config {} in etcd: {}", index.getIndexName(), e.getMessage(), e);
            throw new Exception("Failed to update index config in etcd", e);
        }
    }
    
    @Override
    public void deleteIndexConfig(String indexName) throws Exception {
        log.info("Deleting index config {} from etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(indexName);
            executeEtcdDelete(indexPath);
            
            log.info("Successfully deleted index config {} from etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to delete index config {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to delete index config from etcd", e);
        }
    }
    
    @Override
    public void setIndexMappings(String indexName, String mappings) throws Exception {
        log.debug("Setting index mappings for {} in etcd", indexName);
        
        try {
            String mappingsPath = pathResolver.getIndexMappingsPath(indexName);
            executeEtcdPut(mappingsPath, mappings);
            
            log.debug("Successfully set index mappings for {} in etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to set index mappings for {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to set index mappings in etcd", e);
        }
    }
    
    @Override
    public void setIndexSettings(String indexName, String settings) throws Exception {
        log.debug("Setting index settings for {} in etcd", indexName);
        
        try {
            String settingsPath = pathResolver.getIndexSettingsPath(indexName);
            executeEtcdPut(settingsPath, settings);
            
            log.debug("Successfully set index settings for {} in etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to set index settings for {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to set index settings in etcd", e);
        }
    }
    
    // =================================================================
    // SHARD ALLOCATION OPERATIONS
    // =================================================================
    
    @Override
    public List<ShardAllocation> getAllPlannedAllocations(String indexName) throws Exception {
        log.debug("Getting all planned allocations for index {} from etcd", indexName);
        
        try {
            // Get all keys under /<cluster-name>/indices/<index-name>/shard/*/planned-allocation
            String shardPrefix = Paths.get(pathResolver.getIndicesPrefix(), indexName, "shard").toString();
            GetResponse response = executeEtcdPrefixQuery(shardPrefix);
            
            List<ShardAllocation> allocations = new ArrayList<>();
            for (var kv : response.getKvs()) {
                String key = kv.getKey().toString(StandardCharsets.UTF_8);
                String value = kv.getValue().toString(StandardCharsets.UTF_8);
                
                // Check if this is a planned-allocation key
                if (key.endsWith("/planned-allocation")) {
                    try {
                        ShardAllocation allocation = objectMapper.readValue(value, ShardAllocation.class);
                        allocations.add(allocation);
                    } catch (Exception e) {
                        log.warn("Failed to parse planned allocation JSON for key {}: {}", key, e.getMessage());
                    }
                }
            }
            
            log.debug("Retrieved {} planned allocations for index {} from etcd", allocations.size(), indexName);
            return allocations;
            
        } catch (Exception e) {
            log.error("Failed to get planned allocations for index {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to retrieve planned allocations from etcd", e);
        }
    }
    
    @Override
    public void deletePlannedAllocation(String indexName, String shardId) throws Exception {
        log.debug("Deleting planned allocation for index {} shard {} from etcd", indexName, shardId);
        
        try {
            String allocationPath = pathResolver.getShardPlannedAllocationPath(indexName, shardId);
            executeEtcdDelete(allocationPath);
            
            log.debug("Successfully deleted planned allocation for index {} shard {} from etcd", indexName, shardId);
            
        } catch (Exception e) {
            log.error("Failed to delete planned allocation for index {} shard {} from etcd: {}", 
                indexName, shardId, e.getMessage(), e);
            throw new Exception("Failed to delete planned allocation from etcd", e);
        }
    }
    
    // =================================================================
    // CLUSTER OPERATIONS
    // =================================================================
    
    @Override
    public void initialize() throws Exception {
        log.info("Initialize called - already done in constructor");
        // Start leader election process
        startLeaderElection();
    }
    
    @Override
    public void close() throws Exception {
        log.info("Closing etcd metadata store");
        
        try {
            if (etcdClient != null) {
                etcdClient.close();
                log.info("etcd client closed successfully");
            }
        } catch (Exception e) {
            log.error("Error closing etcd client: {}", e.getMessage(), e);
            throw new Exception("Failed to close etcd client", e);
        }
    }
    
    @Override
    public String getClusterName() {
        return clusterName;
    }
    
    /**
     * Get the path resolver for external use
     */
    public EtcdPathResolver getPathResolver() {
        return pathResolver;
    }
    
    // =================================================================
    // PRIVATE HELPER METHODS FOR ETCD OPERATIONS
    // =================================================================
    
    /**
     * Executes etcd prefix query to retrieve all keys matching the given prefix
     */
    private GetResponse executeEtcdPrefixQuery(String prefix) throws Exception {
        // Add trailing slash for etcd prefix queries to ensure precise matching
        String prefixWithSlash = prefix + PATH_DELIMITER;
        ByteSequence prefixBytes = ByteSequence.from(prefixWithSlash, StandardCharsets.UTF_8);
        return kvClient.get(
            prefixBytes,
            GetOption.newBuilder().withPrefix(prefixBytes).build()
        ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Executes etcd get operation for a single key
     */
    private GetResponse executeEtcdGet(String key) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        return kvClient.get(keyBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Executes etcd put operation for a key-value pair
     */
    private void executeEtcdPut(String key, String value) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        ByteSequence valueBytes = ByteSequence.from(value, StandardCharsets.UTF_8);
        kvClient.put(keyBytes, valueBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Executes etcd delete operation for a key
     */
    private void executeEtcdDelete(String key) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        kvClient.delete(keyBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Deserializes list of objects from etcd GetResponse
     */
    private <T> List<T> deserializeObjectList(GetResponse response, Class<T> clazz) throws Exception {
        List<T> items = new ArrayList<>();
        for (var kv : response.getKvs()) {
            String json = kv.getValue().toString(StandardCharsets.UTF_8);
            T item = objectMapper.readValue(json, clazz);
            items.add(item);
        }
        return items;
    }
    
    /**
     * Deserializes single object from etcd GetResponse
     */
    private <T> Optional<T> deserializeObject(GetResponse response, Class<T> clazz) throws Exception {
        if (response.getCount() == 0) {
            return Optional.empty();
        }
        String json = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        T item = objectMapper.readValue(json, clazz);
        return Optional.of(item);
    }
    
    /**
     * Retrieves all objects of a specific type using etcd prefix query
     */
    private <T> List<T> getAllObjectsByPrefix(String prefix, Class<T> clazz) throws Exception {
        GetResponse response = executeEtcdPrefixQuery(prefix);
        return deserializeObjectList(response, clazz);
    }
    
    /**
     * Retrieves single object by etcd path
     */
    private <T> Optional<T> getObjectByPath(String path, Class<T> clazz) throws Exception {
        GetResponse response = executeEtcdGet(path);
        return deserializeObject(response, clazz);
    }
    
    /**
     * Stores object as JSON at the specified etcd path
     */
    private void storeObjectAsJson(String path, Object object) throws Exception {
        String json = objectMapper.writeValueAsString(object);
        executeEtcdPut(path, json);
    }

     // =================================================================
    // CONTROLLER TASKS OPERATIONS
    // =================================================================
    public CompletableFuture<Boolean> startLeaderElection() {
        Election election = etcdClient.getElectionClient();
        String electionKey = clusterName + ELECTION_KEY_SUFFIX;

        CompletableFuture<Boolean> result = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                ByteSequence electionKeyBytes = ByteSequence.from(electionKey, UTF_8);
                ByteSequence nodeIdBytes = ByteSequence.from(nodeId, UTF_8);

                long ttlSeconds = LEADER_ELECTION_TTL_SECONDS;
                LeaseGrantResponse leaseGrant = etcdClient.getLeaseClient()
                        .grant(ttlSeconds)
                        .get();
                long leaseId = leaseGrant.getID();

                etcdClient.getLeaseClient().keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
                    @Override
                    public void onNext(LeaseKeepAliveResponse res) {}
                    @Override
                    public void onError(Throwable t) {
                        log.error("KeepAlive error: {}", t.getMessage());
                        isLeader.set(false);
                        result.completeExceptionally(t);
                    }
                    @Override
                    public void onCompleted() {
                        isLeader.set(false);
                    }
                });

                election.campaign(electionKeyBytes, leaseId, nodeIdBytes)
                        .thenAccept(leaderKey -> {
                            log.info("Node {} is the LEADER.", nodeId);
                            isLeader.set(true);
                            result.complete(true);
                        })
                        .exceptionally(ex -> {
                            result.completeExceptionally(ex);
                            return null;
                        });

            } catch (Exception e) {
                log.error("Leader election error", e);
                result.completeExceptionally(e);
            }
        });

        return result;
    }


    public boolean isLeader() {
        return isLeader.get();
    }
    
}
