package io.clustercontroller.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.TaskMetadata;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.GetOption;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EtcdMetadataStore using mocked etcd dependencies.
 */
public class EtcdMetadataStoreTest {

    private static final String CLUSTER = "test-cluster";
    private static final String[] ENDPOINTS = new String[]{"http://localhost:2379"};

    private MockedStatic<Client> clientStaticMock;
    private Client mockEtcdClient;
    private KV mockKv;

    @BeforeEach
    public void setUp() throws Exception {
        // Static mock for Client.builder()
        clientStaticMock = Mockito.mockStatic(Client.class);

        // Mock the builder chain Client.builder().endpoints(...).build()
        ClientBuilder mockBuilder = mock(ClientBuilder.class);
        clientStaticMock.when(Client::builder).thenReturn(mockBuilder);
        when(mockBuilder.endpoints(any(String[].class))).thenReturn(mockBuilder);

        // Mock etcd Client and KV
        mockEtcdClient = mock(Client.class);
        mockKv = mock(KV.class);
        when(mockEtcdClient.getKVClient()).thenReturn(mockKv);
        when(mockBuilder.build()).thenReturn(mockEtcdClient);

        // Ensure singleton is reset before each test
        resetSingleton();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (clientStaticMock != null) clientStaticMock.close();
        resetSingleton();
    }

    // ------------------------- helpers -------------------------

    private void resetSingleton() throws Exception {
        Field f = EtcdMetadataStore.class.getDeclaredField("instance");
        f.setAccessible(true);
        f.set(null, null);
    }

    private EtcdMetadataStore newStore() throws Exception {
        return EtcdMetadataStore.createTestInstance(CLUSTER, ENDPOINTS, "test-node", mockEtcdClient, mockKv);
    }

    private GetResponse mockGetResponse(List<KeyValue> kvs) {
        GetResponse resp = mock(GetResponse.class);
        when(resp.getKvs()).thenReturn(kvs);
        when(resp.getCount()).thenReturn((long) kvs.size());
        return resp;
    }

    private KeyValue mockKv(String valueUtf8) {
        KeyValue kv = mock(KeyValue.class);
        ByteSequence val = ByteSequence.from(valueUtf8, StandardCharsets.UTF_8);
        when(kv.getValue()).thenReturn(val);
        return kv;
    }

    private PutResponse mockPutResponse() {
        return mock(PutResponse.class);
    }

    private DeleteResponse mockDeleteResponse(long deleted) {
        DeleteResponse dr = mock(DeleteResponse.class, withSettings().lenient());
        doReturn(deleted).when(dr).getDeleted();
        return dr;
    }

    // ------------------------- singleton tests -------------------------

    @Test
    public void testSingletonGetInstance() throws Exception {
        EtcdMetadataStore s1 = newStore();
        EtcdMetadataStore s2 = EtcdMetadataStore.getInstance(CLUSTER, ENDPOINTS);
        assertThat(s1).isSameAs(s2);
        assertThat(s1.getClusterName()).isEqualTo(CLUSTER);
    }

    @Test
    public void testGetInstanceNoArgsThrowsWhenUninitialized() {
        // Not calling newStore()
        assertThatThrownBy(() -> EtcdMetadataStore.getInstance())
            .isInstanceOf(IllegalStateException.class);
    }

    // ------------------------- tasks -------------------------

    @Test
    public void testGetAllTasksSortedByPriority() throws Exception {
        EtcdMetadataStore store = newStore();

        // Two tasks (priority 5 and 0), expect sorted ascending by priority
        String tHigh = "{\"name\":\"task-high\",\"priority\":5}";
        String tTop = "{\"name\":\"task-top\",\"priority\":0}";

        GetResponse resp = mockGetResponse(Arrays.asList(
                mockKv(tHigh),
                mockKv(tTop)
        ));

        when(mockKv.get(any(ByteSequence.class), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        List<TaskMetadata> tasks = store.getAllTasks();

        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).getName()).isEqualTo("task-top");
        assertThat(tasks.get(0).getPriority()).isEqualTo(0);
        assertThat(tasks.get(1).getName()).isEqualTo("task-high");
        assertThat(tasks.get(1).getPriority()).isEqualTo(5);
    }

    @Test
    public void testGetTaskFound() throws Exception {
        EtcdMetadataStore store = newStore();

        String tJson = "{\"name\":\"index-task\",\"priority\":3}";
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(tJson)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<TaskMetadata> got = store.getTask("index-task");
        assertThat(got).isPresent();
        assertThat(got.get().getName()).isEqualTo("index-task");
        assertThat(got.get().getPriority()).isEqualTo(3);
    }

    @Test
    public void testGetTaskNotFound() throws Exception {
        EtcdMetadataStore store = newStore();

        GetResponse resp = mockGetResponse(Collections.emptyList());
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<TaskMetadata> got = store.getTask("missing");
        assertThat(got).isEmpty();
    }

    @Test
    public void testCreateTaskWritesJsonAndReturnsName() throws Exception {
        EtcdMetadataStore store = newStore();

        // Replace the store's ObjectMapper with a mock to control JSON
        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        TaskMetadata task = mock(TaskMetadata.class);
        when(task.getName()).thenReturn("cleanup-task");
        when(om.writeValueAsString(task)).thenReturn("{\"name\":\"cleanup-task\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        String name = store.createTask(task);
        assertThat(name).isEqualTo("cleanup-task");
        verify(mockKv, times(1)).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testUpdateTaskPutsJson() throws Exception {
        EtcdMetadataStore store = newStore();

        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        TaskMetadata task = mock(TaskMetadata.class);
        when(task.getName()).thenReturn("maintenance-task");
        when(om.writeValueAsString(task)).thenReturn("{\"name\":\"maintenance-task\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        store.updateTask(task);
        verify(mockKv, times(1)).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testDeleteTask() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock successful delete - we don't need to check the response since method returns void
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(DeleteResponse.class)));

        // Call delete method
        store.deleteTask("old-task");
        
        // Verify etcd delete was called with correct parameters
        verify(mockKv, times(1)).delete(any(ByteSequence.class));
    }

    @Test
    public void testDeleteTaskWithException() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock etcd failure
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd connection failed")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.deleteTask("failing-task"))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to delete task from etcd");
    }

    // ------------------------- search units -------------------------

    @Test
    public void testGetAllSearchUnits() throws Exception {
        EtcdMetadataStore store = newStore();

        String u1 = "{\"name\":\"node1\"}";
        String u2 = "{\"name\":\"node2\"}";

        GetResponse resp = mockGetResponse(Arrays.asList(
                mockKv(u1), mockKv(u2)
        ));
        when(mockKv.get(any(ByteSequence.class), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        List<SearchUnit> units = store.getAllSearchUnits();
        assertThat(units).hasSize(2);
        assertThat(units.get(0).getName()).isIn("node1", "node2");
    }

    @Test
    public void testGetSearchUnitFound() throws Exception {
        EtcdMetadataStore store = newStore();

        String json = "{\"name\":\"node3\"}";
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(json)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<SearchUnit> got = store.getSearchUnit("node3");
        assertThat(got).isPresent();
        assertThat(got.get().getName()).isEqualTo("node3");
    }

    @Test
    public void testGetSearchUnitNotFound() throws Exception {
        EtcdMetadataStore store = newStore();

        GetResponse resp = mockGetResponse(Collections.emptyList());
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<SearchUnit> got = store.getSearchUnit("missing");
        assertThat(got).isEmpty();
    }

    @Test
    public void testUpsertSearchUnit() throws Exception {
        EtcdMetadataStore store = newStore();

        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        SearchUnit unit = mock(SearchUnit.class);
        when(om.writeValueAsString(unit)).thenReturn("{\"name\":\"node4\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        store.upsertSearchUnit("node4", unit);
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testUpdateSearchUnit() throws Exception {
        EtcdMetadataStore store = newStore();

        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        SearchUnit unit = mock(SearchUnit.class);
        when(unit.getName()).thenReturn("node5");
        when(om.writeValueAsString(unit)).thenReturn("{\"name\":\"node5\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        store.updateSearchUnit(unit);
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testDeleteSearchUnit() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock successful delete - we don't need to check the response since method returns void
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(DeleteResponse.class)));

        // Call delete method
        store.deleteSearchUnit("node6");
        
        // Verify etcd delete was called with correct parameters
        verify(mockKv).delete(any(ByteSequence.class));
    }

    @Test
    public void testDeleteSearchUnitWithException() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock etcd failure
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd timeout")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.deleteSearchUnit("node7"))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to delete search unit from etcd");
    }

    // ------------------------- index configs -------------------------

    @Test
    public void testGetAllIndexConfigs() throws Exception {
        EtcdMetadataStore store = newStore();

        GetResponse resp = mockGetResponse(Arrays.asList(
                mockKv("{\"settings\":{\"refresh_interval\":\"1s\"}}"),
                mockKv("{\"settings\":{\"number_of_shards\":3}}")
        ));
        when(mockKv.get(any(ByteSequence.class), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        List<String> configs = store.getAllIndexConfigs();
        assertThat(configs).hasSize(2);
        assertThat(configs.get(0)).contains("settings");
    }

    @Test
    public void testGetIndexConfigFound() throws Exception {
        EtcdMetadataStore store = newStore();

        String cfg = "{\"analysis\":{\"analyzer\":{\"std\":\"standard\"}}}";
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(cfg)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<String> got = store.getIndexConfig("user-index");
        assertThat(got).isPresent();
        assertThat(got.get()).contains("analysis");
    }

    @Test
    public void testGetIndexConfigNotFound() throws Exception {
        EtcdMetadataStore store = newStore();

        GetResponse resp = mockGetResponse(Collections.emptyList());
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<String> got = store.getIndexConfig("missing");
        assertThat(got).isEmpty();
    }

    @Test
    public void testCreateIndexConfig() throws Exception {
        EtcdMetadataStore store = newStore();

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        Index testIndex = new Index("test-index", List.of(1));
        String name = store.createIndexConfig(testIndex);
        assertThat(name).isEqualTo("test-index");
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testUpdateIndexConfig() throws Exception {
        EtcdMetadataStore store = newStore();

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        Index testIndex = new Index("user-index", List.of(1));
        store.updateIndexConfig(testIndex);
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testDeleteIndexConfig() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock successful delete - we don't need to check the response since method returns void
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(DeleteResponse.class)));

        // Call delete method
        store.deleteIndexConfig("old-index");
        
        // Verify etcd delete was called with correct parameters
        verify(mockKv).delete(any(ByteSequence.class));
    }

    @Test
    public void testDeleteIndexConfigWithException() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock etcd failure
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd network error")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.deleteIndexConfig("test-index"))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to delete index config from etcd");
    }

    @Test
    public void testSetIndexMappings() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String mappings = "{\"properties\": {\"field1\": {\"type\": \"text\"}}}";

        // Mock successful put response
        PutResponse mockPutResponse = mock(PutResponse.class);
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse));

        // Execute
        store.setIndexMappings(indexName, mappings);

        // Verify the put call was made with correct key and value
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        ArgumentCaptor<ByteSequence> valueCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).put(keyCaptor.capture(), valueCaptor.capture());

        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        String capturedValue = valueCaptor.getValue().toString(UTF_8);

        assertThat(capturedKey).contains("test-cluster/indices/test-index/mappings");
        assertThat(capturedValue).isEqualTo(mappings);
    }

    @Test
    public void testSetIndexMappingsWithException() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String mappings = "{\"properties\": {\"field1\": {\"type\": \"text\"}}}";

        // Mock etcd failure
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd connection failed")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.setIndexMappings(indexName, mappings))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to set index mappings in etcd");
    }

    @Test
    public void testSetIndexSettings() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String settings = "{\"number_of_shards\": 1, \"number_of_replicas\": 2}";

        // Mock successful put response
        PutResponse mockPutResponse = mock(PutResponse.class);
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse));

        // Execute
        store.setIndexSettings(indexName, settings);

        // Verify the put call was made with correct key and value
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        ArgumentCaptor<ByteSequence> valueCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).put(keyCaptor.capture(), valueCaptor.capture());

        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        String capturedValue = valueCaptor.getValue().toString(UTF_8);

        assertThat(capturedKey).contains("test-cluster/indices/test-index/settings");
        assertThat(capturedValue).isEqualTo(settings);
    }

    @Test
    public void testSetIndexSettingsWithException() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String settings = "{\"number_of_shards\": 1}";

        // Mock etcd failure
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd timeout")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.setIndexSettings(indexName, settings))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to set index settings in etcd");
    }

    // ------------------------- lifecycle -------------------------

    @Test
    public void testCloseClosesEtcdClient() throws Exception {
        EtcdMetadataStore store = newStore();
        store.close();
        verify(mockEtcdClient, times(1)).close();
    }

    // ------------------------- reflection util -------------------------

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }
}