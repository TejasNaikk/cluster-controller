package io.clustercontroller.indices;

import io.clustercontroller.models.Index;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IndexManagerTest {

    @Mock
    private MetadataStore metadataStore;

    private IndexManager indexManager;

    @BeforeEach
    void setUp() {
        indexManager = new IndexManager(metadataStore);
    }

    @Test
    void testCreateIndex_Success() throws Exception {
        // Given
        String indexName = "test-index";
        List<SearchUnit> availableSearchUnits = createMockSearchUnits();
        String createIndexRequestJson = """
            {
                "index_name": "test-index",
                "mappings": "{\\"properties\\": {\\"field1\\": {\\"type\\": \\"text\\"}}}",
                "settings": "{\\"number_of_shards\\": 1}"
            }
            """;

        // Mock dependencies
        when(metadataStore.getIndexConfig(indexName)).thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnits()).thenReturn(availableSearchUnits);
        when(metadataStore.createIndexConfig(any(Index.class))).thenReturn("doc-id-123");

        // When
        indexManager.createIndex(createIndexRequestJson);

        // Then
        ArgumentCaptor<Index> indexCaptor = ArgumentCaptor.forClass(Index.class);
        verify(metadataStore).createIndexConfig(indexCaptor.capture());

        Index capturedIndex = indexCaptor.getValue();
        assertThat(capturedIndex.getIndexName()).isEqualTo(indexName);
        assertThat(capturedIndex.getShardReplicaCount()).isNotNull();
        assertThat(capturedIndex.getAllocationPlan()).isNotNull();
        assertThat(capturedIndex.getCreatedAt()).isNotNull();

        verify(metadataStore).getAllSearchUnits();
        
        // Verify that setIndexMappings and setIndexSettings are called with correct values
        verify(metadataStore).setIndexMappings("test-index", "{\"properties\": {\"field1\": {\"type\": \"text\"}}}");
        verify(metadataStore).setIndexSettings("test-index", "{\"number_of_shards\": 1}");
    }


    @Test
    void testCreateIndex_NoAvailableSearchUnits() throws Exception {
        // Given
        String createIndexRequestJson = """
            {
                "index_name": "test-index"
            }
            """;
        List<SearchUnit> emptySearchUnits = new ArrayList<>();

        when(metadataStore.getIndexConfig("test-index")).thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnits()).thenReturn(emptySearchUnits);

        // When/Then
        assertThatThrownBy(() -> indexManager.createIndex(createIndexRequestJson))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("No search units available for index allocation");

        verify(metadataStore).getAllSearchUnits();
        verify(metadataStore, never()).createIndexConfig(any(Index.class));
    }

    @Test
    void testCreateIndex_InvalidJson() {
        // Given
        String invalidJson = "{ invalid json }";

        // When/Then
        assertThatThrownBy(() -> indexManager.createIndex(invalidJson))
                .isInstanceOf(Exception.class);

        // Note: We can't verify method calls that throw exceptions without handling them
    }

    @Test
    void testCreateIndex_IndexAlreadyExists() throws Exception {
        // Given
        String createIndexRequestJson = """
            {
                "index_name": "existing-index"
            }
            """;

        when(metadataStore.getIndexConfig("existing-index"))
                .thenReturn(Optional.of("existing config"));

        // When
        indexManager.createIndex(createIndexRequestJson);

        // Then
        verify(metadataStore).getIndexConfig("existing-index");
        verify(metadataStore, never()).getAllSearchUnits();
        verify(metadataStore, never()).createIndexConfig(any(Index.class));
    }

    @Test
    void testDeleteIndex_Success() throws Exception {
        // Given
        String indexConfig = """
            {
                "index_name": "delete-me-index"
            }
            """;

        // When
        indexManager.deleteIndex(indexConfig);

        // Then
        // Since deleteIndex just logs currently, we verify it doesn't throw
        // In a real implementation, you'd verify the deletion logic
    }

    @Test
    void testCreateIndexRequest_DefaultValues() throws Exception {
        // This test verifies the inner CreateIndexRequest class behavior
        String minimalJson = """
            {
                "index_name": "minimal-index"
            }
            """;

        List<SearchUnit> availableSearchUnits = createMockSearchUnits();
        when(metadataStore.getIndexConfig("minimal-index")).thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnits()).thenReturn(availableSearchUnits);
        when(metadataStore.createIndexConfig(any(Index.class))).thenReturn("doc-id");

        // When
        indexManager.createIndex(minimalJson);

        // Then
        ArgumentCaptor<Index> indexCaptor = ArgumentCaptor.forClass(Index.class);
        verify(metadataStore).createIndexConfig(indexCaptor.capture());

        Index capturedIndex = indexCaptor.getValue();
        assertThat(capturedIndex.getIndexName()).isEqualTo("minimal-index");
    }

    private List<SearchUnit> createMockSearchUnits() {
        List<SearchUnit> searchUnits = new ArrayList<>();
        
        SearchUnit unit1 = new SearchUnit();
        unit1.setName("node-1");
        unit1.setRole("primary");
        unit1.setStatePulled(io.clustercontroller.enums.HealthState.GREEN);
        searchUnits.add(unit1);

        SearchUnit unit2 = new SearchUnit();
        unit2.setName("node-2");
        unit2.setRole("replica");
        unit2.setStatePulled(io.clustercontroller.enums.HealthState.GREEN);
        searchUnits.add(unit2);

        SearchUnit unit3 = new SearchUnit();
        unit3.setName("node-3");
        unit3.setRole("ingest");
        unit3.setStatePulled(io.clustercontroller.enums.HealthState.GREEN);
        searchUnits.add(unit3);

        return searchUnits;
    }

    @Test
    void testCreateIndex_WithoutMappingsAndSettings() throws Exception {
        // Given
        String indexName = "minimal-index";
        String createIndexRequestJson = """
            {
                "index_name": "minimal-index"
            }
            """;
        List<SearchUnit> availableSearchUnits = createMockSearchUnits();

        // Mock dependencies
        when(metadataStore.getIndexConfig(indexName)).thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnits()).thenReturn(availableSearchUnits);
        when(metadataStore.createIndexConfig(any(Index.class))).thenReturn("doc-id");

        // When
        indexManager.createIndex(createIndexRequestJson);

        // Then
        verify(metadataStore).createIndexConfig(any(Index.class));
        verify(metadataStore).getAllSearchUnits();
        
        // Verify that setIndexMappings and setIndexSettings are NOT called when not provided
        verify(metadataStore, never()).setIndexMappings(anyString(), anyString());
        verify(metadataStore, never()).setIndexSettings(anyString(), anyString());
    }

    @Test
    void testCreateIndex_WithOnlyMappings() throws Exception {
        // Given
        String indexName = "mappings-only-index";
        String createIndexRequestJson = """
            {
                "index_name": "mappings-only-index",
                "mappings": "{\\"properties\\": {\\"title\\": {\\"type\\": \\"text\\"}}}"
            }
            """;
        List<SearchUnit> availableSearchUnits = createMockSearchUnits();

        // Mock dependencies
        when(metadataStore.getIndexConfig(indexName)).thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnits()).thenReturn(availableSearchUnits);
        when(metadataStore.createIndexConfig(any(Index.class))).thenReturn("doc-id");

        // When
        indexManager.createIndex(createIndexRequestJson);

        // Then
        verify(metadataStore).createIndexConfig(any(Index.class));
        verify(metadataStore).setIndexMappings("mappings-only-index", "{\"properties\": {\"title\": {\"type\": \"text\"}}}");
        verify(metadataStore, never()).setIndexSettings(anyString(), anyString());
    }

    @Test
    void testCreateIndex_WithOnlySettings() throws Exception {
        // Given
        String indexName = "settings-only-index";
        String createIndexRequestJson = """
            {
                "index_name": "settings-only-index",
                "settings": "{\\"number_of_replicas\\": 2}"
            }
            """;
        List<SearchUnit> availableSearchUnits = createMockSearchUnits();

        // Mock dependencies
        when(metadataStore.getIndexConfig(indexName)).thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnits()).thenReturn(availableSearchUnits);
        when(metadataStore.createIndexConfig(any(Index.class))).thenReturn("doc-id");

        // When
        indexManager.createIndex(createIndexRequestJson);

        // Then
        verify(metadataStore).createIndexConfig(any(Index.class));
        verify(metadataStore, never()).setIndexMappings(anyString(), anyString());
        verify(metadataStore).setIndexSettings("settings-only-index", "{\"number_of_replicas\": 2}");
    }
}