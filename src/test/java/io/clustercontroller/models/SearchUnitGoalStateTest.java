package io.clustercontroller.models;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SearchUnitGoalStateTest {

    // ========== equals() tests ==========

    @Test
    void testEqualsWithSameLocalShards() {
        SearchUnitGoalState state1 = createGoalState("index1", "0", "PRIMARY");
        SearchUnitGoalState state2 = createGoalState("index1", "0", "PRIMARY");
        
        assertThat(state1).isEqualTo(state2);
        assertThat(state1.hashCode()).isEqualTo(state2.hashCode());
    }

    @Test
    void testEqualsIgnoresLastUpdatedAndVersion() {
        SearchUnitGoalState state1 = createGoalState("index1", "0", "PRIMARY");
        state1.setLastUpdated("2024-01-01T00:00:00Z");
        state1.setVersion(1);
        
        SearchUnitGoalState state2 = createGoalState("index1", "0", "PRIMARY");
        state2.setLastUpdated("2024-12-31T23:59:59Z");
        state2.setVersion(999);
        
        // Should be equal because localShards are the same
        assertThat(state1).isEqualTo(state2);
    }

    @Test
    void testNotEqualsWithDifferentShardId() {
        SearchUnitGoalState state1 = createGoalState("index1", "0", "PRIMARY");
        SearchUnitGoalState state2 = createGoalState("index1", "1", "PRIMARY");
        
        assertThat(state1).isNotEqualTo(state2);
    }

    @Test
    void testNotEqualsWithDifferentRole() {
        SearchUnitGoalState state1 = createGoalState("index1", "0", "PRIMARY");
        SearchUnitGoalState state2 = createGoalState("index1", "0", "SEARCH_REPLICA");
        
        assertThat(state1).isNotEqualTo(state2);
    }

    @Test
    void testNotEqualsWithDifferentIndex() {
        SearchUnitGoalState state1 = createGoalState("index1", "0", "PRIMARY");
        SearchUnitGoalState state2 = createGoalState("index2", "0", "PRIMARY");
        
        assertThat(state1).isNotEqualTo(state2);
    }

    @Test
    void testEqualsWithMultipleShards() {
        SearchUnitGoalState state1 = new SearchUnitGoalState();
        Map<String, Map<String, String>> localShards1 = new HashMap<>();
        localShards1.put("index1", new HashMap<>());
        localShards1.get("index1").put("0", "PRIMARY");
        localShards1.get("index1").put("1", "SEARCH_REPLICA");
        localShards1.put("index2", new HashMap<>());
        localShards1.get("index2").put("0", "PRIMARY");
        state1.setLocalShards(localShards1);
        
        SearchUnitGoalState state2 = new SearchUnitGoalState();
        Map<String, Map<String, String>> localShards2 = new HashMap<>();
        localShards2.put("index1", new HashMap<>());
        localShards2.get("index1").put("0", "PRIMARY");
        localShards2.get("index1").put("1", "SEARCH_REPLICA");
        localShards2.put("index2", new HashMap<>());
        localShards2.get("index2").put("0", "PRIMARY");
        state2.setLocalShards(localShards2);
        
        assertThat(state1).isEqualTo(state2);
    }

    @Test
    void testEqualsWithBothNullLocalShards() {
        SearchUnitGoalState state1 = new SearchUnitGoalState();
        state1.setLocalShards(null);
        
        SearchUnitGoalState state2 = new SearchUnitGoalState();
        state2.setLocalShards(null);
        
        assertThat(state1).isEqualTo(state2);
    }

    @Test
    void testNotEqualsWithOneNullLocalShards() {
        SearchUnitGoalState state1 = createGoalState("index1", "0", "PRIMARY");
        
        SearchUnitGoalState state2 = new SearchUnitGoalState();
        state2.setLocalShards(null);
        
        assertThat(state1).isNotEqualTo(state2);
    }

    @Test
    void testEqualsWithEmptyLocalShards() {
        SearchUnitGoalState state1 = new SearchUnitGoalState();
        state1.setLocalShards(new HashMap<>());
        
        SearchUnitGoalState state2 = new SearchUnitGoalState();
        state2.setLocalShards(new HashMap<>());
        
        assertThat(state1).isEqualTo(state2);
    }

    // ========== hasShardWithRole() tests ==========

    @Test
    void testHasShardWithRoleReturnsTrue() {
        SearchUnitGoalState state = createGoalState("index1", "0", "PRIMARY");
        
        assertThat(state.hasShardWithRole("index1", "0", "PRIMARY")).isTrue();
    }

    @Test
    void testHasShardWithRoleReturnsFalseForDifferentRole() {
        SearchUnitGoalState state = createGoalState("index1", "0", "PRIMARY");
        
        // Same index and shard, but different role
        assertThat(state.hasShardWithRole("index1", "0", "SEARCH_REPLICA")).isFalse();
    }

    @Test
    void testHasShardWithRoleReturnsFalseForDifferentShard() {
        SearchUnitGoalState state = createGoalState("index1", "0", "PRIMARY");
        
        assertThat(state.hasShardWithRole("index1", "1", "PRIMARY")).isFalse();
    }

    @Test
    void testHasShardWithRoleReturnsFalseForDifferentIndex() {
        SearchUnitGoalState state = createGoalState("index1", "0", "PRIMARY");
        
        assertThat(state.hasShardWithRole("index2", "0", "PRIMARY")).isFalse();
    }

    @Test
    void testHasShardWithRoleReturnsFalseForNullLocalShards() {
        SearchUnitGoalState state = new SearchUnitGoalState();
        state.setLocalShards(null);
        
        assertThat(state.hasShardWithRole("index1", "0", "PRIMARY")).isFalse();
    }

    @Test
    void testHasShardWithRoleReturnsFalseForEmptyLocalShards() {
        SearchUnitGoalState state = new SearchUnitGoalState();
        state.setLocalShards(new HashMap<>());
        
        assertThat(state.hasShardWithRole("index1", "0", "PRIMARY")).isFalse();
    }

    @Test
    void testHasShardWithRoleWithMultipleShards() {
        SearchUnitGoalState state = new SearchUnitGoalState();
        Map<String, Map<String, String>> localShards = new HashMap<>();
        localShards.put("index1", new HashMap<>());
        localShards.get("index1").put("0", "PRIMARY");
        localShards.get("index1").put("1", "SEARCH_REPLICA");
        localShards.put("index2", new HashMap<>());
        localShards.get("index2").put("0", "PRIMARY");
        state.setLocalShards(localShards);
        
        assertThat(state.hasShardWithRole("index1", "0", "PRIMARY")).isTrue();
        assertThat(state.hasShardWithRole("index1", "1", "SEARCH_REPLICA")).isTrue();
        assertThat(state.hasShardWithRole("index2", "0", "PRIMARY")).isTrue();
        assertThat(state.hasShardWithRole("index2", "0", "SEARCH_REPLICA")).isFalse();
        assertThat(state.hasShardWithRole("index3", "0", "PRIMARY")).isFalse();
    }

    // ========== Helper methods ==========

    private SearchUnitGoalState createGoalState(String indexName, String shardId, String role) {
        SearchUnitGoalState state = new SearchUnitGoalState();
        Map<String, Map<String, String>> localShards = new HashMap<>();
        localShards.put(indexName, new HashMap<>());
        localShards.get(indexName).put(shardId, role);
        state.setLocalShards(localShards);
        return state;
    }
}
