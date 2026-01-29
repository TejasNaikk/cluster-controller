package io.clustercontroller.metrics;

import io.clustercontroller.enums.NodeRole;

import java.util.HashMap;
import java.util.Map;

import static io.clustercontroller.metrics.MetricsConstants.CLUSTER_ID_TAG;
import static io.clustercontroller.metrics.MetricsConstants.INDEX_NAME_TAG;
import static io.clustercontroller.metrics.MetricsConstants.NODE_NAME_TAG;
import static io.clustercontroller.metrics.MetricsConstants.ROLE_TAG;
import static io.clustercontroller.metrics.MetricsConstants.SHARD_ID_TAG;

/**
 * Utility class for handling metrics.
 */
public class MetricsUtils {
    /**
     * Builds a map of metrics tags including cluster ID, index name, shard ID, and node role.
     *
     * @param clusterId the cluster ID
     * @param indexName the index name
     * @param shardId the shard ID
     * @param role the node role
     * @return a map of metrics tags
     */
    public static Map<String, String> buildMetricsTagsByRole(String clusterId, String indexName, String shardId, NodeRole role) {
        Map<String, String> tags = buildMetricsTags(clusterId, indexName, shardId);
        tags.put(ROLE_TAG, role.getValue());
        return tags;
    }

    /**
     * Builds a map of metrics tags including cluster ID, index name, and shard ID.
     *
     * @param clusterId the cluster ID
     * @param indexName the index name
     * @param shardId the shard ID
     * @return a map of metrics tags
     */
    public static Map<String, String> buildMetricsTags(String clusterId, String indexName, String shardId) {
        Map<String, String> tags = new HashMap<>();
        tags.put(CLUSTER_ID_TAG, clusterId);
        tags.put(INDEX_NAME_TAG, indexName);
        tags.put(SHARD_ID_TAG, shardId);
        return tags;
    }
    
    /**
     * Builds a map of metrics tags including cluster ID, index name, shard ID, and node name.
     * Used for replica-level metrics where we need to identify specific nodes.
     *
     * @param clusterId the cluster ID
     * @param indexName the index name
     * @param shardId the shard ID
     * @param nodeName the node name
     * @return a map of metrics tags
     */
    public static Map<String, String> buildMetricsTagsWithNode(String clusterId, String indexName, String shardId, String nodeName) {
        Map<String, String> tags = buildMetricsTags(clusterId, indexName, shardId);
        tags.put(NODE_NAME_TAG, nodeName);
        return tags;
    }
    
    /**
     * Builds a map of metrics tags for node-level metrics.
     *
     * @param clusterId the cluster ID
     * @param nodeName the node name
     * @param role the node role
     * @return a map of metrics tags
     */
    public static Map<String, String> buildNodeMetricsTags(String clusterId, String nodeName, String role) {
        Map<String, String> tags = new HashMap<>();
        tags.put(CLUSTER_ID_TAG, clusterId);
        tags.put(NODE_NAME_TAG, nodeName);
        tags.put(ROLE_TAG, role);
        return tags;
    }
    
    /**
     * Builds a map of metrics tags for index-level metrics.
     *
     * @param clusterId the cluster ID
     * @param indexName the index name
     * @return a map of metrics tags
     */
    public static Map<String, String> buildIndexMetricsTags(String clusterId, String indexName) {
        Map<String, String> tags = new HashMap<>();
        tags.put(CLUSTER_ID_TAG, clusterId);
        tags.put(INDEX_NAME_TAG, indexName);
        return tags;
    }
}
