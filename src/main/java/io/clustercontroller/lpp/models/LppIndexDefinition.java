package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Index definition registered with the LPP controller.
 *
 * <p>Stored in etcd at /lpp/{env}/indices/{collection}.{indexName}/conf.
 * Tells the controller how many shards this index has and how many replica groups it needs.
 */
@Data
@NoArgsConstructor
public class LppIndexDefinition {

    @JsonProperty("collection")
    private String collection;

    @JsonProperty("index_name")
    private String indexName;

    /** Versioned index name currently active. */
    @JsonProperty("full_index_name")
    private String fullIndexName;

    /** Total number of shards for this index. */
    @JsonProperty("num_shards")
    private int numShards;

    /** Desired number of replica groups (each group has replicaCount nodes). */
    @JsonProperty("num_groups")
    private int numGroups = 1;

    /** Role of nodes serving this index: INGEST or SEARCH. */
    @JsonProperty("role")
    private String role;

    public LppIndexDefinition(String collection, String indexName, String fullIndexName,
                               int numShards, int numGroups, String role) {
        this.collection = collection;
        this.indexName = indexName;
        this.fullIndexName = fullIndexName;
        this.numShards = numShards;
        this.numGroups = numGroups;
        this.role = role;
    }

    public String getKey() {
        return collection + "." + indexName;
    }
}
