package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A single shard entry — the unit of allocation in LPP.
 *
 * <p>Key = "{collection}.{fullIndexName}.{shardId}"
 * e.g. "grocery.local_index.1.3" — collection=grocery, concrete index=local_index.1, shard=3.
 *
 * <p>The key uses fullIndexName (the concrete versioned index), NOT indexName (the logical alias).
 * This is critical because local_index.1 shard 3 and local_index.2 shard 3 are completely
 * different shards with different segment data. During an index upgrade both versions coexist
 * simultaneously in the system and must have distinct keys.
 *
 * <p>indexName ("local_index") is kept as metadata for routing alias resolution — the gateway
 * can look up "which fullIndexName does local_index currently point to" to find the active version.
 */
@Data
@NoArgsConstructor
public class LppShardEntry {

    @JsonProperty("collection")
    private String collection;

    @JsonProperty("index_name")
    private String indexName;

    /** The versioned index name currently active (e.g., "local_index.2025_01_24-12_47_01_PDT"). */
    @JsonProperty("full_index_name")
    private String fullIndexName;

    @JsonProperty("shard_id")
    private int shardId;

    /** Remote storage path override. Empty means use default derived from index settings. */
    @JsonProperty("remote_path")
    private String remotePath = "";

    public LppShardEntry(String collection, String indexName, String fullIndexName, int shardId) {
        this.collection = collection;
        this.indexName = indexName;
        this.fullIndexName = fullIndexName;
        this.shardId = shardId;
    }

    /** Canonical key: collection.fullIndexName.shardId — e.g. "grocery.local_index.1.3" */
    public String getKey() {
        return collection + "." + fullIndexName + "." + shardId;
    }
}
