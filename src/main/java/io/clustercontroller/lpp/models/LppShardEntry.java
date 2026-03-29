package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A single shard entry — the unit of allocation in LPP.
 *
 * <p>Key = "{collection}.{indexName}.{shardId}" (e.g., "grocery.local_index.0").
 * This is the same key format used by LppShardSlot on the data plane.
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

    /** Canonical key: collection.indexName.shardId */
    public String getKey() {
        return collection + "." + indexName + "." + shardId;
    }
}
