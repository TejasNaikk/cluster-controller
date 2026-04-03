package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Index definition registered with the LPP controller.
 *
 * <p>Stored in etcd at /lpp/{env}/indices/{collection}.{fullIndexName}/conf.
 *
 * <h3>Per-shard scale</h3>
 * <p>{@code scalePerShard} is the primary way to set group scale. It is a list whose
 * i-th entry is the number of groups that should serve shard i.
 * Example: {@code [2, 3, 2]} → shard 0 gets 2 groups, shard 1 gets 3, shard 2 gets 2.
 *
 * <p>If {@code scalePerShard} is absent or shorter than {@code numShards}, the controller
 * falls back to {@code numIngestGroups} (uniform scale) for the missing entries.
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

    /**
     * Uniform scale fallback — number of groups per shard when {@code scalePerShard}
     * does not specify an entry for that shard.
     */
    @JsonProperty("num_ingest_groups")
    private int numIngestGroups = 1;

    /**
     * Per-shard group scale. Entry i = number of groups for shard i.
     * Takes precedence over {@code numIngestGroups} when present.
     */
    @JsonProperty("scale_per_shard")
    private List<Integer> scalePerShard = new ArrayList<>();

    /**
     * In reader-writer separation mode, number of dedicated search groups per shard.
     * Ignored in hybrid mode.
     */
    @JsonProperty("num_search_groups")
    private int numSearchGroups = 1;

    public LppIndexDefinition(String collection, String indexName, String fullIndexName,
                               int numShards, int numIngestGroups, int numSearchGroups) {
        this.collection = collection;
        this.indexName = indexName;
        this.fullIndexName = fullIndexName;
        this.numShards = numShards;
        this.numIngestGroups = numIngestGroups;
        this.numSearchGroups = numSearchGroups;
    }

    public LppIndexDefinition(String collection, String indexName, String fullIndexName,
                               int numShards, int numGroups) {
        this(collection, indexName, fullIndexName, numShards, numGroups, numGroups);
    }

    /**
     * Returns the number of groups to assign to the given shard.
     * Uses {@code scalePerShard[shardId]} if defined, otherwise {@code numIngestGroups}.
     */
    @JsonIgnore
    public int getShardScale(int shardId) {
        if (scalePerShard != null && shardId < scalePerShard.size()) {
            return scalePerShard.get(shardId);
        }
        return numIngestGroups;
    }

    /** etcd key: collection.fullIndexName */
    @JsonIgnore
    public String getKey() {
        return collection + "." + fullIndexName;
    }
}
