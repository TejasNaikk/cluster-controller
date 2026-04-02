package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Index definition registered with the LPP controller.
 *
 * <p>Stored in etcd at /lpp/{env}/indices/{collection}.{fullIndexName}/conf.
 * One registration per concrete index version — e.g. "grocery.local_index.1"
 * and "grocery.local_index.2" are two separate registrations that can coexist
 * during an upgrade.
 *
 * <p>indexName is the logical alias ("local_index"). fullIndexName is the concrete
 * versioned index ("local_index.1"). Shards and allocations are always keyed by
 * fullIndexName so versions don't collide.
 *
 * <h3>Group replication</h3>
 * <p>Each shard is served by two independent sets of groups:
 * <ul>
 *   <li>{@code numIngestGroups} — groups handling the write/ingest path for this shard.
 *       These nodes receive segment data from the pipeline.</li>
 *   <li>{@code numSearchGroups} — groups handling read/search queries for this shard.
 *       Can overlap with ingest groups (and does, today, since all groups do both).</li>
 * </ul>
 *
 * <p>For now all groups handle both ingest and search, so setting both to the same
 * value is the expected configuration. Separate counts exist so that in the future
 * we can have dedicated ingest-only or search-only groups without changing the schema.
 *
 * <p>Example: 6 shards, 2 ingest groups per shard, 2 search groups per shard.
 * With 6 available groups this distributes load evenly.
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
     * Number of replica groups serving the ingest/write path per shard.
     * All groups handle both ingest and search today; this controls replication factor.
     */
    @JsonProperty("num_ingest_groups")
    private int numIngestGroups = 1;

    /**
     * Number of replica groups serving the search/read path per shard.
     * Defaults to the same as numIngestGroups. Set higher for read-heavy indices.
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

    /**
     * Convenience constructor for symmetric replication (same group count for ingest and search).
     * This is the typical configuration when all groups do both ingest and search.
     */
    public LppIndexDefinition(String collection, String indexName, String fullIndexName,
                               int numShards, int numGroups) {
        this(collection, indexName, fullIndexName, numShards, numGroups, numGroups);
    }

    /** etcd key: collection.fullIndexName — e.g. "grocery.local_index.1" */
    @JsonIgnore
    public String getKey() {
        return collection + "." + fullIndexName;
    }
}
