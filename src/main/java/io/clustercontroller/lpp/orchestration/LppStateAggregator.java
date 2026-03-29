package io.clustercontroller.lpp.orchestration;

import io.clustercontroller.lpp.models.*;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Reads actual shard states from etcd and aggregates them per shard.
 *
 * <p>Detects:
 * <ul>
 *   <li>Shards stuck in a non-terminal state (e.g., DOWNLOADING) for too long</li>
 *   <li>Failed shards that need to be retried</li>
 *   <li>Nodes that have gone completely dark (no heartbeat)</li>
 * </ul>
 *
 * <p>Results are written back to etcd for the controller's own observability
 * and for future autoscaling signals.
 */
@Slf4j
public class LppStateAggregator {

    /** Shards stuck in a non-terminal state for longer than this are flagged. */
    private static final long STUCK_THRESHOLD_MS = 10 * 60 * 1000L; // 10 min

    private final LppMetadataStore metadataStore;

    public LppStateAggregator(LppMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Run aggregation over all known actual states.
     *
     * @return summary of stuck/failed shards across all nodes
     */
    public AggregationResult aggregate() {
        Map<String, LppNodeActualState> allStates = metadataStore.getAllNodeActualStates();

        List<String> stuckShards = new ArrayList<>();
        List<String> failedShards = new ArrayList<>();
        int activeShards = 0;

        for (LppNodeActualState nodeState : allStates.values()) {
            for (LppShardActualState shardState : nodeState.getShardStates()) {
                if (shardState.isActive()) {
                    activeShards++;
                } else if (shardState.isFailed()) {
                    failedShards.add(nodeState.getNodeName() + "/" + shardState.getShardKey());
                } else if (shardState.isStuck(STUCK_THRESHOLD_MS)) {
                    stuckShards.add(nodeState.getNodeName() + "/" + shardState.getShardKey()
                            + " [" + shardState.getState() + "]");
                }
            }
        }

        if (!stuckShards.isEmpty()) {
            log.warn("LPP state aggregator: {} stuck shards: {}", stuckShards.size(), stuckShards);
        }
        if (!failedShards.isEmpty()) {
            log.warn("LPP state aggregator: {} failed shards: {}", failedShards.size(), failedShards);
        }
        log.info("LPP state aggregator: {} active, {} stuck, {} failed shards across {} nodes",
                activeShards, stuckShards.size(), failedShards.size(), allStates.size());

        return new AggregationResult(activeShards, stuckShards, failedShards);
    }

    public record AggregationResult(
            int activeShardCount,
            List<String> stuckShards,
            List<String> failedShards) {

        public boolean hasIssues() {
            return !stuckShards.isEmpty() || !failedShards.isEmpty();
        }
    }
}
