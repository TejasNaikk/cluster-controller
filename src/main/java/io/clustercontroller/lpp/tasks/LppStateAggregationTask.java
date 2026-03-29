package io.clustercontroller.lpp.tasks;

import io.clustercontroller.lpp.orchestration.LppStateAggregator;
import lombok.extern.slf4j.Slf4j;

/**
 * Aggregates actual shard states from LPP nodes and logs warnings for stuck/failed shards.
 * This is an observe-only task for now — future iterations will trigger controller actions
 * (e.g., retry failed shards, trigger autoscaling).
 */
@Slf4j
public class LppStateAggregationTask {

    public static final String NAME = "lpp-state-aggregation";

    private final LppTaskContext ctx;

    public LppStateAggregationTask(LppTaskContext ctx) {
        this.ctx = ctx;
    }

    public String execute() {
        try {
            LppStateAggregator.AggregationResult result = ctx.getStateAggregator().aggregate();
            if (result.hasIssues()) {
                log.warn("LPP state aggregation: issues detected — {} stuck, {} failed shards",
                        result.stuckShards().size(), result.failedShards().size());
            }
            return "SUCCESS";
        } catch (Exception e) {
            log.error("LPP state aggregation task failed", e);
            return "FAILED";
        }
    }

    public String getName() { return NAME; }
}
