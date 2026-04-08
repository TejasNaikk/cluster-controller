package io.clustercontroller.lpp.tasks;

import io.clustercontroller.lpp.models.LppRoutingTable;
import lombok.extern.slf4j.Slf4j;

/**
 * Computes and persists the LPP routing table goal state.
 *
 * <p>Runs after {@link LppOrchestrationTask} and shadow simulation so it sees the
 * most up-to-date actual states (including simulator-written ACTIVE states).
 */
@Slf4j
public class LppRoutingTableTask {

    public static final String NAME = "lpp-routing-table";

    private final LppTaskContext ctx;

    public LppRoutingTableTask(LppTaskContext ctx) {
        this.ctx = ctx;
    }

    public String execute() {
        try {
            if (ctx.getCurrentAllocations().isEmpty()) {
                log.debug("LPP routing table task: no allocations, skipping");
                return "SKIPPED";
            }

            LppRoutingTable table = ctx.getRoutingTableOrchestrator()
                    .computeAndPersist(ctx.getCurrentAllocations());

            int totalShards = table.getRoutes().values().stream()
                    .mapToInt(shards -> shards.size())
                    .sum();
            int routableShards = (int) table.getRoutes().values().stream()
                    .flatMap(shards -> shards.values().stream())
                    .filter(nodes -> !nodes.isEmpty())
                    .count();

            log.info("LPP routing table task: version={}, {}/{} shards have routes",
                    table.getVersion(), routableShards, totalShards);

            return routableShards == totalShards ? "COMPLETE" : "PARTIAL";
        } catch (Exception e) {
            log.error("LPP routing table task failed", e);
            return "FAILED";
        }
    }

    public String getName() { return NAME; }
}
