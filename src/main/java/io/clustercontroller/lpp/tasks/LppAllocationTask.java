package io.clustercontroller.lpp.tasks;

import io.clustercontroller.lpp.models.LppIndexDefinition;
import io.clustercontroller.lpp.models.LppShardPlannedAllocation;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * Runs bin-packing allocation for all registered indices given the current group topology.
 * Reads groups from context (set by {@link LppDiscoveryTask}) and writes planned
 * allocations to etcd.
 */
@Slf4j
public class LppAllocationTask {

    public static final String NAME = "lpp-allocation";

    private final LppTaskContext ctx;

    public LppAllocationTask(LppTaskContext ctx) {
        this.ctx = ctx;
    }

    public String execute() {
        try {
            if (ctx.getCurrentGroups().isEmpty()) {
                log.warn("LPP allocation task: no groups available, skipping");
                return "SKIPPED";
            }

            List<LppIndexDefinition> indices = ctx.loadIndexDefinitions();
            if (indices.isEmpty()) {
                log.warn("LPP allocation task: no index definitions registered, clearing allocations");
                ctx.setCurrentAllocations(Map.of());
                return "SKIPPED";
            }

            Map<String, LppShardPlannedAllocation> allocations =
                    ctx.getAllocator().allocate(ctx.getCurrentGroups(), indices);
            ctx.setCurrentAllocations(allocations);

            log.info("LPP allocation task complete: {} shard allocations", allocations.size());
            return "SUCCESS";
        } catch (Exception e) {
            log.error("LPP allocation task failed", e);
            return "FAILED";
        }
    }

    public String getName() { return NAME; }
}
