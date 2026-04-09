package io.clustercontroller.lpp.tasks;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Pushes goal states to divergent nodes per invocation (20%-per-group rolling update).
 * Reads planned allocations from context (set by {@link LppAllocationTask}).
 */
@Slf4j
public class LppOrchestrationTask {

    public static final String NAME = "lpp-orchestration";

    private final LppTaskContext ctx;

    public LppOrchestrationTask(LppTaskContext ctx) {
        this.ctx = ctx;
    }

    public String execute() {
        try {
            // Always run orchestration even with empty allocations so orphaned goal-states
            // from previously registered indices get cleaned up.
            List<String> updated = ctx.getOrchestrator().orchestrate(
                    ctx.getCurrentAllocations(),
                    ctx.getCurrentGroups(),
                    ctx.getRegion());

            if (!updated.isEmpty()) {
                log.info("LPP orchestration task: pushed goal state to {} node(s): {}", updated.size(), updated);
                return "SUCCESS";
            } else {
                log.info("LPP orchestration task: all nodes converged");
                return "CONVERGED";
            }
        } catch (Exception e) {
            log.error("LPP orchestration task failed", e);
            return "FAILED";
        }
    }

    public String getName() { return NAME; }
}
