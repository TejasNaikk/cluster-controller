package io.clustercontroller.lpp.tasks;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Pushes one node's goal state (manifest) to etcd per invocation (rolling update).
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
            if (ctx.getCurrentAllocations().isEmpty()) {
                log.debug("LPP orchestration task: no allocations to push");
                return "SKIPPED";
            }

            Optional<String> updated = ctx.getOrchestrator().orchestrate(
                    ctx.getCurrentAllocations(),
                    ctx.getCurrentGroups(),
                    ctx.getRegion());

            if (updated.isPresent()) {
                log.info("LPP orchestration task: pushed goal state to node {}", updated.get());
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
