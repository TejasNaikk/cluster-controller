package io.clustercontroller.lpp.tasks;

import io.clustercontroller.lpp.models.LppGroup;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Discovers LPP nodes from Grail, merges with etcd actual-states, builds group topology.
 * Stores results in {@link LppTaskContext} for downstream tasks in the same cycle.
 */
@Slf4j
public class LppDiscoveryTask {

    public static final String NAME = "lpp-discovery";

    private final LppTaskContext ctx;

    public LppDiscoveryTask(LppTaskContext ctx) {
        this.ctx = ctx;
    }

    public String execute() {
        try {
            Map<String, LppGroup> groups = ctx.getDiscovery().discover();
            ctx.setCurrentGroups(groups);
            log.info("LPP discovery task complete: {} groups", groups.size());
            return "SUCCESS";
        } catch (Exception e) {
            log.error("LPP discovery task failed", e);
            return "FAILED";
        }
    }

    public String getName() { return NAME; }
}
