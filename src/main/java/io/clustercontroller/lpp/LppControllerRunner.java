package io.clustercontroller.lpp;

import io.clustercontroller.lpp.tasks.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * LPP controller main loop. Activated by the {@code lpp} Spring profile.
 *
 * <p>Each iteration runs the four LPP tasks in priority order:
 * <ol>
 *   <li>Discovery   — pull topology from Grail, prune stale nodes</li>
 *   <li>Allocation  — bin-pack shards onto groups</li>
 *   <li>Orchestration — push goal state to one divergent node (rolling update)</li>
 *   <li>State aggregation — count active/stuck/failed shards</li>
 * </ol>
 */
@Slf4j
@Component
@Profile("lpp")
public class LppControllerRunner {

    private final LppTaskContext ctx;
    private final long intervalSeconds;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "lpp-controller-loop"));

    public LppControllerRunner(
            LppTaskContext ctx,
            @Value("${lpp.intervalSeconds:30}") long intervalSeconds) {
        this.ctx = ctx;
        this.intervalSeconds = intervalSeconds;
    }

    @PostConstruct
    public void start() {
        log.info("LPP controller starting (namespace={}, region={}, intervalSeconds={})",
                ctx.getNamespace(), ctx.getRegion(), intervalSeconds);
        scheduler.scheduleWithFixedDelay(this::runLoop, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        log.info("LPP controller stopping");
        scheduler.shutdown();
    }

    private void runLoop() {
        try {
            log.info("LPP loop start");

            String discovery = new LppDiscoveryTask(ctx).execute();
            log.info("LPP discovery → {}", discovery);

            String allocation = new LppAllocationTask(ctx).execute();
            log.info("LPP allocation → {}", allocation);

            String orchestration = new LppOrchestrationTask(ctx).execute();
            log.info("LPP orchestration → {}", orchestration);

            String aggregation = new LppStateAggregationTask(ctx).execute();
            log.info("LPP state-aggregation → {}", aggregation);

            log.info("LPP loop done");
        } catch (Exception e) {
            log.error("LPP loop unexpected error", e);
        }
    }
}
