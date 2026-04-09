package io.clustercontroller.lpp;

import io.clustercontroller.election.LeaderElection;
import io.clustercontroller.lpp.orchestration.LppShadowNodeSimulator;
import io.clustercontroller.lpp.tasks.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * LPP controller main loop. Activated by the {@code lpp} Spring profile.
 *
 * <p>Each iteration:
 * <ol>
 *   <li>Checks leader election — non-leaders skip the loop entirely.</li>
 *   <li>Discovery   — read actual-state heartbeats from etcd, prune stale nodes, build
 *       role-specific group topology (ingestGroups + searchGroups)</li>
 *   <li>Allocation  — bin-pack shards onto groups (two-pass: ingest then search)</li>
 *   <li>Orchestration — push goal state to divergent nodes (20% rollout per group)</li>
 *   <li>Shadow simulation — if enabled, write fake ACTIVE actual states so the
 *       convergence check can gate the next orchestration batch</li>
 *   <li>State aggregation — count active/stuck/failed shards</li>
 * </ol>
 */
@Slf4j
@Component
@Profile("lpp")
public class LppControllerRunner {

    private final LppTaskContext ctx;
    private final LeaderElection leaderElection;
    private final long intervalSeconds;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "lpp-controller-loop"));

    @Nullable
    private final LppShadowNodeSimulator shadowSimulator;

    public LppControllerRunner(
            LppTaskContext ctx,
            LeaderElection leaderElection,
            @Nullable @Autowired(required = false) LppShadowNodeSimulator shadowSimulator,
            @Value("${lpp.intervalSeconds:30}") long intervalSeconds) {
        this.ctx = ctx;
        this.leaderElection = leaderElection;
        this.shadowSimulator = shadowSimulator;
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
        leaderElection.shutdown();
        scheduler.shutdown();
    }

    private void runLoop() {
        try {
            if (!leaderElection.isLeader()) {
                log.debug("LPP: not leader, skipping loop");
                return;
            }

            log.info("LPP loop start [leader]");

            String discovery = new LppActualStateDiscoveryTask(ctx).execute();
            log.info("LPP discovery → {}", discovery);

            String allocation = new LppAllocationTask(ctx).execute();
            log.info("LPP allocation → {}", allocation);

            String orchestration = new LppOrchestrationTask(ctx).execute();
            log.info("LPP orchestration → {}", orchestration);

            // Shadow mode: fake ACTIVE actual states so convergence check passes next tick
            if (shadowSimulator != null) {
                log.debug("LPP shadow: simulating node actual-state reporting");
                shadowSimulator.simulate();
            }

            // Routing table: computed AFTER shadow sim so simulated ACTIVE states are visible
            String routing = new LppRoutingTableTask(ctx).execute();
            log.info("LPP routing-table → {}", routing);

            String aggregation = new LppStateAggregationTask(ctx).execute();
            log.info("LPP state-aggregation → {}", aggregation);

            log.info("LPP loop done");
        } catch (Exception e) {
            log.error("LPP loop unexpected error", e);
        }
    }
}
