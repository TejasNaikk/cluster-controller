package io.clustercontroller.lpp.orchestration;

import io.clustercontroller.lpp.models.LppNodeActualState;
import io.clustercontroller.lpp.models.LppNodeGoalState;
import io.clustercontroller.lpp.models.LppShardActualState;
import io.clustercontroller.lpp.models.LppShardEntry;
import io.clustercontroller.lpp.store.LppMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Shadow-mode simulator that fakes node actual-state reporting.
 *
 * <p>In a real deployment, LPP nodes write their actual state to etcd after loading shards.
 * In shadow / staging mode (before real LP nodes respond to goal states), this class reads
 * all node goal states and immediately writes back a fake actual state where every shard is
 * in the {@code ACTIVE} state.
 *
 * <p>This enables the full controller loop — discovery → allocation → orchestration →
 * convergence check — to complete end-to-end without waiting for real nodes.
 *
 * <h3>How to enable</h3>
 * <pre>
 * lpp:
 *   shadow:
 *     simulateNodeReporting: true
 *     simulatedActivationDelayMs: 0   # optional: simulate async delay
 * </pre>
 *
 * <h3>Interaction with orchestrator convergence</h3>
 * <p>The orchestrator's convergence check ({@code isActuallyConverged}) reads actual state
 * from etcd. Because the simulator writes ACTIVE actual state, convergence is detected on
 * the next orchestrator tick — just as it would be with real nodes, minus the download time.
 *
 * <p>Run the simulator step <em>after</em> orchestration each tick:
 * <pre>
 *   orchestrator.orchestrate(allocations, groups, region);
 *   shadowSimulator.simulate();   // writes fake ACTIVE actual states
 * </pre>
 */
@Slf4j
public class LppShadowNodeSimulator {

    private final LppMetadataStore metadataStore;
    private final long activationDelayMs;

    /**
     * @param metadataStore     etcd store for reading goal states and writing actual states
     * @param activationDelayMs artificial delay before marking shards ACTIVE (0 = instant)
     */
    public LppShadowNodeSimulator(LppMetadataStore metadataStore, long activationDelayMs) {
        this.metadataStore = metadataStore;
        this.activationDelayMs = activationDelayMs;
        log.info("LPP shadow simulator initialized — all goal states will be reflected as ACTIVE actual states");
    }

    /**
     * For every node that has a goal state written to etcd, write (or update) a fake actual
     * state where all desired shards are in the ACTIVE state.
     *
     * <p>Nodes that already have an actual state with all shards ACTIVE are skipped to avoid
     * unnecessary etcd writes.
     */
    public void simulate() {
        // We need all goal states. The store provides individual gets; iterate via actual-states
        // (which were written by Grail discovery) to know which nodes exist.
        metadataStore.getAllNodeActualStates().forEach((nodeName, existingActual) -> {
            Optional<LppNodeGoalState> goalOpt = metadataStore.getNodeGoalState(nodeName);
            if (goalOpt.isEmpty()) {
                log.debug("LPP shadow: node {} has no goal state yet, skipping", nodeName);
                return;
            }

            LppNodeGoalState goalState = goalOpt.get();
            if (goalState.getShards().isEmpty()) {
                log.debug("LPP shadow: node {} goal state has no shards, skipping", nodeName);
                return;
            }

            // Build fake actual state with all goal-state shards marked ACTIVE
            LppNodeActualState fakeActual = buildFakeActualState(existingActual, goalState);

            // Skip write if nothing changed (all shards already ACTIVE with same shard set)
            if (isAlreadyActive(existingActual, goalState)) {
                log.debug("LPP shadow: node {} already fully ACTIVE, skipping write", nodeName);
                return;
            }

            metadataStore.putNodeActualState(fakeActual);
            log.info("LPP shadow: wrote ACTIVE actual state for node {} ({} shards)",
                    nodeName, fakeActual.getShardStates().size());
        });
    }

    private LppNodeActualState buildFakeActualState(LppNodeActualState base, LppNodeGoalState goal) {
        LppNodeActualState fake = new LppNodeActualState(
                base.getNodeName(),
                base.getOdinInstance(),
                base.getGrailShardId(),
                base.getRole(),
                base.getZone());
        fake.setHost(base.getHost());
        fake.setPort(base.getPort());
        fake.setRegion(base.getRegion());
        fake.setHeartbeatTimestampMs(System.currentTimeMillis());

        List<LppShardActualState> shardStates = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (LppShardEntry entry : goal.getShards()) {
            LppShardActualState shardState = new LppShardActualState();
            shardState.setShardKey(entry.getKey());
            shardState.setCollection(entry.getCollection());
            shardState.setIndexName(entry.getIndexName());
            shardState.setFullIndexName(entry.getFullIndexName());
            shardState.setShardId(entry.getShardId());
            shardState.setState("ACTIVE");
            shardState.setLastStateChangeMs(now - activationDelayMs);
            shardStates.add(shardState);
        }

        fake.setShardStates(shardStates);
        fake.setShardCount(shardStates.size());
        return fake;
    }

    /** True if the node's actual state already has all goal-state shards as ACTIVE. */
    private boolean isAlreadyActive(LppNodeActualState actual, LppNodeGoalState goal) {
        if (actual.getShardStates().isEmpty()) return false;

        var activeKeys = actual.getShardStates().stream()
                .filter(s -> "ACTIVE".equals(s.getState()))
                .map(LppShardActualState::getShardKey)
                .collect(java.util.stream.Collectors.toSet());

        return goal.getShards().stream()
                .map(LppShardEntry::getKey)
                .allMatch(activeKeys::contains);
    }
}
