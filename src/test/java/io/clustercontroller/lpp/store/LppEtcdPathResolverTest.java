package io.clustercontroller.lpp.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LppEtcdPathResolverTest {

    // region=dca, env=staging
    private final LppEtcdPathResolver resolver = new LppEtcdPathResolver("dca", "staging");

    @Test
    void nodeActualStatePath() {
        assertThat(resolver.nodeActualStatePath("node-001"))
                .isEqualTo("/lpp/dca/nodes/node-001/actual-state");
    }

    @Test
    void nodeGoalStatePath() {
        assertThat(resolver.nodeGoalStatePath("node-001"))
                .isEqualTo("/lpp/dca/nodes/node-001/goal-state");
    }

    @Test
    void nodesPrefixEndsWithSlash() {
        assertThat(resolver.nodesPrefix()).isEqualTo("/lpp/dca/nodes/");
    }

    @Test
    void shardPlannedAllocationPath() {
        assertThat(resolver.shardPlannedAllocationPath("grocery.local_index.0"))
                .isEqualTo("/lpp/dca/shards/grocery.local_index.0/planned-allocation");
    }

    @Test
    void shardCapacityPath() {
        assertThat(resolver.shardCapacityPath("grocery.local_index.0"))
                .isEqualTo("/lpp/dca/shards/grocery.local_index.0/capacity");
    }

    @Test
    void groupConfPath() {
        assertThat(resolver.groupConfPath("delivery-grocery-ingester-1"))
                .isEqualTo("/lpp/dca/groups/delivery-grocery-ingester-1/conf");
    }

    @Test
    void indexConfPath() {
        assertThat(resolver.indexConfPath("grocery.local_index"))
                .isEqualTo("/lpp/dca/indices/grocery.local_index/conf");
    }

    @Test
    void taskPath() {
        assertThat(resolver.taskPath("lpp-discovery"))
                .isEqualTo("/lpp/dca/ctl-tasks/lpp-discovery");
    }

    @Test
    void routingTablePath() {
        assertThat(resolver.routingTablePath())
                .isEqualTo("/lpp/search-gateway/staging/dca/routing-table");
    }

    @Test
    void routingTablePathVariesByEnvAndRegion() {
        LppEtcdPathResolver prod = new LppEtcdPathResolver("sjc", "production");
        assertThat(prod.routingTablePath())
                .isEqualTo("/lpp/search-gateway/production/sjc/routing-table");
        assertThat(prod.routingTablePath()).isNotEqualTo(resolver.routingTablePath());
    }

    @Test
    void differentRegionsProduceDifferentPaths() {
        LppEtcdPathResolver sjc = new LppEtcdPathResolver("sjc", "staging");
        assertThat(sjc.nodeActualStatePath("node-001"))
                .isEqualTo("/lpp/sjc/nodes/node-001/actual-state")
                .isNotEqualTo(resolver.nodeActualStatePath("node-001"));
    }
}
