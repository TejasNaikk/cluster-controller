package io.clustercontroller.lpp.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LppEtcdPathResolverTest {

    private final LppEtcdPathResolver resolver = new LppEtcdPathResolver("staging");

    @Test
    void nodeActualStatePath() {
        assertThat(resolver.nodeActualStatePath("node-001"))
                .isEqualTo("/lpp/staging/nodes/node-001/actual-state");
    }

    @Test
    void nodeGoalStatePath() {
        assertThat(resolver.nodeGoalStatePath("node-001"))
                .isEqualTo("/lpp/staging/nodes/node-001/goal-state");
    }

    @Test
    void nodesPrefixEndsWithSlash() {
        assertThat(resolver.nodesPrefix()).isEqualTo("/lpp/staging/nodes/");
    }

    @Test
    void shardPlannedAllocationPath() {
        assertThat(resolver.shardPlannedAllocationPath("grocery.local_index.0"))
                .isEqualTo("/lpp/staging/shards/grocery.local_index.0/planned-allocation");
    }

    @Test
    void shardCapacityPath() {
        assertThat(resolver.shardCapacityPath("grocery.local_index.0"))
                .isEqualTo("/lpp/staging/shards/grocery.local_index.0/capacity");
    }

    @Test
    void groupConfPath() {
        assertThat(resolver.groupConfPath("delivery-grocery-ingester-1"))
                .isEqualTo("/lpp/staging/groups/delivery-grocery-ingester-1/conf");
    }

    @Test
    void indexConfPath() {
        assertThat(resolver.indexConfPath("grocery.local_index"))
                .isEqualTo("/lpp/staging/indices/grocery.local_index/conf");
    }

    @Test
    void taskPath() {
        assertThat(resolver.taskPath("lpp-discovery"))
                .isEqualTo("/lpp/staging/ctl-tasks/lpp-discovery");
    }

    @Test
    void differentEnvsProduceDifferentPaths() {
        LppEtcdPathResolver prod = new LppEtcdPathResolver("production");
        assertThat(prod.nodeActualStatePath("node-001"))
                .isEqualTo("/lpp/production/nodes/node-001/actual-state")
                .isNotEqualTo(resolver.nodeActualStatePath("node-001"));
    }
}
