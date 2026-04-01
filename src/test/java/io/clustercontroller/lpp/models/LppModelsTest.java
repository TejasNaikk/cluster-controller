package io.clustercontroller.lpp.models;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LppShardEntryTest {

    @Test
    void keyUsesFullIndexNameNotIndexName() {
        // Key must discriminate between versions: local_index.1 shard 0 ≠ local_index.2 shard 0
        LppShardEntry entry = new LppShardEntry("grocery", "local_index", "local_index.1", 0);
        assertThat(entry.getKey()).isEqualTo("grocery.local_index.1.0");
    }

    @Test
    void keyForDifferentVersionIsDistinct() {
        LppShardEntry v1 = new LppShardEntry("grocery", "local_index", "local_index.1", 3);
        LppShardEntry v2 = new LppShardEntry("grocery", "local_index", "local_index.2", 3);
        assertThat(v1.getKey()).isEqualTo("grocery.local_index.1.3");
        assertThat(v2.getKey()).isEqualTo("grocery.local_index.2.3");
        assertThat(v1.getKey()).isNotEqualTo(v2.getKey());
    }

    @Test
    void defaultRemotePathIsEmpty() {
        LppShardEntry entry = new LppShardEntry("grocery", "local_index", "local_index.1", 0);
        assertThat(entry.getRemotePath()).isEmpty();
    }
}

class LppGroupTest {

    @Test
    void isHealthyReturnsFalseWhenEmpty() {
        LppGroup group = new LppGroup("g1", "zone-a", "INGEST");
        assertThat(group.isHealthy()).isFalse();
    }

    @Test
    void isHealthyReturnsTrueWhenAllNodesHealthy() {
        LppGroup group = new LppGroup("g1", "zone-a", "INGEST");
        LppNode node = new LppNode("n1", "g1", "shard-0", "INGEST", "zone-a");
        node.setHealthState("GREEN");
        group.addNode(node);
        assertThat(group.isHealthy()).isTrue();
    }

    @Test
    void isHealthyReturnsFalseWhenNodeDrained() {
        LppGroup group = new LppGroup("g1", "zone-a", "INGEST");
        LppNode node = new LppNode("n1", "g1", "shard-0", "INGEST", "zone-a");
        node.setHealthState("GREEN");
        node.setAdminState("DRAIN");
        group.addNode(node);
        assertThat(group.isHealthy()).isFalse();
    }

    @Test
    void isHealthyReturnsFalseWhenOneNodeDrained() {
        // isHealthy = strict: ALL nodes must be healthy + non-drained
        LppGroup group = new LppGroup("g1", "zone-a", "INGEST");
        LppNode healthy = new LppNode("n1", "g1", "shard-0", "INGEST", "zone-a");
        healthy.setHealthState("GREEN");
        LppNode drained = new LppNode("n2", "g1", "shard-0", "INGEST", "zone-a");
        drained.setHealthState("GREEN");
        drained.setAdminState("DRAIN");
        group.addNode(healthy);
        group.addNode(drained);
        assertThat(group.isHealthy()).isFalse();
    }

    @Test
    void hasEligibleNodesTrueWhenAtLeastOneNonDrained() {
        // hasEligibleNodes = softer: at least 1 non-drained node
        LppGroup group = new LppGroup("g1", "zone-a", "INGEST");
        LppNode healthy = new LppNode("n1", "g1", "shard-0", "INGEST", "zone-a");
        healthy.setHealthState("GREEN");
        LppNode drained = new LppNode("n2", "g1", "shard-0", "INGEST", "zone-a");
        drained.setHealthState("GREEN");
        drained.setAdminState("DRAIN");
        group.addNode(healthy);
        group.addNode(drained);
        // isHealthy=false (strict), hasEligibleNodes=true (soft — 1 node is not DRAIN)
        assertThat(group.isHealthy()).isFalse();
        assertThat(group.hasEligibleNodes()).isTrue();
    }

    @Test
    void hasEligibleNodesFalseWhenAllDrained() {
        LppGroup group = new LppGroup("g1", "zone-a", "INGEST");
        for (int i = 0; i < 3; i++) {
            LppNode n = new LppNode("n" + i, "g1", "shard-0", "INGEST", "zone-a");
            n.setHealthState("GREEN");
            n.setAdminState("DRAIN");
            group.addNode(n);
        }
        assertThat(group.hasEligibleNodes()).isFalse();
    }


    @Test
    void isFullAtDefaultReplicaCount() {
        LppGroup group = new LppGroup("g1", "zone-a", "INGEST");
        for (int i = 0; i < 3; i++) {
            LppNode node = new LppNode("n" + i, "g1", "shard-0", "INGEST", "zone-a");
            node.setHealthState("GREEN");
            group.addNode(node);
        }
        assertThat(group.isFull()).isTrue();
        assertThat(group.size()).isEqualTo(3);
    }
}

class LppNodeGoalStateTest {

    @Test
    void bumpVersionIncrementsVersion() {
        LppNodeGoalState gs = new LppNodeGoalState("node-1", "INGEST", "local");
        assertThat(gs.getVersion()).isEqualTo(0);
        gs.bumpVersion();
        assertThat(gs.getVersion()).isEqualTo(1);
        gs.bumpVersion();
        assertThat(gs.getVersion()).isEqualTo(2);
    }

    @Test
    void addShardGrowsList() {
        LppNodeGoalState gs = new LppNodeGoalState("node-1", "INGEST", "local");
        gs.addShard(new LppShardEntry("grocery", "local_index", "local_index.1", 0));
        gs.addShard(new LppShardEntry("grocery", "local_index", "local_index.1", 1));
        assertThat(gs.getShards()).hasSize(2);
    }
}

class LppShardActualStateTest {

    @Test
    void isStuckReturnsFalseForActiveState() {
        LppShardActualState s = new LppShardActualState();
        s.setState("ACTIVE");
        s.setLastStateChangeMs(System.currentTimeMillis() - 999_999);
        assertThat(s.isStuck(1000)).isFalse();
    }

    @Test
    void isStuckReturnsTrueWhenDownloadingTooLong() {
        LppShardActualState s = new LppShardActualState();
        s.setState("DOWNLOADING");
        s.setLastStateChangeMs(System.currentTimeMillis() - 20_000);
        assertThat(s.isStuck(10_000)).isTrue();
    }

    @Test
    void isStuckReturnsFalseWhenDownloadingRecently() {
        LppShardActualState s = new LppShardActualState();
        s.setState("DOWNLOADING");
        s.setLastStateChangeMs(System.currentTimeMillis() - 1_000);
        assertThat(s.isStuck(10_000)).isFalse();
    }
}
