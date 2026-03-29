package io.clustercontroller.lpp.models;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LppShardEntryTest {

    @Test
    void keyFormat() {
        LppShardEntry entry = new LppShardEntry("grocery", "local_index", "local_index.1", 0);
        assertThat(entry.getKey()).isEqualTo("grocery.local_index.0");
    }

    @Test
    void keyIncludesShardId() {
        LppShardEntry shard2 = new LppShardEntry("grocery", "local_index", "local_index.1", 2);
        assertThat(shard2.getKey()).isEqualTo("grocery.local_index.2");
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
