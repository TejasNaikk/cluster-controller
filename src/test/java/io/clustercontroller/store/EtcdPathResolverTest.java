package io.clustercontroller.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EtcdPathResolverTest {

    private EtcdPathResolver pathResolver;
    private final String testClusterName = "test-cluster";

    @BeforeEach
    void setUp() {
        pathResolver = EtcdPathResolver.getInstance();
    }

    @Test
    void testSingletonPattern() {
        EtcdPathResolver instance1 = EtcdPathResolver.getInstance();
        EtcdPathResolver instance2 = EtcdPathResolver.getInstance();
        assertThat(instance1).isSameAs(instance2);
    }

    @Test
    void testGetClusterRoot() {
        String path = pathResolver.getClusterRoot(testClusterName);
        assertThat(path).isEqualTo("/test-cluster");
    }

    @Test
    void testGetControllerTasksPrefix() {
        String path = pathResolver.getControllerTasksPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/ctl-tasks");
    }

    @Test
    void testGetControllerTaskPath() {
        String path = pathResolver.getControllerTaskPath(testClusterName, "task1");
        assertThat(path).isEqualTo("/test-cluster/ctl-tasks/task1");
    }

    @Test
    void testGetSearchUnitsPrefix() {
        String path = pathResolver.getSearchUnitsPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/search-unit");
    }

    @Test
    void testGetSearchUnitConfPath() {
        String path = pathResolver.getSearchUnitConfPath(testClusterName, "unit1");
        assertThat(path).isEqualTo("/test-cluster/search-unit/unit1/conf");
    }

    @Test
    void testGetSearchUnitGoalStatePath() {
        String path = pathResolver.getSearchUnitGoalStatePath(testClusterName, "unit1");
        assertThat(path).isEqualTo("/test-cluster/search-unit/unit1/goal-state");
    }

    @Test
    void testGetSearchUnitActualStatePath() {
        String path = pathResolver.getSearchUnitActualStatePath(testClusterName, "unit1");
        assertThat(path).isEqualTo("/test-cluster/search-unit/unit1/actual-state");
    }

    @Test
    void testGetIndicesPrefix() {
        String path = pathResolver.getIndicesPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/indices");
    }

    @Test
    void testGetIndexConfPath() {
        String path = pathResolver.getIndexConfPath(testClusterName, "index1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/conf");
    }

    @Test
    void testGetIndexMappingsPath() {
        String path = pathResolver.getIndexMappingsPath(testClusterName, "index1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/mappings");
    }

    @Test
    void testGetIndexSettingsPath() {
        String path = pathResolver.getIndexSettingsPath(testClusterName, "index1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/settings");
    }

    @Test
    void testGetShardPlannedAllocationPath() {
        String path = pathResolver.getShardPlannedAllocationPath(testClusterName, "index1", "shard1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/shard1/planned-allocation");
    }

    @Test
    void testGetShardActualAllocationPath() {
        String path = pathResolver.getShardActualAllocationPath(testClusterName, "index1", "shard1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/shard1/actual-allocation");
    }

    @Test
    void testGetCoordinatorsPrefix() {
        String path = pathResolver.getCoordinatorsPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/coordinators");
    }

    @Test
    void testGetCoordinatorGoalStatePath() {
        String path = pathResolver.getCoordinatorGoalStatePath(testClusterName, "coordinators", "default-coordinator");
        assertThat(path).isEqualTo("/test-cluster/coordinators/default-coordinator/goal-state");
    }

    @Test
    void testGetCoordinatorActualStatePath() {
        String path = pathResolver.getCoordinatorActualStatePath(testClusterName, "coord1");
        assertThat(path).isEqualTo("/test-cluster/coordinators/coord1/actual-state");
    }

    @Test
    void testGetLeaderElectionPath() {
        String path = pathResolver.getLeaderElectionPath(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/leader-election");
    }

    @Test
    void testMultipleClusterNames() {
        String cluster1Path = pathResolver.getIndexConfPath("cluster1", "index1");
        String cluster2Path = pathResolver.getIndexConfPath("cluster2", "index1");
        
        assertThat(cluster1Path).isEqualTo("/cluster1/indices/index1/conf");
        assertThat(cluster2Path).isEqualTo("/cluster2/indices/index1/conf");
        assertThat(cluster1Path).isNotEqualTo(cluster2Path);
    }

    // =================================================================
    // RUNTIME ENVIRONMENT TESTS
    // =================================================================

    @Test
    void testDefaultRuntimeEnv() {
        // Default should be "staging"
        assertThat(pathResolver.getRuntimeEnv()).isNotNull();
    }

    @Test
    void testSetRuntimeEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("production");
            assertThat(pathResolver.getRuntimeEnv()).isEqualTo("production");
        } finally {
            // Restore original to not affect other tests
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testSetRuntimeEnvIgnoresNull() {
        String originalEnv = pathResolver.getRuntimeEnv();
        pathResolver.setRuntimeEnv(null);
        assertThat(pathResolver.getRuntimeEnv()).isEqualTo(originalEnv);
    }

    @Test
    void testSetRuntimeEnvIgnoresBlank() {
        String originalEnv = pathResolver.getRuntimeEnv();
        pathResolver.setRuntimeEnv("   ");
        assertThat(pathResolver.getRuntimeEnv()).isEqualTo(originalEnv);
    }

    // =================================================================
    // MULTI-CLUSTER PATHS WITH RUNTIME ENVIRONMENT
    // =================================================================

    @Test
    void testGetMultiClusterRootIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("staging");
            String path = pathResolver.getMultiClusterRoot();
            assertThat(path).isEqualTo("/multi-cluster/staging");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testGetControllerHeartbeatPathIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("production");
            String path = pathResolver.getControllerHeartbeatPath("controller-1");
            assertThat(path).isEqualTo("/multi-cluster/production/controllers/controller-1/heartbeat");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testGetControllerAssignmentPathIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("staging");
            String path = pathResolver.getControllerAssignmentPath("controller-1", "cluster-a");
            assertThat(path).isEqualTo("/multi-cluster/staging/controllers/controller-1/assigned/cluster-a");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testGetClusterLockPathIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("production");
            String path = pathResolver.getClusterLockPath("cluster-a");
            assertThat(path).isEqualTo("/multi-cluster/production/locks/clusters/cluster-a");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testGetClusterRegistryPathIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("staging");
            String path = pathResolver.getClusterRegistryPath("cluster-a");
            assertThat(path).isEqualTo("/multi-cluster/staging/clusters/cluster-a/metadata");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testGetClusterAssignedControllerPathIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("production");
            String path = pathResolver.getClusterAssignedControllerPath("cluster-a");
            assertThat(path).isEqualTo("/multi-cluster/production/clusters/cluster-a/assigned-to");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testGetControllersPrefixIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("staging");
            String path = pathResolver.getControllersPrefix();
            assertThat(path).isEqualTo("/multi-cluster/staging/controllers");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testGetClustersPrefixIncludesEnv() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("production");
            String path = pathResolver.getClustersPrefix();
            assertThat(path).isEqualTo("/multi-cluster/production/clusters");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }

    @Test
    void testDifferentEnvsProduceDifferentPaths() {
        String originalEnv = pathResolver.getRuntimeEnv();
        try {
            pathResolver.setRuntimeEnv("staging");
            String stagingPath = pathResolver.getClusterLockPath("cluster-a");
            
            pathResolver.setRuntimeEnv("production");
            String productionPath = pathResolver.getClusterLockPath("cluster-a");
            
            assertThat(stagingPath).isNotEqualTo(productionPath);
            assertThat(stagingPath).contains("/staging/");
            assertThat(productionPath).contains("/production/");
        } finally {
            pathResolver.setRuntimeEnv(originalEnv);
        }
    }
}