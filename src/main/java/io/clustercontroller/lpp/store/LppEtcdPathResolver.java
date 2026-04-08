package io.clustercontroller.lpp.store;

import java.nio.file.Paths;

/**
 * Centralized etcd path resolver for all LPP controller keys.
 *
 * <p>Key structure:
 * <pre>
 * /lpp/{region}/
 *   nodes/{nodeName}/actual-state    ← node reports here (or Grail-derived)
 *   nodes/{nodeName}/goal-state      ← controller writes manifest here
 *   shards/{fullIndexName}/{shardId}/planned-allocation  ← controller's shard placement plan
 *   shards/{fullIndexName}/{shardId}/capacity       ← desired capacity for this shard
 *   groups/{groupId}/conf            ← group (odin instance) definition
 *   groups/{groupId}/state           ← group state (capacity, health)
 *   indices/{indexKey}/conf          ← index definition (numShards, numGroups, role)
 *   ctl-tasks/{taskName}             ← task scheduling (same pattern as OS controller)
 *
 * /lpp/search-gateway/{env}/{region}/routing-table
 *   ← routing table consumed by LPP search gateway
 *   env    = deployment environment (staging, prod)
 *   region = geographic region (dca, sjc, …)
 * </pre>
 */
public class LppEtcdPathResolver {

    private static final String SEP = "/";
    private static final String ROOT = "lpp";

    private final String region;
    private final String env;

    public LppEtcdPathResolver(String region, String env) {
        this.region = region;
        this.env = env;
    }

    // -------------------------------------------------------------------------
    // NODE PATHS
    // -------------------------------------------------------------------------

    public String nodesPrefix() {
        return join(ROOT, region, "nodes", "");
    }

    public String nodeActualStatePath(String nodeName) {
        return join(ROOT, region, "nodes", nodeName, "actual-state");
    }

    public String nodeGoalStatePath(String nodeName) {
        return join(ROOT, region, "nodes", nodeName, "goal-state");
    }

    // -------------------------------------------------------------------------
    // SHARD PATHS
    // -------------------------------------------------------------------------

    public String shardsPrefix() {
        return join(ROOT, region, "shards", "");
    }

    public String shardPlannedAllocationPath(String shardKey) {
        return join(ROOT, region, "shards", shardKey, "planned-allocation");
    }

    public String shardCapacityPath(String shardKey) {
        return join(ROOT, region, "shards", shardKey, "capacity");
    }

    // -------------------------------------------------------------------------
    // GROUP PATHS
    // -------------------------------------------------------------------------

    public String groupsPrefix() {
        return join(ROOT, region, "groups", "");
    }

    public String groupConfPath(String groupId) {
        return join(ROOT, region, "groups", groupId, "conf");
    }

    public String groupStatePath(String groupId) {
        return join(ROOT, region, "groups", groupId, "state");
    }

    // -------------------------------------------------------------------------
    // INDEX PATHS
    // -------------------------------------------------------------------------

    public String indicesPrefix() {
        return join(ROOT, region, "indices", "");
    }

    public String indexConfPath(String indexKey) {
        return join(ROOT, region, "indices", indexKey, "conf");
    }

    // -------------------------------------------------------------------------
    // ROUTING TABLE PATH
    // -------------------------------------------------------------------------

    public String routingTablePath() {
        return join(ROOT, "search-gateway", env, region, "routing-table");
    }

    // -------------------------------------------------------------------------
    // TASK PATHS  (same pattern as OS controller, scoped to lpp region)
    // -------------------------------------------------------------------------

    public String tasksPrefix() {
        return join(ROOT, region, "ctl-tasks", "");
    }

    public String taskPath(String taskName) {
        return join(ROOT, region, "ctl-tasks", taskName);
    }

    // -------------------------------------------------------------------------

    private String join(String... parts) {
        return SEP + String.join(SEP, parts);
    }

    public String getEnv() {
        return env;
    }

    public String getRegion() {
        return region;
    }
}
