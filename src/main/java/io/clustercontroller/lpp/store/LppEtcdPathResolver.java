package io.clustercontroller.lpp.store;

import java.nio.file.Paths;

/**
 * Centralized etcd path resolver for all LPP controller keys.
 *
 * <p>Key structure:
 * <pre>
 * /lpp/{env}/
 *   nodes/{nodeName}/actual-state    ← node reports here (or Grail-derived)
 *   nodes/{nodeName}/goal-state      ← controller writes manifest here
 *   shards/{shardKey}/planned-allocation  ← controller's shard placement plan
 *   shards/{shardKey}/capacity       ← desired capacity for this shard
 *   groups/{groupId}/conf            ← group (odin instance) definition
 *   groups/{groupId}/state           ← group state (capacity, health)
 *   indices/{indexKey}/conf          ← index definition (numShards, numGroups, role)
 *   ctl-tasks/{taskName}             ← task scheduling (same pattern as OS controller)
 * </pre>
 */
public class LppEtcdPathResolver {

    private static final String SEP = "/";
    private static final String ROOT = "lpp";

    private final String env;

    public LppEtcdPathResolver(String env) {
        this.env = env;
    }

    // -------------------------------------------------------------------------
    // NODE PATHS
    // -------------------------------------------------------------------------

    public String nodesPrefix() {
        return join(ROOT, env, "nodes", "");
    }

    public String nodeActualStatePath(String nodeName) {
        return join(ROOT, env, "nodes", nodeName, "actual-state");
    }

    public String nodeGoalStatePath(String nodeName) {
        return join(ROOT, env, "nodes", nodeName, "goal-state");
    }

    // -------------------------------------------------------------------------
    // SHARD PATHS
    // -------------------------------------------------------------------------

    public String shardsPrefix() {
        return join(ROOT, env, "shards", "");
    }

    public String shardPlannedAllocationPath(String shardKey) {
        return join(ROOT, env, "shards", shardKey, "planned-allocation");
    }

    public String shardCapacityPath(String shardKey) {
        return join(ROOT, env, "shards", shardKey, "capacity");
    }

    // -------------------------------------------------------------------------
    // GROUP PATHS
    // -------------------------------------------------------------------------

    public String groupsPrefix() {
        return join(ROOT, env, "groups", "");
    }

    public String groupConfPath(String groupId) {
        return join(ROOT, env, "groups", groupId, "conf");
    }

    public String groupStatePath(String groupId) {
        return join(ROOT, env, "groups", groupId, "state");
    }

    // -------------------------------------------------------------------------
    // INDEX PATHS
    // -------------------------------------------------------------------------

    public String indicesPrefix() {
        return join(ROOT, env, "indices", "");
    }

    public String indexConfPath(String indexKey) {
        return join(ROOT, env, "indices", indexKey, "conf");
    }

    // -------------------------------------------------------------------------
    // ROUTING TABLE PATH
    // -------------------------------------------------------------------------

    public String routingTablePath() {
        return join(ROOT, env, "routing", "goal-state");
    }

    // -------------------------------------------------------------------------
    // TASK PATHS  (same pattern as OS controller, scoped to lpp env)
    // -------------------------------------------------------------------------

    public String tasksPrefix() {
        return join(ROOT, env, "ctl-tasks", "");
    }

    public String taskPath(String taskName) {
        return join(ROOT, env, "ctl-tasks", taskName);
    }

    // -------------------------------------------------------------------------

    private String join(String... parts) {
        return SEP + String.join(SEP, parts);
    }

    public String getEnv() {
        return env;
    }
}
