package io.clustercontroller.lpp.config;

/**
 * Constants for LPP controller.
 */
public final class LppConstants {

    private LppConstants() {}

    // etcd path segments
    public static final String PATH_LPP = "lpp";
    public static final String PATH_NODES = "nodes";
    public static final String PATH_SHARDS = "shards";
    public static final String PATH_GROUPS = "groups";
    public static final String PATH_INDICES = "indices";
    public static final String PATH_CTL_TASKS = "ctl-tasks";

    public static final String SUFFIX_ACTUAL_STATE = "actual-state";
    public static final String SUFFIX_GOAL_STATE = "goal-state";
    public static final String SUFFIX_PLANNED_ALLOCATION = "planned-allocation";
    public static final String SUFFIX_CAPACITY = "capacity";
    public static final String SUFFIX_CONF = "conf";
    public static final String SUFFIX_STATE = "state";

    // Defaults
    public static final int DEFAULT_REPLICA_COUNT = 3;
    // Configurable via -Dlpp.staleNodeTimeoutMs (default 10 min)
    public static final long STALE_NODE_TIMEOUT_MS = Long.getLong("lpp.staleNodeTimeoutMs", 10 * 60 * 1000L);

    // Roles (mirrors LucenePlusRole in the data plane)
    public static final String ROLE_INGEST = "INGEST";
    public static final String ROLE_SEARCH = "SEARCH";

    // Node admin states
    public static final String ADMIN_STATE_NORMAL = "NORMAL";
    public static final String ADMIN_STATE_DRAIN = "DRAIN";

    // Shard slot states (mirrors LppShardSlotState in data plane)
    public static final String SHARD_STATE_PENDING = "PENDING";
    public static final String SHARD_STATE_DOWNLOADING = "DOWNLOADING";
    public static final String SHARD_STATE_ACTIVE = "ACTIVE";
    public static final String SHARD_STATE_REMOVING = "REMOVING";
    public static final String SHARD_STATE_REMOVED = "REMOVED";
    public static final String SHARD_STATE_FAILED = "FAILED";

    // Task names
    public static final String TASK_DISCOVERY = "lpp-discovery";
    public static final String TASK_ALLOCATION = "lpp-allocation";
    public static final String TASK_ORCHESTRATION = "lpp-orchestration";
    public static final String TASK_ROUTING_TABLE = "lpp-routing-table";
    public static final String TASK_STATE_AGGREGATION = "lpp-state-aggregation";

    // Task priorities (lower number = higher priority)
    public static final int PRIORITY_DISCOVERY = 1;
    public static final int PRIORITY_ALLOCATION = 2;
    public static final int PRIORITY_ORCHESTRATION = 3;
    public static final int PRIORITY_STATE_AGGREGATION = 4;
}
