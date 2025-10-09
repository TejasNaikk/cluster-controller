package io.clustercontroller;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import io.clustercontroller.tasks.TaskFactory;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Generic task manager for scheduling and executing tasks.
 * Agnostic to specific task types - delegates execution to Task implementations.
 */
@Slf4j
public class TaskManager {
    
    private final MetadataStore metadataStore;
    private final TaskContext taskContext;
    private final String clusterName;
    
    private final ScheduledExecutorService scheduler;
    private final long intervalSeconds;
    private boolean isRunning = false;
    
    public TaskManager(MetadataStore metadataStore, TaskContext taskContext, String clusterName, long intervalSeconds) {
        this.metadataStore = metadataStore;
        this.taskContext = taskContext;
        this.clusterName = clusterName;
        this.intervalSeconds = intervalSeconds;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    public TaskMetadata createTask(String taskName, String input, int priority) {
        log.info("Creating task: name={}, priority={}", taskName, priority);
        TaskMetadata taskMetadata = new TaskMetadata(taskName, priority);
        taskMetadata.setInput(input);
        try {
            metadataStore.createTask(clusterName, taskMetadata);
        } catch (Exception e) {
            log.error("Failed to create task: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create task", e);
        }
        return taskMetadata;
    }
    
    public List<TaskMetadata> getAllTasks() {
        log.debug("Getting all tasks");
        try {
            return metadataStore.getAllTasks(clusterName);
        } catch (Exception e) {
            log.error("Failed to get all tasks: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to get tasks", e);
        }
    }
    
    public Optional<TaskMetadata> getTask(String taskName) {
        log.debug("Getting task: {}", taskName);
        try {
            return metadataStore.getTask(clusterName, taskName);
        } catch (Exception e) {
            log.error("Failed to get task {}: {}", taskName, e.getMessage(), e);
            throw new RuntimeException("Failed to get task", e);
        }
    }
    
    public void updateTask(TaskMetadata taskMetadata) {
        log.debug("Updating task: {}", taskMetadata.getName());
        try {
            metadataStore.updateTask(clusterName, taskMetadata);
        } catch (Exception e) {
            log.error("Failed to update task {}: {}", taskMetadata.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to update task", e);
        }
    }
    
    public void deleteTask(String taskName) {
        log.info("Deleting task: {}", taskName);
        try {
            metadataStore.deleteTask(clusterName, taskName);
        } catch (Exception e) {
            log.error("Failed to delete task {}: {}", taskName, e.getMessage(), e);
            throw new RuntimeException("Failed to delete task", e);
        }
    }
    
    public void start() {
        log.info("Starting task manager");
        isRunning = true;
        
        // Bootstrap recurring system tasks
        bootstrapRecurringTasks();
        
        scheduler.scheduleWithFixedDelay(
                this::processTaskLoop,
                0,
                intervalSeconds,
                TimeUnit.SECONDS
        );
    }
    
    public void stop() {
        log.info("Stopping task manager");
        isRunning = false;
        scheduler.shutdown();
        try {
            metadataStore.close();
        } catch (Exception e) {
            log.error("Error closing metadata store: {}", e.getMessage());
        }
    }
    
    private void processTaskLoop() {
        try {
            // Only process tasks if this node is the leader
            if (!metadataStore.isLeader()) {
                log.debug("Skipping task processing - not the leader");
                return;
            }
            
            log.info("Running task processing loop - checking for tasks");
            
            List<TaskMetadata> taskMetadataList = getAllTasks();
            log.info("Found {} tasks in etcd", taskMetadataList.size());
            for (TaskMetadata task : taskMetadataList) {
                log.info("Task: {} status: {} priority: {}", task.getName(), task.getStatus(), task.getPriority());
            }
            
            cleanupOldTasks(taskMetadataList);
            
            TaskMetadata taskMetadataToProcess = selectNextTask(taskMetadataList);
            if (taskMetadataToProcess != null) {
                log.info("Processing task: {}", taskMetadataToProcess.getName());
                String result = executeTask(taskMetadataToProcess);
                log.info("Task {} completed with result: {}", taskMetadataToProcess.getName(), result);
            } else {
                log.info("No pending tasks to process");
            }
        } catch (Exception e) {
            log.error("Error in task processing loop: {}", e.getMessage(), e);
        }
    }
    
    private String executeTask(TaskMetadata taskMetadata) {
        try {
            taskMetadata.setStatus(TASK_STATUS_RUNNING);
            updateTask(taskMetadata);
            
            log.info("Executing task: {}", taskMetadata.getName());
            
            // Create Task implementation from metadata and execute
            Task task = TaskFactory.createTask(taskMetadata);
            String result = task.execute(taskContext);
            
            taskMetadata.setStatus(result);
            updateTask(taskMetadata);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to execute task {}: {}", taskMetadata.getName(), e.getMessage(), e);
            taskMetadata.setStatus(TASK_STATUS_FAILED);
            try {
                updateTask(taskMetadata);
            } catch (Exception updateException) {
                log.error("Failed to update task status to failed: {}", updateException.getMessage());
            }
            return TASK_STATUS_FAILED;
        }
    }
    
    private TaskMetadata selectNextTask(List<TaskMetadata> tasks) {
        // Simple priority-based selection - execute all repeat tasks regardless of status
        // This prevents priority inversion and task starvation
        return tasks.stream()
                .filter(task -> TASK_SCHEDULE_REPEAT.equals(task.getSchedule()) || TASK_STATUS_PENDING.equals(task.getStatus()))
                .min((t1, t2) -> {
                    // Compare by priority (lower number = higher priority)
                    int priorityCompare = Integer.compare(t1.getPriority(), t2.getPriority());
                    if (priorityCompare != 0) return priorityCompare;
                    return t1.getLastUpdated().compareTo(t2.getLastUpdated());
                })
                .orElse(null);
    }
    
    private void cleanupOldTasks(List<TaskMetadata> tasks) {
        // TODO: Implement task cleanup logic
        log.debug("Cleaning up old tasks");
    }
    
    /**
     * Bootstrap recurring system tasks at startup.
     * Creates the core allocation and orchestration tasks for ALL clusters in etcd.
     */
    private void bootstrapRecurringTasks() {
        log.info("Bootstrapping recurring system tasks for all clusters");
        
        try {
            // Discover all clusters in etcd
            List<String> clusters = metadataStore.getAllClusters();
            
            if (clusters.isEmpty()) {
                log.warn("No clusters found in etcd, creating tasks for default cluster: {}", clusterName);
                clusters = List.of(clusterName);
            }
            
            log.info("Found {} clusters, bootstrapping tasks for each", clusters.size());
            
            // Create tasks for each cluster
            for (String cluster : clusters) {
                log.info("Bootstrapping tasks for cluster: {}", cluster);
                createRecurringTaskIfNotExists(cluster, TASK_ACTION_SHARD_ALLOCATOR, 0);
                createRecurringTaskIfNotExists(cluster, TASK_ACTION_ACTUAL_ALLOCATION_UPDATER, 1);
                createRecurringTaskIfNotExists(cluster, TASK_ACTION_GOAL_STATE_ORCHESTRATOR, 2);
            }
            
            log.info("Successfully bootstrapped recurring system tasks for {} cluster(s)", clusters.size());
        } catch (Exception e) {
            log.error("Error bootstrapping recurring tasks: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Create a recurring task for a specific cluster if it doesn't already exist.
     */
    private void createRecurringTaskIfNotExists(String clusterId, String taskName, int priority) {
        try {
            // Check if task already exists for this cluster
            Optional<TaskMetadata> existingTask = metadataStore.getTask(clusterId, taskName);
            if (existingTask.isPresent()) {
                log.debug("Task '{}' already exists for cluster '{}', skipping creation", taskName, clusterId);
                return;
            }
            
            // Create new task for this cluster
            TaskMetadata task = new TaskMetadata(taskName, priority);
            task.setInput("");
            task.setSchedule(TASK_SCHEDULE_REPEAT);
            metadataStore.createTask(clusterId, task);
            
            log.info("Created recurring task '{}' for cluster '{}' (priority: {})", taskName, clusterId, priority);
        } catch (Exception e) {
            log.warn("Failed to create recurring task '{}' for cluster '{}': {}", taskName, clusterId, e.getMessage());
        }
    }
}
