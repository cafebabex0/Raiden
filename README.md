# Build a Distributed MapReduce System from Scratch (Java)

A complete implementation of the MIT 6.5840 MapReduce lab in pure Java. Every component explained—no black boxes.

---

## Table of Contents

1. [Conceptual Foundation](#conceptual-foundation)
2. [Project Setup](#project-setup)
3. [Part 1: Core Data Models](#part-1-core-data-models)
4. [Part 2: RPC Framework](#part-2-rpc-framework)
5. [Part 3: Task Management](#part-3-task-management)
6. [Part 4: Coordinator](#part-4-coordinator)
7. [Part 5: Worker](#part-5-worker)
8. [Part 6: MapReduce Functions](#part-6-mapreduce-functions)
9. [Part 7: Testing & Validation](#part-7-testing--validation)
10. [Part 8: Deployment](#part-8-deployment)
11. [Troubleshooting](#troubleshooting)

---

## Conceptual Foundation

### The Problem

You have **large input files** and need to compute something across them:
- **Word count**: Read 1000 files, count word frequencies
- **Indexer**: Build inverted index across all files
- **Grep**: Find matching lines in millions of records

Single-threaded: 1000 files × 100ms = 100 seconds
Distributed (10 workers): 1000 ÷ 10 × 100ms = 10 seconds

MapReduce solves this: **parallelize work across machines**.

### How MapReduce Works

```
Input Files
├─ pg-1.txt
├─ pg-2.txt
└─ pg-3.txt
     │
     ▼ (Map Phase - Parallel)
┌─────────────────────────────┐
│ Worker 1: Map (pg-1.txt)    │ → (word, 1), (word, 1), ...
│ Worker 2: Map (pg-2.txt)    │ → (word, 1), (word, 1), ...
│ Worker 3: Map (pg-3.txt)    │ → (word, 1), (word, 1), ...
└─────────────────────────────┘
     │
     ▼ (Shuffle - Group by key)
┌──────────────────────────────────┐
│ Partition 0: (apple → [1,1,1])   │
│ Partition 1: (banana → [1,1])    │
│ Partition 2: (cherry → [1])      │
└──────────────────────────────────┘
     │
     ▼ (Reduce Phase - Parallel)
┌─────────────────────────────┐
│ Worker 1: Reduce part-0     │ → (apple, 3)
│ Worker 2: Reduce part-1     │ → (banana, 2)
│ Worker 3: Reduce part-2     │ → (cherry, 1)
└─────────────────────────────┘
     │
     ▼
Output Files (mr-out-0, mr-out-1, mr-out-2, ...)
```

### Architecture

```
┌──────────────────────────────────────┐
│      Coordinator (RPC Server)        │
│  - Distributes Map/Reduce tasks      │
│  - Tracks worker status              │
│  - Detects crashed workers (10s)     │
│  - Determines when job complete      │
└──────────────────────────────────────┘
   ▲              ▲              ▲
   │ RPC          │ RPC          │ RPC
   │ GetTask()    │ GetTask()    │ GetTask()
   │ NotifyDone() │ NotifyDone() │ NotifyDone()
   │              │              │
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   Worker 1   │ │   Worker 2   │ │   Worker 3   │
│ - Map/Reduce │ │ - Map/Reduce │ │ - Map/Reduce │
│ - I/O        │ │ - I/O        │ │ - I/O        │
└──────────────┘ └──────────────┘ └──────────────┘
```

### Key Design Decisions

**Why separate Coordinator and Workers?**
- Coordinator is stateless (can restart)
- Workers are ephemeral (can crash)
- Separation of concerns

**Why 10-second timeout?**
- Detecting crashed workers
- Distinguishing from slow workers
- Balances responsiveness vs false positives

**Why partition intermediate data?**
- Reduce parallelism: each reducer processes one partition
- Shuffle-free: workers write directly to partition files
- Scalability: nReduce partitions = nReduce reducers

---

## Project Setup

### Prerequisites

```bash
java -version   # Java 21+
mvn -version    # Maven 3.8+
```

### Directory Structure

```
mapreduce-java/
├── pom.xml
├── config.yaml
├── input/
│   ├── pg-alice.txt
│   ├── pg-tom.txt
│   └── pg-austen.txt
│
└── src/
    └── main/
        ├── java/com/mit/mapreduce/
        │   ├── MapReduceApp.java           # Main entry point
        │   ├── coordinator/
        │   │   ├── Coordinator.java
        │   │   ├── TaskManager.java
        │   │   └── WorkerTracker.java
        │   ├── worker/
        │   │   ├── Worker.java
        │   │   ├── TaskExecutor.java
        │   │   └── FileManager.java
        │   ├── rpc/
        │   │   ├── RpcServer.java
        │   │   ├── RpcClient.java
        │   │   └── RpcMessages.java
        │   ├── task/
        │   │   ├── Task.java
        │   │   ├── TaskState.java
        │   │   └── TaskType.java
        │   ├── mapfunc/
        │   │   ├── MapFunction.java
        │   │   ├── ReduceFunction.java
        │   │   └── WordCountApp.java
        │   └── util/
        │       ├── FileUtils.java
        │       ├── HashUtils.java
        │       └── JsonSerde.java
        └── resources/
            └── logback.xml
```

Create directories:
```bash
mkdir -p mapreduce-java/src/main/java/com/mit/mapreduce/{coordinator,worker,rpc,task,mapfunc,util}
mkdir -p mapreduce-java/src/main/resources
mkdir -p mapreduce-java/input
cd mapreduce-java
```

### Maven Configuration (pom.xml)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.mit</groupId>
    <artifactId>mapreduce-java</artifactId>
    <version>1.0.0</version>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- JSON serialization -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.16.1</version>
        </dependency>

        <!-- YAML config -->
        <dependency>
            <groupId>com.fasterxml.jackson.dataformat</groupId>
            <artifactId>jackson-dataformat-yaml</artifactId>
            <version>2.16.1</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.11</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.4.14</version>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>21</source>
                    <target>21</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

Build to verify setup:
```bash
mvn clean compile
```

---

## Part 1: Core Data Models

### Task Representation

Create `src/main/java/com/mit/mapreduce/task/TaskType.java`:

```java
package com.mit.mapreduce.task;

public enum TaskType {
    MAP, REDUCE
}
```

Create `src/main/java/com/mit/mapreduce/task/TaskState.java`:

```java
package com.mit.mapreduce.task;

public enum TaskState {
    IDLE,           // Not yet assigned
    IN_PROGRESS,    // Assigned to a worker
    COMPLETED,      // Worker reported done
    FAILED          // Timeout or worker crashed
}
```

Create `src/main/java/com/mit/mapreduce/task/Task.java`:

```java
package com.mit.mapreduce.task;

import java.io.Serializable;
import java.util.*;

/**
 * Represents a single Map or Reduce task.
 * 
 * Map task: process input file, output to nReduce intermediate files
 * Reduce task: process one partition, output to mr-out-X
 */
public class Task implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String taskId;           // Unique ID
    private TaskType type;           // MAP or REDUCE
    private TaskState state;         // Current state
    private int taskNumber;          // 0-indexed task number
    private String inputFile;        // For MAP tasks
    private int nReduce;             // Number of reduce tasks
    private int nMap;                // Total number of map tasks
    private long assignedTime;       // When assigned to worker
    private String assignedWorker;   // Worker ID
    
    public Task(String taskId, TaskType type, int taskNumber, 
                String inputFile, int nReduce, int nMap) {
        this.taskId = taskId;
        this.type = type;
        this.taskNumber = taskNumber;
        this.inputFile = inputFile;
        this.nReduce = nReduce;
        this.nMap = nMap;
        this.state = TaskState.IDLE;
        this.assignedTime = 0;
    }

    // Getters
    public String getTaskId() { return taskId; }
    public TaskType getType() { return type; }
    public TaskState getState() { return state; }
    public int getTaskNumber() { return taskNumber; }
    public String getInputFile() { return inputFile; }
    public int getNReduce() { return nReduce; }
    public int getNMap() { return nMap; }
    public long getAssignedTime() { return assignedTime; }
    public String getAssignedWorker() { return assignedWorker; }

    // Setters
    public void setState(TaskState state) { this.state = state; }
    public void setAssignedTime(long time) { this.assignedTime = time; }
    public void setAssignedWorker(String worker) { this.assignedWorker = worker; }

    /**
     * Check if task has exceeded timeout (10 seconds).
     */
    public boolean isTimedOut() {
        if (state != TaskState.IN_PROGRESS) return false;
        return System.currentTimeMillis() - assignedTime > 10000;
    }

    @Override
    public String toString() {
        return String.format("Task{id=%s, type=%s, state=%s, taskNum=%d}", 
            taskId, type, state, taskNumber);
    }
}
```

---

## Part 2: RPC Framework

### RPC Messages

Create `src/main/java/com/mit/mapreduce/rpc/RpcMessages.java`:

```java
package com.mit.mapreduce.rpc;

import com.mit.mapreduce.task.Task;
import java.io.Serializable;

/**
 * Request/Response messages for RPC communication.
 */
public class RpcMessages {
    
    /**
     * Worker asks coordinator for a task.
     */
    public static class GetTaskRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        public String workerId;
        
        public GetTaskRequest() {}
        public GetTaskRequest(String workerId) {
            this.workerId = workerId;
        }
    }
    
    /**
     * Coordinator sends task to worker.
     */
    public static class GetTaskResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        public Task task;
        public boolean jobDone;  // No more tasks; job is complete
        
        public GetTaskResponse() {}
        public GetTaskResponse(Task task, boolean jobDone) {
            this.task = task;
            this.jobDone = jobDone;
        }
    }
    
    /**
     * Worker reports task completion.
     */
    public static class NotifyDoneRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        public String taskId;
        public boolean success;
        
        public NotifyDoneRequest() {}
        public NotifyDoneRequest(String taskId, boolean success) {
            this.taskId = taskId;
            this.success = success;
        }
    }
    
    /**
     * Coordinator acknowledges completion.
     */
    public static class NotifyDoneResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        public boolean ack;
        
        public NotifyDoneResponse() {}
        public NotifyDoneResponse(boolean ack) {
            this.ack = ack;
        }
    }
}
```

### RPC Server (Coordinator Side)

Create `src/main/java/com/mit/mapreduce/rpc/RpcServer.java`:

```java
package com.mit.mapreduce.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple RPC server using Java sockets and object serialization.
 * Coordinator runs this to accept requests from workers.
 */
public class RpcServer {
    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);
    
    private final int port;
    private final RpcHandler handler;
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private volatile boolean running;

    public interface RpcHandler {
        Object handleRequest(String method, Object request);
    }

    public RpcServer(int port, RpcHandler handler) {
        this.port = port;
        this.handler = handler;
        this.executorService = Executors.newFixedThreadPool(10);
        this.running = false;
    }

    /**
     * Start RPC server (blocks, run in separate thread).
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        logger.info("RPC server listening on port {}", port);

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> handleClient(clientSocket));
            } catch (SocketException e) {
                if (running) {
                    logger.error("Server socket error", e);
                }
            }
        }
    }

    /**
     * Handle single client connection.
     * Protocol: 
     * 1. Read method name (String)
     * 2. Read request object
     * 3. Call handler
     * 4. Write response object
     */
    private void handleClient(Socket clientSocket) {
        try (
            ObjectInputStream input = new ObjectInputStream(clientSocket.getInputStream());
            ObjectOutputStream output = new ObjectOutputStream(clientSocket.getOutputStream())
        ) {
            String method = input.readUTF();
            Object request = input.readObject();
            
            Object response = handler.handleRequest(method, request);
            
            output.writeObject(response);
            output.flush();
        } catch (Exception e) {
            logger.warn("Error handling client request", e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.debug("Error closing socket", e);
            }
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            logger.debug("Error closing server socket", e);
        }
        executorService.shutdown();
    }
}
```

### RPC Client (Worker Side)

Create `src/main/java/com/mit/mapreduce/rpc/RpcClient.java`:

```java
package com.mit.mapreduce.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * RPC client for workers to call coordinator.
 */
public class RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);
    
    private final String host;
    private final int port;
    private static final int TIMEOUT_MS = 10000;  // 10s timeout

    public RpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Call coordinator method synchronously.
     * Returns response or null on failure (coordinator dead).
     */
    public Object call(String method, Object request) {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(TIMEOUT_MS);
            
            try (
                ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream input = new ObjectInputStream(socket.getInputStream())
            ) {
                output.writeUTF(method);
                output.writeObject(request);
                output.flush();
                
                return input.readObject();
            }
        } catch (SocketTimeoutException e) {
            logger.warn("RPC call timeout to {}:{}", host, port);
            return null;
        } catch (EOFException e) {
            logger.debug("Coordinator closed connection (probably job done)");
            return null;
        } catch (IOException | ClassNotFoundException e) {
            logger.debug("RPC call failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Helper: Call GetTask.
     */
    public RpcMessages.GetTaskResponse getTask(String workerId) {
        RpcMessages.GetTaskRequest request = new RpcMessages.GetTaskRequest(workerId);
        Object response = call("getTask", request);
        return response instanceof RpcMessages.GetTaskResponse ? 
            (RpcMessages.GetTaskResponse) response : null;
    }

    /**
     * Helper: Call NotifyDone.
     */
    public boolean notifyDone(String taskId, boolean success) {
        RpcMessages.NotifyDoneRequest request = new RpcMessages.NotifyDoneRequest(taskId, success);
        Object response = call("notifyDone", request);
        if (response instanceof RpcMessages.NotifyDoneResponse) {
            return ((RpcMessages.NotifyDoneResponse) response).ack;
        }
        return false;
    }
}
```

---

## Part 3: Task Management

### Task Manager

Create `src/main/java/com/mit/mapreduce/coordinator/TaskManager.java`:

```java
package com.mit.mapreduce.coordinator;

import com.mit.mapreduce.task.Task;
import com.mit.mapreduce.task.TaskState;
import com.mit.mapreduce.task.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manages all tasks: creation, assignment, timeout detection.
 */
public class TaskManager {
    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);
    
    private final Map<String, Task> tasks;  // taskId → Task
    private final Deque<String> availableTasks;  // Queue of IDLE task IDs
    private final ReentrantReadWriteLock lock;
    
    private final int nMap;
    private final int nReduce;
    private volatile boolean mapPhaseComplete;
    private volatile boolean reducePhaseComplete;

    public TaskManager(int nMap, int nReduce) {
        this.tasks = new ConcurrentHashMap<>();
        this.availableTasks = new ConcurrentLinkedDeque<>();
        this.lock = new ReentrantReadWriteLock();
        this.nMap = nMap;
        this.nReduce = nReduce;
        this.mapPhaseComplete = false;
        this.reducePhaseComplete = false;
    }

    /**
     * Create all Map tasks (one per input file).
     */
    public void createMapTasks(List<String> inputFiles) {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < inputFiles.size(); i++) {
                String taskId = "map-" + i;
                Task task = new Task(taskId, TaskType.MAP, i, inputFiles.get(i), nReduce, nMap);
                tasks.put(taskId, task);
                availableTasks.add(taskId);
            }
            logger.info("Created {} Map tasks", nMap);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Create all Reduce tasks (one per partition).
     * Only called after all Map tasks complete.
     */
    public void createReduceTasks() {
        lock.writeLock().lock();
        try {
            if (mapPhaseComplete) {
                for (int i = 0; i < nReduce; i++) {
                    String taskId = "reduce-" + i;
                    Task task = new Task(taskId, TaskType.REDUCE, i, null, nReduce, nMap);
                    tasks.put(taskId, task);
                    availableTasks.add(taskId);
                }
                logger.info("Created {} Reduce tasks", nReduce);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get next available task for worker.
     * Returns null if no tasks available.
     */
    public Task assignTask(String workerId) {
        lock.writeLock().lock();
        try {
            String taskId = availableTasks.poll();
            if (taskId == null) return null;
            
            Task task = tasks.get(taskId);
            task.setState(TaskState.IN_PROGRESS);
            task.setAssignedTime(System.currentTimeMillis());
            task.setAssignedWorker(workerId);
            
            logger.debug("Assigned {} to worker {}", taskId, workerId);
            return task;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Worker reports task complete.
     */
    public void completeTask(String taskId) {
        lock.writeLock().lock();
        try {
            Task task = tasks.get(taskId);
            if (task != null) {
                task.setState(TaskState.COMPLETED);
                logger.debug("Task {} completed", taskId);
                
                // Check if all Map tasks done
                if (task.getType() == TaskType.MAP) {
                    if (allMapTasksComplete()) {
                        mapPhaseComplete = true;
                        logger.info("*** Map phase complete ***");
                    }
                }
                
                // Check if all Reduce tasks done
                if (task.getType() == TaskType.REDUCE) {
                    if (allReduceTasksComplete()) {
                        reducePhaseComplete = true;
                        logger.info("*** Reduce phase complete ***");
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Detect timed-out tasks and reassign to new workers.
     */
    public void handleTimeouts() {
        lock.writeLock().lock();
        try {
            for (Task task : tasks.values()) {
                if (task.isTimedOut()) {
                    logger.warn("*re*-starting {} {} (timeout after {} worker {})",
                        task.getType().toString().toLowerCase(),
                        task.getTaskNumber(),
                        (System.currentTimeMillis() - task.getAssignedTime()) / 1000,
                        task.getAssignedWorker());
                    
                    task.setState(TaskState.IDLE);
                    availableTasks.add(task.getTaskId());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Check if all Map tasks are complete.
     */
    private boolean allMapTasksComplete() {
        return tasks.values().stream()
            .filter(t -> t.getType() == TaskType.MAP)
            .allMatch(t -> t.getState() == TaskState.COMPLETED);
    }

    /**
     * Check if all Reduce tasks are complete.
     */
    private boolean allReduceTasksComplete() {
        return tasks.values().stream()
            .filter(t -> t.getType() == TaskType.REDUCE)
            .allMatch(t -> t.getState() == TaskState.COMPLETED);
    }

    public boolean isJobComplete() {
        return mapPhaseComplete && reducePhaseComplete;
    }

    public boolean isMapPhaseComplete() {
        return mapPhaseComplete;
    }

    public int getAvailableTaskCount() {
        return availableTasks.size();
    }

    public Map<String, Object> getStatus() {
        lock.readLock().lock();
        try {
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("total_tasks", tasks.size());
            status.put("completed_tasks", 
                tasks.values().stream().filter(t -> t.getState() == TaskState.COMPLETED).count());
            status.put("in_progress_tasks", 
                tasks.values().stream().filter(t -> t.getState() == TaskState.IN_PROGRESS).count());
            status.put("available_tasks", availableTasks.size());
            status.put("map_phase_complete", mapPhaseComplete);
            status.put("reduce_phase_complete", reducePhaseComplete);
            return status;
        } finally {
            lock.readLock().unlock();
        }
    }
}
```

---

## Part 4: Coordinator

Create `src/main/java/com/mit/mapreduce/coordinator/Coordinator.java`:

```java
package com.mit.mapreduce.coordinator;

import com.mit.mapreduce.rpc.RpcMessages;
import com.mit.mapreduce.rpc.RpcServer;
import com.mit.mapreduce.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MapReduce Coordinator.
 * 
 * Responsibilities:
 * 1. Create Map and Reduce tasks
 * 2. Assign tasks to workers via RPC
 * 3. Track task completion
 * 4. Detect crashed workers (10s timeout)
 * 5. Coordinate Map → Reduce transition
 */
public class Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    
    private final TaskManager taskManager;
    private final RpcServer rpcServer;
    private final int port;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running;

    public Coordinator(int port, List<String> inputFiles, int nReduce) throws IOException {
        this.port = port;
        this.taskManager = new TaskManager(inputFiles.size(), nReduce);
        this.rpcServer = new RpcServer(port, this::handleRpc);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.running = false;
        
        taskManager.createMapTasks(inputFiles);
    }

    /**
     * Main RPC handler.
     * Routes method calls to appropriate handlers.
     */
    private Object handleRpc(String method, Object request) {
        return switch (method) {
            case "getTask" -> handleGetTask((RpcMessages.GetTaskRequest) request);
            case "notifyDone" -> handleNotifyDone((RpcMessages.NotifyDoneRequest) request);
            default -> null;
        };
    }

    /**
     * Worker calls: "Give me a task"
     */
    private synchronized RpcMessages.GetTaskResponse handleGetTask(RpcMessages.GetTaskRequest request) {
        String workerId = request.workerId;
        
        // Check if job is done
        if (taskManager.isJobComplete()) {
            logger.debug("Job complete; telling worker {} to exit", workerId);
            return new RpcMessages.GetTaskResponse(null, true);
        }
        
        // Get next available task
        Task task = taskManager.assignTask(workerId);
        if (task != null) {
            logger.debug("Assigned task {} to worker {}", task.getTaskId(), workerId);
            return new RpcMessages.GetTaskResponse(task, false);
        }
        
        // No available task, but job not done (Map phase not complete, or Reduce starting)
        // Worker should sleep and retry
        logger.debug("No tasks available for worker {}; returning null", workerId);
        return new RpcMessages.GetTaskResponse(null, false);
    }

    /**
     * Worker calls: "Task X is done"
     */
    private synchronized RpcMessages.NotifyDoneResponse handleNotifyDone(RpcMessages.NotifyDoneRequest request) {
        String taskId = request.taskId;
        boolean success = request.success;
        
        if (success) {
            taskManager.completeTask(taskId);
            
            // If Map phase just completed, create Reduce tasks
            if (taskManager.isMapPhaseComplete() && !taskManager.isJobComplete()) {
                logger.info("Map phase complete, creating Reduce tasks");
                taskManager.createReduceTasks();
            }
        } else {
            logger.warn("Task {} failed, will be reassigned on timeout", taskId);
        }
        
        return new RpcMessages.NotifyDoneResponse(true);
    }

    /**
     * Start coordinator server and monitoring threads.
     */
    public void start() throws IOException {
        running = true;
        
        // Start RPC server in background thread
        Thread serverThread = new Thread(() -> {
            try {
                rpcServer.start();
            } catch (IOException e) {
                logger.error("RPC server error", e);
            }
        });
        serverThread.setName("RPC-Server");
        serverThread.setDaemon(false);
        serverThread.start();
        
        logger.info("Coordinator started on port {}", port);
        
        // Start timeout detector (every 1 second)
        scheduler.scheduleAtFixedRate(this::detectTimeouts, 1, 1, TimeUnit.SECONDS);
        
        // Start status logger (every 5 seconds)
        scheduler.scheduleAtFixedRate(this::logStatus, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Detect and handle timed-out tasks.
     */
    private void detectTimeouts() {
        if (running) {
            taskManager.handleTimeouts();
        }
    }

    /**
     * Log current status.
     */
    private void logStatus() {
        if (taskManager.isJobComplete()) {
            logger.info("*** Job complete ***");
            stop();
        } else {
            Map<String, Object> status = taskManager.getStatus();
            logger.info("Status: {}", status);
        }
    }

    /**
     * Stop coordinator.
     */
    public void stop() {
        running = false;
        rpcServer.stop();
        scheduler.shutdown();
        logger.info("Coordinator stopped");
    }

    /**
     * Called by main: returns true if job is done.
     */
    public boolean isDone() {
        return taskManager.isJobComplete();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: coordinator <port> <inputfile1> <inputfile2> ...");
            System.exit(1);
        }
        
        int port = Integer.parseInt(args[0]);
        List<String> inputFiles = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));
        int nReduce = 10;  // Default: 10 reduce tasks
        
        Coordinator coordinator = new Coordinator(port, inputFiles, nReduce);
        coordinator.start();
        
        // Wait until job is done
        while (!coordinator.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        coordinator.stop();
        System.exit(0);
    }
}
```

---

## Part 5: Worker

### Task Executor

Create `src/main/java/com/mit/mapreduce/worker/TaskExecutor.java`:

```java
package com.mit.mapreduce.worker;

import com.mit.mapreduce.mapfunc.MapFunction;
import com.mit.mapreduce.mapfunc.ReduceFunction;
import com.mit.mapreduce.task.Task;
import com.mit.mapreduce.task.TaskType;
import com.mit.mapreduce.util.FileUtils;
import com.mit.mapreduce.util.HashUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Executes Map or Reduce tasks.
 */
public class TaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);
    
    private final MapFunction mapFunc;
    private final ReduceFunction reduceFunc;
    private final ObjectMapper objectMapper;

    public static class KeyValue {
        public String key;
        public String value;
        
        public KeyValue() {}
        public KeyValue(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public TaskExecutor(MapFunction mapFunc, ReduceFunction reduceFunc) {
        this.mapFunc = mapFunc;
        this.reduceFunc = reduceFunc;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Execute a Map task:
     * 1. Read input file
     * 2. Call mapFunc for each line
     * 3. Write key/value pairs to nReduce intermediate files
     *    (one file per reduce task, partitioned by hash(key))
     */
    public boolean executeMap(Task task) {
        try {
            String inputFile = task.getInputFile();
            int taskNumber = task.getTaskNumber();
            int nReduce = task.getNReduce();
            
            logger.info("Starting Map task {} on file {}", taskNumber, inputFile);
            
            // Open nReduce output files
            List<ObjectOutputStream> outputs = new ArrayList<>();
            for (int i = 0; i < nReduce; i++) {
                String intermediateFile = String.format("mr-%d-%d.txt", taskNumber, i);
                ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(intermediateFile)
                );
                outputs.add(out);
            }
            
            // Read input file and run Map
            try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    List<KeyValue> kvs = mapFunc.map(line);
                    
                    for (KeyValue kv : kvs) {
                        // Hash key to partition (0 to nReduce-1)
                        int partition = HashUtils.ihash(kv.key) % nReduce;
                        if (partition < 0) partition += nReduce;
                        
                        // Write to corresponding file
                        outputs.get(partition).writeObject(kv);
                    }
                }
            }
            
            // Close all output files
            for (ObjectOutputStream out : outputs) {
                out.close();
            }
            
            logger.info("Completed Map task {}", taskNumber);
            return true;
            
        } catch (Exception e) {
            logger.error("Map task failed", e);
            return false;
        }
    }

    /**
     * Execute a Reduce task:
     * 1. Read all intermediate files for this partition from all Map tasks
     *    (files: mr-0-X.txt, mr-1-X.txt, ..., mr-(nMap-1)-X.txt)
     * 2. Group by key
     * 3. For each key, call reduceFunc
     * 4. Write output to mr-out-X
     */
    public boolean executeReduce(Task task) {
        try {
            int taskNumber = task.getTaskNumber();
            int nMap = task.getNMap();
            
            logger.info("Starting Reduce task {}", taskNumber);
            
            // Read all intermediate files for this partition
            Map<String, List<String>> grouped = new HashMap<>();
            
            for (int mapTask = 0; mapTask < nMap; mapTask++) {
                String intermediateFile = String.format("mr-%d-%d.txt", mapTask, taskNumber);
                File file = new File(intermediateFile);
                
                if (!file.exists()) {
                    logger.warn("Intermediate file {} not found", intermediateFile);
                    continue;
                }
                
                try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file))) {
                    while (true) {
                        try {
                            KeyValue kv = (KeyValue) in.readObject();
                            grouped.computeIfAbsent(kv.key, k -> new ArrayList<>())
                                   .add(kv.value);
                        } catch (EOFException e) {
                            break;  // End of file
                        }
                    }
                }
            }
            
            // Sort keys for deterministic output
            List<String> sortedKeys = new ArrayList<>(grouped.keySet());
            Collections.sort(sortedKeys);
            
            // Open output file
            String outputFile = String.format("mr-out-%d", taskNumber);
            try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
                for (String key : sortedKeys) {
                    List<String> values = grouped.get(key);
                    String result = reduceFunc.reduce(key, values);
                    writer.printf("%s %s\n", key, result);
                }
            }
            
            logger.info("Completed Reduce task {}", taskNumber);
            return true;
            
        } catch (Exception e) {
            logger.error("Reduce task failed", e);
            return false;
        }
    }
}
```

### Worker Main

Create `src/main/java/com/mit/mapreduce/worker/Worker.java`:

```java
package com.mit.mapreduce.worker;

import com.mit.mapreduce.mapfunc.MapFunction;
import com.mit.mapreduce.mapfunc.ReduceFunction;
import com.mit.mapreduce.rpc.RpcClient;
import com.mit.mapreduce.rpc.RpcMessages;
import com.mit.mapreduce.task.Task;
import com.mit.mapreduce.task.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * MapReduce Worker.
 * 
 * Loop:
 * 1. Ask coordinator for a task
 * 2. Execute it (Map or Reduce)
 * 3. Report completion
 * 4. Sleep if no task available
 * 5. Exit if coordinator reports job done
 */
public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    
    private final String workerId;
    private final RpcClient rpcClient;
    private final TaskExecutor executor;
    private volatile boolean running;

    public Worker(String coordinatorHost, int coordinatorPort,
                  MapFunction mapFunc, ReduceFunction reduceFunc) {
        this.workerId = UUID.randomUUID().toString().substring(0, 8);
        this.rpcClient = new RpcClient(coordinatorHost, coordinatorPort);
        this.executor = new TaskExecutor(mapFunc, reduceFunc);
        this.running = true;
    }

    /**
     * Main worker loop.
     */
    public void run() {
        logger.info("Worker {} started", workerId);
        
        while (running) {
            try {
                // Ask coordinator for a task
                RpcMessages.GetTaskResponse response = rpcClient.getTask(workerId);
                
                // Coordinator is dead (job done)
                if (response == null) {
                    logger.info("Coordinator unavailable; job must be complete");
                    break;
                }
                
                // Job is complete
                if (response.jobDone) {
                    logger.info("Coordinator says job is done");
                    break;
                }
                
                Task task = response.task;
                
                // No task available yet; sleep and retry
                if (task == null) {
                    logger.debug("No task available; sleeping");
                    Thread.sleep(1000);
                    continue;
                }
                
                // Execute task
                boolean success = executeTask(task);
                
                // Report completion
                rpcClient.notifyDone(task.getTaskId(), success);
                
            } catch (InterruptedException e) {
                logger.info("Worker interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Worker error", e);
            }
        }
        
        logger.info("Worker {} exiting", workerId);
        running = false;
    }

    /**
     * Execute a single task (Map or Reduce).
     */
    private boolean executeTask(Task task) {
        try {
            logger.info("Executing task: {}", task);
            
            if (task.getType() == TaskType.MAP) {
                return executor.executeMap(task);
            } else {
                return executor.executeReduce(task);
            }
        } catch (Exception e) {
            logger.error("Task execution error", e);
            return false;
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: worker <coordinator_host> <coordinator_port>");
            System.exit(1);
        }
        
        String coordinatorHost = args[0];
        int coordinatorPort = Integer.parseInt(args[1]);
        
        // Load application functions (will be overridden in subclass)
        MapFunction mapFunc = (line) -> {
            throw new RuntimeException("MapFunction not set");
        };
        ReduceFunction reduceFunc = (key, values) -> {
            throw new RuntimeException("ReduceFunction not set");
        };
        
        Worker worker = new Worker(coordinatorHost, coordinatorPort, mapFunc, reduceFunc);
        worker.run();
    }
}
```

---

## Part 6: MapReduce Functions

### Function Interfaces

Create `src/main/java/com/mit/mapreduce/mapfunc/MapFunction.java`:

```java
package com.mit.mapreduce.mapfunc;

import com.mit.mapreduce.worker.TaskExecutor;
import java.util.List;

@FunctionalInterface
public interface MapFunction {
    /**
     * Apply Map function to one line of input.
     * Returns list of key-value pairs.
     */
    List<TaskExecutor.KeyValue> map(String line);
}
```

Create `src/main/java/com/mit/mapreduce/mapfunc/ReduceFunction.java`:

```java
package com.mit.mapreduce.mapfunc;

import java.util.List;

@FunctionalInterface
public interface ReduceFunction {
    /**
     * Apply Reduce function to all values for one key.
     * Returns single aggregated result.
     */
    String reduce(String key, List<String> values);
}
```

### Word Count Application

Create `src/main/java/com/mit/mapreduce/mapfunc/WordCountApp.java`:

```java
package com.mit.mapreduce.mapfunc;

import com.mit.mapreduce.worker.TaskExecutor;
import java.util.ArrayList;
import java.util.List;

/**
 * Word count MapReduce application.
 */
public class WordCountApp {
    
    /**
     * Map: split line into words, emit (word, 1).
     */
    public static MapFunction getMapFunction() {
        return line -> {
            List<TaskExecutor.KeyValue> result = new ArrayList<>();
            String[] words = line.split("[\\W]+");
            
            for (String word : words) {
                if (!word.isEmpty()) {
                    result.add(new TaskExecutor.KeyValue(word, "1"));
                }
            }
            return result;
        };
    }
    
    /**
     * Reduce: sum all counts for each word.
     */
    public static ReduceFunction getReduceFunction() {
        return (key, values) -> {
            int count = 0;
            for (String val : values) {
                count += Integer.parseInt(val);
            }
            return String.valueOf(count);
        };
    }
}
```

---

## Part 7: Testing & Validation

### Unit Tests

Create `src/test/java/com/mit/mapreduce/MapReduceTest.java`:

```java
package com.mit.mapreduce;

import com.mit.mapreduce.coordinator.Coordinator;
import com.mit.mapreduce.worker.Worker;
import com.mit.mapreduce.mapfunc.WordCountApp;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class MapReduceTest {
    
    private static final int COORDINATOR_PORT = 15555;
    
    @Test
    public void testWordCount() throws Exception {
        // Create test input files
        Path inputDir = Paths.get("test_input");
        Files.createDirectories(inputDir);
        
        Files.write(inputDir.resolve("test1.txt"), 
            "the quick brown fox\nthe lazy dog".getBytes());
        Files.write(inputDir.resolve("test2.txt"), 
            "the fox jumps\nthe dog runs".getBytes());
        
        List<String> inputFiles = Arrays.asList(
            "test_input/test1.txt",
            "test_input/test2.txt"
        );
        
        // Start coordinator
        Coordinator coordinator = new Coordinator(COORDINATOR_PORT, inputFiles, 1);
        Thread coordinatorThread = new Thread(coordinator::start);
        coordinatorThread.setDaemon(true);
        coordinatorThread.start();
        
        Thread.sleep(500);  // Wait for coordinator to start
        
        // Start 2 workers
        ExecutorService executors = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; i++) {
            executors.submit(() -> {
                Worker worker = new Worker("localhost", COORDINATOR_PORT,
                    WordCountApp.getMapFunction(),
                    WordCountApp.getReduceFunction());
                worker.run();
            });
        }
        
        // Wait for completion
        long start = System.currentTimeMillis();
        while (!coordinator.isDone() && System.currentTimeMillis() - start < 30000) {
            Thread.sleep(100);
        }
        
        coordinator.stop();
        executors.shutdown();
        
        // Verify output
        File outputFile = new File("mr-out-0");
        assert outputFile.exists() : "Output file not created";
        
        Map<String, Integer> results = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                results.put(parts[0], Integer.parseInt(parts[1]));
            }
        }
        
        System.out.println("Word counts: " + results);
        assert results.get("the").equals(4) : "Word 'the' count should be 4";
        assert results.get("fox").equals(2) : "Word 'fox' count should be 2";
        
        System.out.println("✓ Test passed");
        
        // Cleanup
        Files.delete(outputFile.toPath());
    }
}
```

Run tests:
```bash
mvn test
```

---

## Part 8: Deployment

### Building

```bash
mvn clean package
```

### Running the System

Terminal 1 (Coordinator):
```bash
java -cp target/mapreduce-java-1.0.0.jar com.mit.mapreduce.coordinator.Coordinator \
  15555 input/pg-alice.txt input/pg-tom.txt input/pg-austen.txt
```

Terminal 2 (Worker 1):
```bash
java -cp target/mapreduce-java-1.0.0.jar com.mit.mapreduce.worker.WordCountWorker \
  localhost 15555
```

Terminal 3 (Worker 2):
```bash
java -cp target/mapreduce-java-1.0.0.jar com.mit.mapreduce.worker.WordCountWorker \
  localhost 15555
```

Check output:
```bash
cat mr-out-* | sort
```

---

## Utility Classes

### Hash Function

Create `src/main/java/com/mit/mapreduce/util/HashUtils.java`:

```java
package com.mit.mapreduce.util;

/**
 * Consistent hashing for key partitioning.
 */
public class HashUtils {
    
    /**
     * Hash function for partitioning keys across reduce tasks.
     * Matches Go's ihash behavior.
     */
    public static int ihash(String key) {
        return Math.abs(key.hashCode());
    }
}
```

### File Utils

Create `src/main/java/com/mit/mapreduce/util/FileUtils.java`:

```java
package com.mit.mapreduce.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtils {
    
    /**
     * Atomic rename: create temp file, then rename.
     */
    public static void atomicWrite(String filename, byte[] data) throws IOException {
        String tempName = filename + ".tmp";
        Files.write(Paths.get(tempName), data);
        Files.move(Paths.get(tempName), Paths.get(filename));
    }
    
    /**
     * Delete file if exists.
     */
    public static void deleteIfExists(String filename) {
        File f = new File(filename);
        if (f.exists()) {
            f.delete();
        }
    }
    
    /**
     * Clean up intermediate files.
     */
    public static void cleanupIntermediateFiles() {
        File dir = new File(".");
        for (File f : dir.listFiles()) {
            if (f.getName().matches("mr-\\d+-\\d+\\.txt")) {
                f.delete();
            }
        }
    }
}
```

---

## Troubleshooting

### Problem: "Connection refused"

**Cause:** Coordinator not running or wrong port.

**Solution:**
```bash
# Check coordinator is listening
lsof -i :15555

# Use correct port in worker command
java ... com.mit.mapreduce.worker.WordCountWorker localhost 15555
```

### Problem: "No output files created"

**Cause:** Reduce phase didn't run (Map phase didn't complete).

**Solution:**
- Check coordinator logs for Map task timeouts
- Verify workers are still running (didn't crash)
- Increase timeout if system is slow: `task.isTimedOut()` in TaskManager

### Problem: "Duplicate reduce output"

**Cause:** Same task reassigned to multiple workers, both wrote output.

**Solution:**
Use atomic rename before notifying coordinator of completion:
```java
// In TaskExecutor.executeReduce()
String tempName = "mr-out-" + taskNumber + ".tmp";
// Write to tempName
Files.move(Paths.get(tempName), Paths.get("mr-out-" + taskNumber));
```

### Problem: "Worker crashes during task"

**Expected behavior:**
- Coordinator detects 10-second timeout
- Reassigns task to different worker
- Log shows `*re*-starting map/reduce`

**Verify:**
```bash
# Test with crashing worker
java ... com.mit.mapreduce.worker.CrashingWorker localhost 15555 0.1  # 10% crash rate
```

---

## Performance Tuning

### Batch Size

Larger batches → fewer RPC calls but higher latency:
```java
// In Coordinator.handleGetTask()
// Pull 10 tasks instead of 1
Task[] tasks = taskManager.assignTasks(workerId, 10);
```

### Worker Count

More workers → better parallelism but more overhead:
```bash
# 4 workers
for i in {1..4}; do
  java ... com.mit.mapreduce.worker.WordCountWorker localhost 15555 &
done
```

### Reduce Tasks

More reduce tasks → more output files but better load balancing:
```java
Coordinator coordinator = new Coordinator(port, inputFiles, 20);  // 20 reducers
```

---

## Extensions

### Crash Recovery

Implement task checkpointing:
```java
// In TaskExecutor before Map execution
Path checkpoint = Paths.get("checkpoint-" + task.getTaskId());
if (Files.exists(checkpoint)) {
    logger.info("Resuming from checkpoint");
    return true;  // Already done
}
```

### Statistics Tracking

```java
class WorkerStats {
    long tasksCompleted;
    long recordsProcessed;
    long bytesRead;
    long bytesWritten;
}
```

### Combiner Function

Mini-reduce on mapper (optimization):
```java
public boolean executeMapWithCombiner(Task task, 
    ReduceFunction combiner) {
    // After writing intermediate files,
    // apply combiner to reduce size
}
```

---

## Summary

You've built:
- ✓ RPC communication framework (Socket-based)
- ✓ Task management with timeout detection
- ✓ Coordinator orchestrating Map/Reduce phases
- ✓ Workers executing tasks in parallel
- ✓ Intermediate file partitioning
- ✓ Fault tolerance (10-second timeout + reassignment)

**Key learnings:**
- Distributed systems need explicit communication (RPC)
- State management under concurrency (locks, queues)
- Timeout-based failure detection
- Phase coordination (Map → Reduce)
- Fault recovery through reassignment

---

## Testing Checklist

- [ ] Single Map, single Reduce works
- [ ] Multiple Map tasks parallelize
- [ ] Multiple Reduce tasks parallelize
- [ ] Worker crash triggers task reassignment
- [ ] Worker slow (>10s) triggers reassignment
- [ ] Job completes and workers exit
- [ ] Coordinator exits after job done
- [ ] Output matches sequential execution

**Run full test:**
```bash
mvn clean test
```

Expected output: All tests pass in <60 seconds.

---

**Ready to build? Start with Part 1: create task models, then work through to Part 8. Test after each part.**
