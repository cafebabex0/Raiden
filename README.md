# Build a Concurrent Data Cleaner from Scratch (Java + Kafka + PostgreSQL)

A complete guide to building a production-grade data cleaning pipeline. You'll understand every component and why it exists.

---

## Table of Contents

1. [Conceptual Foundation](#conceptual-foundation)
2. [Project Setup](#project-setup)
3. [Part 1: Core Data Model](#part-1-core-data-model)
4. [Part 2: Cleaning Engine](#part-2-cleaning-engine)
5. [Part 3: PostgreSQL Persistence](#part-3-postgresql-persistence)
6. [Part 4: Kafka Consumer](#part-4-kafka-consumer)
7. [Part 5: Load Balancer](#part-5-load-balancer)
8. [Part 6: Main Orchestrator](#part-6-main-orchestrator)
9. [Part 7: Configuration System](#part-7-configuration-system)
10. [Part 8: JavaFX Dashboard](#part-8-javafx-dashboard)
11. [Part 9: Testing & Validation](#part-9-testing--validation)
12. [Part 10: Docker & Deployment](#part-10-docker--deployment)

---

## Conceptual Foundation

Before coding, understand the problem and architecture.

### The Problem

You have **messy data streaming from Kafka**:
- Some fields are `null`
- Some records are exact duplicates
- Thousands arriving per second
- Need to clean and store in PostgreSQL

Example dirty batch:
```json
[
  {"id": "1", "name": "Alice", "email": null, "age": 30},
  {"id": "2", "name": "Bob", "email": "bob@example.com", "age": null},
  {"id": "1", "name": "Alice", "email": null, "age": 30},  // duplicate!
  {"id": "3", "name": "", "email": "charlie@example.com", "age": 25}  // empty name
]
```

### Desired Output

```json
[
  {"id": "1", "name": "Alice", "age": 30},  // null email removed, deduplicated
  {"id": "2", "name": "Bob", "email": "bob@example.com"},  // null age removed
  {"id": "3", "email": "charlie@example.com", "age": 25}  // empty name removed
]
```

### Why Concurrency Matters

If records arrive at **10,000/sec**:
- Single-threaded: 10,000 records × 1ms = 10 seconds to process one batch
- 8 threads: 10,000 records ÷ 8 = 1.25 seconds
- Database batching: 1000 records per INSERT = 10 operations × 50ms = 500ms

Total latency single-threaded: **10.5 seconds**  
Total latency with 8 threads + batching: **2 seconds**

That's why we need concurrency.

### Architecture Overview

```
┌──────────────┐
│  Kafka       │  Raw data streaming in
└──────┬───────┘
       │
       ▼
┌──────────────────────┐
│ Consumer Thread      │  Polls Kafka, accumulates batches
└──────┬───────────────┘
       │ (1000 records)
       ▼
┌──────────────────────┐
│ BlockingQueue        │  Backpressure control
└──────┬───────────────┘
       │
       ▼
┌───────────────────────────────┐
│ Worker Thread Pool (8)        │  Process concurrently
│  ├─ Remove nulls              │
│  ├─ Deduplicate               │
│  └─ Track metrics             │
└──────┬────────────────────────┘
       │
       ▼
┌──────────────────────┐
│ Database Pool        │  HikariCP (20 connections)
└──────┬───────────────┘
       │
       ▼
┌──────────────────────┐
│ PostgreSQL           │  Persisted cleaned data
└──────────────────────┘
```

Each component has a specific job; they communicate via queues and thread-safe data structures.

---

## Project Setup

### Prerequisites

Install these first:

```bash
# Java 21 (or later)
java -version  # Should show 21+

# Maven 3.8+
mvn -version

# PostgreSQL 13+
psql --version

# Docker (optional, for running Kafka/PostgreSQL)
docker --version
docker-compose --version
```

### Directory Structure

Create this structure:

```
data-cleaner/
├── pom.xml                          # Maven config
├── config.yaml                      # Configuration
├── README.md                        # Documentation
│
└── src/
    └── main/
        ├── java/com/daetl/cleaner/
        │   ├── DataCleanerApp.java          # Main entry point
        │   ├── config/
        │   │   └── CleanerConfig.java
        │   ├── model/
        │   │   └── DataRecord.java
        │   ├── engine/
        │   │   └── DataCleaningEngine.java
        │   ├── kafka/
        │   │   └── SecureKafkaConsumer.java
        │   ├── db/
        │   │   └── PostgresPersistence.java
        │   ├── lb/
        │   │   └── LoadBalancer.java
        │   ├── gui/
        │   │   └── CleanerDashboard.java
        │   └── test/
        │       └── SampleDataProducer.java
        └── resources/
            └── logback.xml              # Logging config
```

Create the directories:

```bash
mkdir -p data-cleaner/src/main/java/com/daetl/cleaner/{config,model,engine,kafka,db,lb,gui,test}
mkdir -p data-cleaner/src/main/resources
cd data-cleaner
```

### Maven Configuration (pom.xml)

This is your build configuration. Create `pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.daetl</groupId>
    <artifactId>data-cleaner</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <kafka.version>3.6.0</kafka.version>
    </properties>

    <dependencies>
        <!-- Kafka for streaming -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- PostgreSQL driver -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.7.0</version>
        </dependency>

        <!-- Connection pooling (critical for performance) -->
        <dependency>
            <groupId>com.zaxxer</groupId>
            <artifactId>HikariCP</artifactId>
            <version>5.1.0</version>
        </dependency>

        <!-- JSON processing -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.16.1</version>
        </dependency>

        <!-- YAML parsing (for config.yaml) -->
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

        <!-- JavaFX for GUI -->
        <dependency>
            <groupId>org.openjfx</groupId>
            <artifactId>javafx-controls</artifactId>
            <version>21</version>
        </dependency>
        <dependency>
            <groupId>org.openjfx</groupId>
            <artifactId>javafx-fxml</artifactId>
            <version>21</version>
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
            <!-- Build fat JAR -->
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
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.daetl.cleaner.DataCleanerApp</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

**What's happening here?**
- `<dependencies>`: Lists libraries we need
- `kafka-clients`: Kafka consumer library
- `postgresql`: JDBC driver to connect to DB
- `HikariCP`: Fast connection pooling
- `jackson-databind`: JSON parsing (Kafka messages are JSON)
- `jackson-dataformat-yaml`: Read config.yaml
- `slf4j + logback`: Logging
- `javafx-*`: GUI toolkit
- `<build>` with `maven-shade-plugin`: Packages everything into one JAR file

Build your project to make sure everything is set up:

```bash
mvn clean compile
```

If you see "BUILD SUCCESS", you're good. If there are errors about dependencies, Maven couldn't download them (network issue or typo).

---

## Part 1: Core Data Model

Start with the data structure. This is simple but foundational.

### Why Start Here?

Every component uses `DataRecord`. Get this right first, then everything else builds on it.

### Concept

A `DataRecord` represents one message from Kafka. It has:
- An ID (unique identifier)
- Fields (key-value pairs, flexible schema)
- Metadata (timestamp, source)

Example:
```
id: "user-123"
fields: {name: "Alice", email: "alice@example.com", age: 30}
timestamp: 1699564800000
source: "kafka"
```

### Implementation

Create `src/main/java/com/daetl/cleaner/model/DataRecord.java`:

```java
package com.daetl.cleaner.model;

import java.util.*;

/**
 * Represents a single data record from Kafka.
 * Flexible schema: fields can be any key-value pairs.
 */
public class DataRecord {
    private String id;
    private Map<String, Object> fields;
    private long timestamp;
    private String source;

    /**
     * Create a new record with ID and fields.
     * Timestamp set to current time automatically.
     */
    public DataRecord(String id, Map<String, Object> fields) {
        this.id = id;
        this.fields = new HashMap<>(fields);  // Copy to avoid external mutations
        this.timestamp = System.currentTimeMillis();
    }

    // Getters and setters
    public String getId() { 
        return id; 
    }
    
    public void setId(String id) { 
        this.id = id; 
    }

    public Map<String, Object> getFields() { 
        return fields; 
    }
    
    public void setFields(Map<String, Object> fields) { 
        this.fields = fields; 
    }

    public long getTimestamp() { 
        return timestamp; 
    }
    
    public void setTimestamp(long timestamp) { 
        this.timestamp = timestamp; 
    }

    public String getSource() { 
        return source; 
    }
    
    public void setSource(String source) { 
        this.source = source; 
    }

    /**
     * Get a single field by key.
     * Usage: record.getField("name") -> "Alice"
     */
    public Object getField(String key) {
        return fields.get(key);
    }

    /**
     * Set a single field.
     */
    public void setField(String key, Object value) {
        fields.put(key, value);
    }

    /**
     * Check if record has any null values.
     * Used for cleaning validation.
     */
    public boolean hasNullValues() {
        return fields.values().stream().anyMatch(v -> 
            v == null || (v instanceof String && ((String)v).isEmpty())
        );
    }

    /**
     * Count how many fields are null or empty.
     */
    public int getNullFieldCount() {
        return (int) fields.values().stream()
            .filter(v -> v == null || (v instanceof String && ((String)v).isEmpty()))
            .count();
    }

    /**
     * Deep copy fields (for safety when passing to other threads).
     */
    public Map<String, Object> deepCopy() {
        return new HashMap<>(fields);
    }

    @Override
    public String toString() {
        return "DataRecord{" +
                "id='" + id + '\'' +
                ", fields=" + fields +
                ", timestamp=" + timestamp +
                '}';
    }

    /**
     * Two records are equal if their fields match.
     * (Used for deduplication checking)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataRecord that = (DataRecord) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
```

**Key decisions:**
- `new HashMap<>(fields)` — Copy on construction to prevent external code from mutating the original
- `hasNullValues()` — Streams API for functional check
- `equals()` based on fields, not ID — For deduplication, we care if content matches

### Testing It

Create a quick test in `src/main/java/com/daetl/cleaner/model/DataRecordTest.java`:

```java
package com.daetl.cleaner.model;

import java.util.*;

public class DataRecordTest {
    public static void main(String[] args) {
        // Test 1: Create a record
        Map<String, Object> fields = new HashMap<>();
        fields.put("name", "Alice");
        fields.put("email", "alice@example.com");
        fields.put("age", 30);

        DataRecord record = new DataRecord("user-1", fields);
        System.out.println("Record: " + record);
        assert record.getField("name").equals("Alice");
        System.out.println("✓ Test 1 passed: Record creation");

        // Test 2: Detect null values
        fields.put("phone", null);
        DataRecord dirtyRecord = new DataRecord("user-2", fields);
        assert dirtyRecord.hasNullValues();
        System.out.println("✓ Test 2 passed: Null detection");

        // Test 3: Deep copy
        Map<String, Object> originalFields = new HashMap<>();
        originalFields.put("x", "original");
        DataRecord record2 = new DataRecord("user-3", originalFields);
        
        originalFields.put("x", "modified");  // Modify original
        assert record2.getField("x").equals("original");  // Record unchanged
        System.out.println("✓ Test 3 passed: Deep copy protection");

        System.out.println("\nAll DataRecord tests passed!");
    }
}
```

Run it:

```bash
javac src/main/java/com/daetl/cleaner/model/DataRecord*.java
java -cp src/main/java com.daetl.cleaner.model.DataRecordTest
```

You should see three passing tests. This validates that `DataRecord` works correctly.

---

## Part 2: Cleaning Engine

Now implement the core cleaning logic: removing nulls and deduplicating.

### Concept

The cleaning engine is the heart of the system. It takes a record and:
1. **Removes null/empty values** → cleaner record
2. **Checks if it's a duplicate** → skip if seen before
3. **Tracks metrics** → counts of processed/cleaned/skipped

**Why separate thread pool?** So 8 records can be cleaned simultaneously instead of one-at-a-time.

**Why ReentrantReadWriteLock?** Dedup checking is read-heavy:
- Most checks: "Is this hash in the set?" (fast read)
- Occasional writes: "Add new hash to set" (slower write)
- Read-write lock lets multiple threads check simultaneously, but exclusive access for writes

### Implementation

Create `src/main/java/com/daetl/cleaner/engine/DataCleaningEngine.java`:

```java
package com.daetl.cleaner.engine;

import com.daetl.cleaner.model.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Cleans records concurrently: removes nulls, deduplicates.
 * 
 * Thread safety: Uses ReentrantReadWriteLock for dedup state.
 * Why? Most operations are reads (checking if seen).
 * Occasional writes (adding new hash). RWLock optimizes this.
 */
public class DataCleaningEngine {
    private static final Logger logger = LoggerFactory.getLogger(DataCleaningEngine.class);
    
    // Tracks which records we've seen (by hash)
    private final Set<Integer> seenRecordHashes;
    
    // Statistics (atomic for thread-safe increments without locks)
    private final AtomicLong recordsProcessed;
    private final AtomicLong recordsCleaned;
    private final AtomicLong recordsSkipped;
    
    // Lock for dedup cache
    private final ReentrantReadWriteLock deduplicationLock;
    private final int concurrentWorkers;

    public DataCleaningEngine(int concurrentWorkers) {
        this.seenRecordHashes = Collections.synchronizedSet(new HashSet<>());
        this.recordsProcessed = new AtomicLong(0);
        this.recordsCleaned = new AtomicLong(0);
        this.recordsSkipped = new AtomicLong(0);
        this.deduplicationLock = new ReentrantReadWriteLock();
        this.concurrentWorkers = concurrentWorkers;
    }

    /**
     * Clean a single record:
     * 1. Remove null values
     * 2. Check if duplicate
     * 3. Return cleaned record with metadata
     */
    public CleanedRecord cleanRecord(DataRecord record) {
        recordsProcessed.incrementAndGet();

        CleanedRecord result = new CleanedRecord(record.getId(), record.getFields());

        // Step 1: Remove nulls (streaming for concurrency safety)
        Map<String, Object> cleanedFields = removeNullValues(record.getFields());
        result.setFields(cleanedFields);
        result.setNullValuesRemoved(record.getFields().size() - cleanedFields.size());

        // Step 2: Check for duplicates
        if (isDuplicate(cleanedFields)) {
            result.setIsDuplicate(true);
            recordsSkipped.incrementAndGet();
            return result;
        }

        recordsCleaned.incrementAndGet();
        result.setStatus("CLEANED");
        return result;
    }

    /**
     * Clean multiple records concurrently using thread pool.
     * 
     * Why not just use a loop?
     * Because 1000 records × 1ms cleaning = 1 second sequentially.
     * With 8 threads: 1000 ÷ 8 = 125ms.
     */
    public List<CleanedRecord> cleanBatch(List<DataRecord> records) {
        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(concurrentWorkers);
        
        try {
            // Submit all records to thread pool
            List<Future<CleanedRecord>> futures = records.stream()
                .map(record -> executor.submit(() -> cleanRecord(record)))
                .collect(Collectors.toList());

            // Wait for results (with timeout)
            List<CleanedRecord> cleaned = new ArrayList<>();
            for (Future<CleanedRecord> future : futures) {
                try {
                    cleaned.add(future.get(30, TimeUnit.SECONDS));
                } catch (TimeoutException e) {
                    logger.warn("Record cleaning timeout");
                }
            }
            return cleaned;
        } finally {
            executor.shutdown();  // Important: free thread pool resources
        }
    }

    /**
     * Remove null and empty string values from a record.
     * 
     * Streams API makes this:
     * - Thread-safe (no side effects)
     * - Concise
     * - Efficient (one pass)
     */
    private Map<String, Object> removeNullValues(Map<String, Object> fields) {
        return fields.entrySet().stream()
            .filter(entry -> entry.getValue() != null)  // Keep non-null
            .filter(entry -> {  // Keep non-empty strings
                if (entry.getValue() instanceof String) {
                    return !((String)entry.getValue()).trim().isEmpty();
                }
                return true;  // Keep other types
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Thread-safe deduplication check using ReentrantReadWriteLock.
     * 
     * Why RWLock?
     * - Read lock: Quick check if we've seen this hash (most of the time)
     * - Write lock: Add new hash (rare, but exclusive)
     * 
     * Double-check pattern:
     * 1. Check with read lock (fast path)
     * 2. If not found, upgrade to write lock
     * 3. Check again before writing (another thread might have added it)
     * 4. Add hash
     */
    private boolean isDuplicate(Map<String, Object> fields) {
        int hash = computeRecordHash(fields);
        
        // Fast path: read lock (multiple threads can do this simultaneously)
        deduplicationLock.readLock().lock();
        try {
            if (seenRecordHashes.contains(hash)) {
                return true;  // Seen before = duplicate
            }
        } finally {
            deduplicationLock.readLock().unlock();
        }

        // Slow path: write lock (exclusive access)
        deduplicationLock.writeLock().lock();
        try {
            // Double-check: another thread might have added it while we released read lock
            if (seenRecordHashes.contains(hash)) {
                return true;
            }
            // Add to seen set
            seenRecordHashes.add(hash);
            return false;  // Not a duplicate
        } finally {
            deduplicationLock.writeLock().unlock();
        }
    }

    /**
     * Compute hash of record (order-independent).
     * 
     * Why order-independent?
     * Kafka messages might have fields in any order.
     * {name:"John", age:30} should hash same as {age:30, name:"John"}
     * 
     * Solution: Sort entries by key, then XOR their hashes.
     */
    private int computeRecordHash(Map<String, Object> fields) {
        return fields.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())  // Sort by key for consistency
            .map(e -> {
                int keyHash = e.getKey().hashCode();
                int valueHash = (e.getValue() != null ? e.getValue().hashCode() : 0);
                return keyHash ^ valueHash;  // XOR combines hashes
            })
            .reduce(0, (a, b) -> a ^ b);  // XOR all together
    }

    /**
     * Clear dedup cache (for new batch or testing).
     */
    public void clearDeduplicationState() {
        deduplicationLock.writeLock().lock();
        try {
            seenRecordHashes.clear();
        } finally {
            deduplicationLock.writeLock().unlock();
        }
    }

    // Metrics accessors
    public long getRecordsProcessed() { 
        return recordsProcessed.get(); 
    }
    
    public long getRecordsCleaned() { 
        return recordsCleaned.get(); 
    }
    
    public long getRecordsSkipped() { 
        return recordsSkipped.get(); 
    }

    /**
     * Result of cleaning a single record.
     * Tracks what happened: nulls removed? duplicate detected?
     */
    public static class CleanedRecord {
        private final String id;
        private Map<String, Object> fields;
        private int nullValuesRemoved;
        private boolean isDuplicate;
        private String status;

        public CleanedRecord(String id, Map<String, Object> fields) {
            this.id = id;
            this.fields = new HashMap<>(fields);
            this.nullValuesRemoved = 0;
            this.isDuplicate = false;
            this.status = "PENDING";
        }

        // Getters/setters
        public String getId() { return id; }
        public Map<String, Object> getFields() { return fields; }
        public void setFields(Map<String, Object> fields) { this.fields = fields; }
        public int getNullValuesRemoved() { return nullValuesRemoved; }
        public void setNullValuesRemoved(int count) { this.nullValuesRemoved = count; }
        public boolean isDuplicate() { return isDuplicate; }
        public void setIsDuplicate(boolean dup) { this.isDuplicate = dup; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        @Override
        public String toString() {
            return "CleanedRecord{" +
                    "id='" + id + '\'' +
                    ", status='" + status + '\'' +
                    ", nullValuesRemoved=" + nullValuesRemoved +
                    ", isDuplicate=" + isDuplicate +
                    '}';
        }
    }
}
```

**Key points:**
- `removeNullValues()`: Streams API makes it concurrency-safe (no shared state)
- `isDuplicate()`: ReentrantReadWriteLock allows multiple threads to check simultaneously
- `computeRecordHash()`: Sorted by key so order doesn't matter
- `cleanBatch()`: ExecutorService submits all records to thread pool, waits for results

### Testing

Create `src/main/java/com/daetl/cleaner/engine/DataCleaningEngineTest.java`:

```java
package com.daetl.cleaner.engine;

import com.daetl.cleaner.model.DataRecord;
import com.daetl.cleaner.engine.DataCleaningEngine.CleanedRecord;

import java.util.*;
import java.util.concurrent.*;

public class DataCleaningEngineTest {
    public static void main(String[] args) throws InterruptedException {
        DataCleaningEngine engine = new DataCleaningEngine(4);

        // Test 1: Remove nulls
        System.out.println("Test 1: Null removal");
        Map<String, Object> fields = new HashMap<>();
        fields.put("name", "Alice");
        fields.put("email", null);
        fields.put("age", 30);
        
        DataRecord record = new DataRecord("1", fields);
        CleanedRecord cleaned = engine.cleanRecord(record);
        
        assert cleaned.getNullValuesRemoved() == 1 : "Should remove 1 null";
        assert cleaned.getFields().size() == 2 : "Should have 2 fields left";
        assert !cleaned.getFields().containsKey("email") : "Email should be removed";
        System.out.println("✓ Null removal works");

        // Test 2: Deduplication
        System.out.println("\nTest 2: Deduplication");
        engine.clearDeduplicationState();
        
        Map<String, Object> fields1 = new HashMap<>();
        fields1.put("name", "Bob");
        fields1.put("age", 25);
        
        DataRecord record1 = new DataRecord("2", fields1);
        CleanedRecord cleaned1 = engine.cleanRecord(record1);
        assert !cleaned1.isDuplicate() : "First occurrence should not be duplicate";
        System.out.println("  First record: not duplicate ✓");
        
        // Same fields, different ID
        Map<String, Object> fields2 = new HashMap<>();
        fields2.put("name", "Bob");
        fields2.put("age", 25);
        
        DataRecord record2 = new DataRecord("3", fields2);
        CleanedRecord cleaned2 = engine.cleanRecord(record2);
        assert cleaned2.isDuplicate() : "Second with same fields should be duplicate";
        System.out.println("  Second record (same fields): duplicate ✓");

        // Test 3: Order-independent hashing
        System.out.println("\nTest 3: Order-independent hashing");
        engine.clearDeduplicationState();
        
        Map<String, Object> fieldsA = new LinkedHashMap<>();
        fieldsA.put("name", "Charlie");
        fieldsA.put("age", 35);
        
        DataRecord recordA = new DataRecord("4", fieldsA);
        CleanedRecord cleanedA = engine.cleanRecord(recordA);
        assert !cleanedA.isDuplicate() : "First record should not be duplicate";
        
        // Reverse order
        Map<String, Object> fieldsB = new LinkedHashMap<>();
        fieldsB.put("age", 35);
        fieldsB.put("name", "Charlie");
        
        DataRecord recordB = new DataRecord("5", fieldsB);
        CleanedRecord cleanedB = engine.cleanRecord(recordB);
        assert cleanedB.isDuplicate() : "Same content, reversed order, should be duplicate";
        System.out.println("  Order-independent hashing works ✓");

        // Test 4: Concurrent cleaning
        System.out.println("\nTest 4: Concurrent batch processing");
        engine.clearDeduplicationState();
        
        List<DataRecord> batch = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> f = new HashMap<>();
            f.put("id", "batch-" + i);
            f.put("value", i);
            if (i % 10 == 0) {
                f.put("optional", null);  // Add some nulls
            }
            batch.add(new DataRecord("rec-" + i, f));
        }
        
        long start = System.currentTimeMillis();
        List<CleanedRecord> cleaned = engine.cleanBatch(batch);
        long elapsed = System.currentTimeMillis() - start;
        
        assert cleaned.size() == 100 : "Should clean all 100 records";
        System.out.println("  Cleaned 100 records in " + elapsed + "ms");
        System.out.println("  Records processed: " + engine.getRecordsProcessed());
        System.out.println("  Records cleaned: " + engine.getRecordsCleaned());

        System.out.println("\n✓ All DataCleaningEngine tests passed!");
    }
}
```

Run tests:

```bash
javac -cp src/main/java src/main/java/com/daetl/cleaner/engine/DataCleaningEngine*.java src/main/java/com/daetl/cleaner/model/DataRecord.java
java -cp src/main/java com.daetl.cleaner.engine.DataCleaningEngineTest
```

All tests should pass. This validates the core cleaning logic.

---

## Part 3: PostgreSQL Persistence

Store cleaned records in PostgreSQL with connection pooling.

### Concept

Database persistence has several challenges:
1. **Connections are expensive**: Creating a connection takes ~100ms
2. **Solution**: Connection pool (HikariCP) reuses connections
3. **Inserts are slow**: One-by-one inserts = 1000 × 50ms = 50 seconds
4. **Solution**: Batch inserts (1000 records in one INSERT)
5. **Duplicates**: Same record twice = primary key violation
6. **Solution**: `ON CONFLICT DO UPDATE` clause

### Implementation

Create `src/main/java/com/daetl/cleaner/db/PostgresPersistence.java`:

```java
package com.daetl.cleaner.db;

import com.daetl.cleaner.engine.DataCleaningEngine.CleanedRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persist cleaned records to PostgreSQL.
 * Uses HikariCP for fast connection pooling.
 * Batches inserts for performance.
 */
public class PostgresPersistence implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(PostgresPersistence.class);
    
    private final HikariDataSource dataSource;
    private final ObjectMapper objectMapper;
    private final AtomicLong recordsStored;
    private final AtomicLong recordsDuplicate;

    public PostgresPersistence(String url, String user, String password, int poolSize) {
        this.objectMapper = new ObjectMapper();
        this.recordsStored = new AtomicLong(0);
        this.recordsDuplicate = new AtomicLong(0);
        this.dataSource = createDataSource(url, user, password, poolSize);
        
        try {
            initializeSchema();
        } catch (SQLException e) {
            logger.error("Failed to initialize schema", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Create HikariCP connection pool.
     * 
     * Why HikariCP?
     * - Fastest connection pool in Java
     * - Low overhead
     * - Built-in health checks
     * - Automatic connection recycling
     */
    private HikariDataSource createDataSource(String url, String user, String password, int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(poolSize);
        config.setMinimumIdle(2);  // Keep at least 2 connections open
        config.setConnectionTimeout(10000);  // 10s timeout
        config.setIdleTimeout(600000);  // Close after 10 min idle
        config.setMaxLifetime(1800000);  // Max 30 min per connection
        config.setAutoCommit(true);  // Auto-commit after each batch
        config.setPoolName("CleanerPool");

        HikariDataSource ds = new HikariDataSource(config);
        logger.info("HikariCP pool created with size: {}", poolSize);
        return ds;
    }

    /**
     * Create database tables if they don't exist.
     * Automatically runs on first startup.
     */
    private void initializeSchema() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // Cleaned records table
            String createCleanedTable = """
                CREATE TABLE IF NOT EXISTS cleaned_records (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    record_id VARCHAR(255) NOT NULL UNIQUE,
                    fields JSONB NOT NULL,
                    null_values_removed INTEGER DEFAULT 0,
                    is_duplicate BOOLEAN DEFAULT FALSE,
                    status VARCHAR(50) DEFAULT 'CLEANED',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """;

            // Metrics table
            String createMetricsTable = """
                CREATE TABLE IF NOT EXISTS cleaning_metrics (
                    id BIGSERIAL PRIMARY KEY,
                    batch_number BIGINT NOT NULL,
                    records_processed BIGINT NOT NULL,
                    records_cleaned BIGINT NOT NULL,
                    records_skipped BIGINT NOT NULL,
                    processing_time_ms BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """;

            stmt.execute(createCleanedTable);
            stmt.execute(createMetricsTable);
            
            // Create indexes for fast queries
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cleaned_status ON cleaned_records(status)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cleaned_created ON cleaned_records(created_at)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_metrics_batch ON cleaning_metrics(batch_number)");

            logger.info("Schema initialized successfully");
        }
    }

    /**
     * Store a batch of cleaned records.
     * Uses ON CONFLICT for automatic deduplication.
     * 
     * Why batching?
     * - Single INSERT with 1000 rows = 1 network round-trip
     * - 1000 individual INSERTs = 1000 network round-trips
     * - Batching is ~1000x faster
     */
    public void storeBatch(List<CleanedRecord> records) throws SQLException {
        // SQL with ON CONFLICT clause
        // If record_id exists, update timestamp (don't crash)
        String sql = """
            INSERT INTO cleaned_records 
                (record_id, fields, null_values_removed, is_duplicate, status)
            VALUES (?, ?::jsonb, ?, ?, ?)
            ON CONFLICT (record_id) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Disable auto-commit for this batch (faster)
            conn.setAutoCommit(false);
            
            // Add all records to batch
            for (CleanedRecord record : records) {
                try {
                    pstmt.setString(1, record.getId());
                    
                    // Convert fields map to JSON string
                    String json = objectMapper.writeValueAsString(record.getFields());
                    pstmt.setString(2, json);
                    
                    pstmt.setInt(3, record.getNullValuesRemoved());
                    pstmt.setBoolean(4, record.isDuplicate());
                    pstmt.setString(5, record.getStatus());
                    
                    pstmt.addBatch();  // Add to batch (not executed yet)

                    if (record.isDuplicate()) {
                        recordsDuplicate.incrementAndGet();
                    } else {
                        recordsStored.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.warn("Failed to prepare record for storage: {}", record.getId(), e);
                }
            }

            // Execute entire batch at once
            int[] results = pstmt.executeBatch();
            conn.commit();  // Commit transaction
            logger.debug("Batch stored: {} records in one operation", results.length);
        } catch (SQLException e) {
            logger.error("Batch storage failed", e);
            throw e;
        }
    }

    /**
     * Store metrics for monitoring/analysis.
     */
    public void storeMetrics(long batchNumber, long processed, long cleaned, long skipped, long processingTimeMs) 
            throws SQLException {
        String sql = """
            INSERT INTO cleaning_metrics 
                (batch_number, records_processed, records_cleaned, records_skipped, processing_time_ms)
            VALUES (?, ?, ?, ?, ?)
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, batchNumber);
            pstmt.setLong(2, processed);
            pstmt.setLong(3, cleaned);
            pstmt.setLong(4, skipped);
            pstmt.setLong(5, processingTimeMs);
            
            pstmt.executeUpdate();
            logger.debug("Metrics stored for batch {}", batchNumber);
        }
    }

    /**
     * Query cleaned records (for testing/monitoring).
     */
    public List<Map<String, Object>> getCleanedRecords(String status, int limit) throws SQLException {
        String sql = """
            SELECT record_id, status, null_values_removed, is_duplicate, created_at 
            FROM cleaned_records 
            WHERE status = ? 
            ORDER BY created_at DESC 
            LIMIT ?
            """;
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, status);
            pstmt.setInt(2, limit);

            try (ResultSet rs = pstmt.executeQuery()) {
                List<Map<String, Object>> records = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> record = new HashMap<>();
                    record.put("record_id", rs.getString("record_id"));
                    record.put("status", rs.getString("status"));
                    record.put("null_values_removed", rs.getInt("null_values_removed"));
                    record.put("is_duplicate", rs.getBoolean("is_duplicate"));
                    record.put("created_at", rs.getTimestamp("created_at"));
                    records.add(record);
                }
                return records;
            }
        }
    }

    /**
     * Get cleaning statistics.
     */
    public CleaningStats getStatistics() throws SQLException {
        String sql = """
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) as duplicate_count,
                SUM(null_values_removed) as total_null_removed
            FROM cleaned_records
            """;

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                long total = rs.getLong("total_records");
                long duplicates = rs.getLong("duplicate_count");
                long nullsRemoved = rs.getLong("total_null_removed");
                
                return new CleaningStats(total, duplicates, nullsRemoved, 
                    recordsStored.get(), recordsDuplicate.get());
            }
        }
        return new CleaningStats(0, 0, 0, 0, 0);
    }

    public long getRecordsStored() { 
        return recordsStored.get(); 
    }
    
    public long getRecordsDuplicate() { 
        return recordsDuplicate.get(); 
    }

    /**
     * Close database connection pool.
     * Important: Must call when shutting down.
     */
    @Override
    public void close() {
        dataSource.close();
        logger.info("Database connection pool closed");
    }

    /**
     * Statistics holder.
     */
    public static class CleaningStats {
        public final long totalRecords;
        public final long duplicateCount;
        public final long totalNullRemoved;
        public final long recordsStored;
        public final long recordsDuplicate;

        public CleaningStats(long totalRecords, long duplicateCount, long totalNullRemoved, 
                           long recordsStored, long recordsDuplicate) {
            this.totalRecords = totalRecords;
            this.duplicateCount = duplicateCount;
            this.totalNullRemoved = totalNullRemoved;
            this.recordsStored = recordsStored;
            this.recordsDuplicate = recordsDuplicate;
        }

        @Override
        public String toString() {
            return "CleaningStats{" +
                    "totalRecords=" + totalRecords +
                    ", duplicateCount=" + duplicateCount +
                    ", totalNullRemoved=" + totalNullRemoved +
                    ", recordsStored=" + recordsStored +
                    ", recordsDuplicate=" + recordsDuplicate +
                    '}';
        }
    }
}
```

**Key points:**
- HikariCP pool reuses connections (no create/destroy overhead)
- Batch insert: 1000 records in one SQL execution
- `ON CONFLICT`: Handle duplicate record_ids gracefully
- JSONB: Store flexible schema (no table migrations needed)
- Indexes: Speed up queries by status and timestamp

### Setup PostgreSQL

You need a running PostgreSQL database. Two options:

**Option 1: Docker (easiest)**

```bash
docker run -d \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=data_cleaner \
  -p 5432:5432 \
  --name postgres-cleaner \
  postgres:15
```

Wait a few seconds for it to start:

```bash
docker exec postgres-cleaner psql -U postgres -d data_cleaner -c "SELECT 1;" 
# Should return: (1 row)
```

**Option 2: Local installation**

```bash
# On Ubuntu/Debian
sudo apt-get install postgresql
sudo systemctl start postgresql

# Create database
createdb data_cleaner
psql data_cleaner -c "SELECT 1;"
```

### Testing

Create `src/main/java/com/daetl/cleaner/db/PostgresPersistenceTest.java`:

```java
package com.daetl.cleaner.db;

import com.daetl.cleaner.engine.DataCleaningEngine.CleanedRecord;
import java.util.*;

public class PostgresPersistenceTest {
    public static void main(String[] args) throws Exception {
        // Connect to PostgreSQL
        PostgresPersistence db = new PostgresPersistence(
            "jdbc:postgresql://localhost:5432/data_cleaner",
            "postgres",
            "postgres",
            10  // pool size
        );

        try {
            System.out.println("Test 1: Store cleaned records");
            
            List<CleanedRecord> batch = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                CleanedRecord record = new CleanedRecord(
                    "test-record-" + i,
                    Map.of("name", "Test" + i, "value", i)
                );
                record.setStatus("CLEANED");
                batch.add(record);
            }
            
            db.storeBatch(batch);
            System.out.println("✓ Stored 10 records");

            System.out.println("\nTest 2: Query statistics");
            PostgresPersistence.CleaningStats stats = db.getStatistics();
            System.out.println("  Total records: " + stats.totalRecords);
            System.out.println("  Records stored: " + stats.recordsStored);
            System.out.println("✓ Statistics retrieved");

            System.out.println("\nTest 3: Query cleaned records");
            List<Map<String, Object>> records = db.getCleanedRecords("CLEANED", 5);
            System.out.println("  Found: " + records.size() + " cleaned records");
            for (Map<String, Object> rec : records) {
                System.out.println("    - " + rec.get("record_id"));
            }
            System.out.println("✓ Queries work");

            System.out.println("\n✓ All PostgresPersistence tests passed!");
        } finally {
            db.close();
        }
    }
}
```

Run test:

```bash
mvn clean compile

javac -cp ".:target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
  src/main/java/com/daetl/cleaner/db/PostgresPersistenceTest.java \
  src/main/java/com/daetl/cleaner/engine/DataCleaningEngine.java \
  src/main/java/com/daetl/cleaner/model/DataRecord.java

java -cp ".:target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
  com.daetl.cleaner.db.PostgresPersistenceTest
```

Should see records stored and queried successfully.

---

## Part 4: Kafka Consumer

Stream data from Kafka with TLS support.

### Concept

Kafka is a distributed message queue. The consumer:
1. Connects to Kafka broker (with optional TLS)
2. Reads from a topic in batches
3. Queues records for processing
4. Handles errors gracefully

### Implementation

Create `src/main/java/com/daetl/cleaner/kafka/SecureKafkaConsumer.java`:

```java
package com.daetl.cleaner.kafka;

import com.daetl.cleaner.model.DataRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consume messages from Kafka and queue them for processing.
 * Supports TLS/SSL for secure connections.
 */
public class SecureKafkaConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SecureKafkaConsumer.class);
    
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running;

    public SecureKafkaConsumer(String brokers, String topic, String consumerGroup, 
                              boolean tlsEnabled, String keystorePath, String keystorePassword) 
            throws IOException {
        this.topic = topic;
        this.objectMapper = new ObjectMapper();
        this.running = new AtomicBoolean(false);
        this.consumer = createSecureConsumer(brokers, consumerGroup, tlsEnabled, keystorePath, keystorePassword);
    }

    /**
     * Create Kafka consumer with optional TLS.
     * 
     * TLS configuration:
     * - security.protocol = SSL
     * - ssl.keystore.location = path to .jks file
     * - ssl.keystore.password = keystore password
     * 
     * Without TLS, these are omitted (PLAINTEXT protocol).
     */
    private KafkaConsumer<String, String> createSecureConsumer(
            String brokers, String consumerGroup, 
            boolean tlsEnabled, String keystorePath, String keystorePassword) {
        
        Properties props = new Properties();
        
        // Basic Kafka config
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);  // Batch size
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        // TLS/SSL Configuration (optional)
        if (tlsEnabled) {
            props.put("security.protocol", "SSL");
            props.put("ssl.keystore.location", keystorePath);
            props.put("ssl.keystore.password", keystorePassword);
            props.put("ssl.key.password", keystorePassword);
            props.put("ssl.truststore.location", keystorePath);
            props.put("ssl.truststore.password", keystorePassword);
            logger.info("TLS enabled with keystore: {}", keystorePath);
        }

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        logger.info("Kafka consumer initialized (TLS: {})", tlsEnabled);
        
        return kafkaConsumer;
    }

    /**
     * Stream records from Kafka into a blocking queue.
     * Runs in separate thread; doesn't block main application.
     * 
     * Why blocking queue?
     * - Backpressure: If processing is slow, queue fills up
     * - Kafka consumer blocks on poll() when queue is full
     * - Prevents unbounded memory growth
     */
    public void streamToQueue(BlockingQueue<List<DataRecord>> queue, int batchSize) {
        running.set(true);
        consumer.subscribe(Collections.singletonList(topic));
        
        Thread consumerThread = new Thread(() -> {
            try {
                List<DataRecord> batch = new ArrayList<>(batchSize);
                
                while (running.get()) {
                    // Poll Kafka with 1-second timeout
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            DataRecord dataRecord = parseRecord(record.value());
                            batch.add(dataRecord);
                            
                            // When batch is full, queue it for processing
                            if (batch.size() >= batchSize) {
                                queue.put(new ArrayList<>(batch));  // Queue (blocking if full)
                                batch.clear();
                                logger.debug("Queued batch of {} records", batchSize);
                            }
                        } catch (Exception e) {
                            logger.warn("Failed to parse Kafka record: {}", record.value(), e);
                        }
                    }
                    
                    // If app is shutting down and queue has partial batch
                    if (!running.get() && !batch.isEmpty()) {
                        queue.put(batch);
                        batch = new ArrayList<>();
                    }
                }
            } catch (InterruptedException e) {
                logger.info("Kafka consumer thread interrupted");
                Thread.currentThread().interrupt();
            } finally {
                logger.info("Kafka consumer streaming stopped");
            }
        });
        
        consumerThread.setName("KafkaConsumer-Thread");
        consumerThread.setDaemon(false);
        consumerThread.start();
    }

    /**
     * Parse JSON record from Kafka message.
     * Expects: {"id": "...", "field1": "value1", ...}
     */
    @SuppressWarnings("unchecked")
    private DataRecord parseRecord(String jsonString) throws Exception {
        Map<String, Object> data = objectMapper.readValue(jsonString, Map.class);
        
        // Extract ID (or generate UUID)
        String id = (String) data.getOrDefault("id", UUID.randomUUID().toString());
        data.remove("id");  // Don't store ID twice
        
        DataRecord record = new DataRecord(id, data);
        record.setSource("kafka");
        return record;
    }

    /**
     * Stop consuming (called on shutdown).
     */
    public void stopConsuming() {
        running.set(false);
    }

    @Override
    public void close() {
        running.set(false);
        consumer.close();
        logger.info("Kafka consumer closed");
    }
}
```

**Key points:**
- TLS config only added if enabled (PLAINTEXT protocol otherwise)
- Blocking queue backpressure: if processing slow, queue fills, Kafka poll blocks
- Separate thread: doesn't freeze main app
- Batch accumulation: collect 1000 records before queueing

---

## Part 5: Load Balancer

Distribute work across multiple worker threads using different strategies.

### Concept

With 8 worker threads, how do you decide which worker gets the next batch?

Strategies:
1. **ROUND_ROBIN**: Rotate in order (worker 1, 2, 3, ..., 8, 1, 2, ...)
2. **LEAST_CONNECTIONS**: Pick worker with fewest active tasks
3. **WEIGHTED**: Probabilistic based on worker weight

LEAST_CONNECTIONS is best: it naturally adapts to uneven processing times.

### Implementation

Create `src/main/java/com/daetl/cleaner/lb/LoadBalancer.java`:

```java
package com.daetl.cleaner.lb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Distribute cleaning work across multiple workers.
 * Supports round-robin, least-connections, and weighted strategies.
 */
public class LoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);
    
    private final List<WorkerNode> workers;
    private final AtomicLong requestCounter;
    private final LoadBalancingStrategy strategy;
    private final ReentrantReadWriteLock lock;

    public enum LoadBalancingStrategy {
        ROUND_ROBIN,
        LEAST_CONNECTIONS,
        WEIGHTED
    }

    public LoadBalancer(LoadBalancingStrategy strategy) {
        this.workers = Collections.synchronizedList(new ArrayList<>());
        this.requestCounter = new AtomicLong(0);
        this.strategy = strategy;
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Register a worker thread.
     */
    public void registerWorker(String workerId, int weight) {
        lock.writeLock().lock();
        try {
            workers.add(new WorkerNode(workerId, weight));
            logger.info("Worker registered: {} with weight: {}", workerId, weight);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Select a worker based on strategy.
     */
    public WorkerNode selectWorker() {
        lock.readLock().lock();
        try {
            if (workers.isEmpty()) {
                throw new IllegalStateException("No workers available");
            }

            switch (strategy) {
                case ROUND_ROBIN:
                    return selectRoundRobin();
                case LEAST_CONNECTIONS:
                    return selectLeastConnections();
                case WEIGHTED:
                    return selectWeighted();
                default:
                    return selectRoundRobin();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Round-robin: cycle through workers in order.
     * Simple but doesn't adapt to processing time differences.
     */
    private WorkerNode selectRoundRobin() {
        int index = (int) (requestCounter.getAndIncrement() % workers.size());
        return workers.get(index);
    }

    /**
     * Least connections: pick worker with fewest active tasks.
     * Best for uneven processing times: adapts automatically.
     */
    private WorkerNode selectLeastConnections() {
        return workers.stream()
            .min(Comparator.comparingLong(WorkerNode::getActiveConnections))
            .orElse(workers.get(0));
    }

    /**
     * Weighted: probabilistic selection based on weight.
     * Useful if you know some workers are faster.
     */
    private WorkerNode selectWeighted() {
        long totalWeight = workers.stream().mapToLong(w -> w.weight).sum();
        double random = Math.random() * totalWeight;
        
        double current = 0;
        for (WorkerNode worker : workers) {
            current += worker.weight;
            if (random <= current) {
                return worker;
            }
        }
        return workers.get(0);
    }

    /**
     * Get load balancer status (for monitoring).
     */
    public Map<String, Object> getStatus() {
        lock.readLock().lock();
        try {
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("total_workers", workers.size());
            status.put("healthy_workers", workers.stream().filter(w -> w.isHealthy).count());
            
            List<Map<String, Object>> workerStats = new ArrayList<>();
            for (WorkerNode worker : workers) {
                Map<String, Object> ws = new LinkedHashMap<>();
                ws.put("id", worker.workerId);
                ws.put("healthy", worker.isHealthy);
                ws.put("active_connections", worker.getActiveConnections());
                ws.put("total_processed", worker.totalProcessed.get());
                workerStats.add(ws);
            }
            status.put("workers", workerStats);
            return status;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Represents one worker thread.
     */
    public static class WorkerNode {
        public final String workerId;
        public final long weight;
        public boolean isHealthy;
        public final AtomicLong activeConnections;
        public final AtomicLong totalProcessed;

        public WorkerNode(String workerId, int weight) {
            this.workerId = workerId;
            this.weight = weight;
            this.isHealthy = true;
            this.activeConnections = new AtomicLong(0);
            this.totalProcessed = new AtomicLong(0);
        }

        public void incrementConnections() {
            activeConnections.incrementAndGet();
        }

        public void decrementConnections() {
            activeConnections.decrementAndGet();
        }

        public long getActiveConnections() {
            return activeConnections.get();
        }

        public void recordProcessed(long count) {
            totalProcessed.addAndGet(count);
        }

        @Override
        public String toString() {
            return "WorkerNode{" +
                    "workerId='" + workerId + '\'' +
                    ", isHealthy=" + isHealthy +
                    ", activeConnections=" + activeConnections +
                    '}';
        }
    }
}
```

---

## Part 6: Main Orchestrator

Tie everything together: Kafka → Cleaning → Database.

### Concept

The main app:
1. Creates Kafka consumer (starts polling in background thread)
2. Creates worker pool
3. Workers pull from queue
4. Each worker cleans records and stores to database
5. Tracks metrics
6. Graceful shutdown

Create `src/main/java/com/daetl/cleaner/DataCleanerApp.java`:

```java
package com.daetl.cleaner;

import com.daetl.cleaner.db.PostgresPersistence;
import com.daetl.cleaner.engine.DataCleaningEngine;
import com.daetl.cleaner.engine.DataCleaningEngine.CleanedRecord;
import com.daetl.cleaner.kafka.SecureKafkaConsumer;
import com.daetl.cleaner.lb.LoadBalancer;
import com.daetl.cleaner.model.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main orchestrator: Kafka → Cleaning → PostgreSQL
 */
public class DataCleanerApp {
    private static final Logger logger = LoggerFactory.getLogger(DataCleanerApp.class);
    
    private final SecureKafkaConsumer kafkaConsumer;
    private final DataCleaningEngine cleaningEngine;
    private final PostgresPersistence persistence;
    private final LoadBalancer loadBalancer;
    private final AtomicBoolean running;
    private final AtomicLong batchCounter;
    private final ExecutorService executorService;
    private final BlockingQueue<List<DataRecord>> processingQueue;
    private final ScheduledExecutorService monitoringService;
    private final int concurrentWorkers;

    public DataCleanerApp(String brokers, String topic, String consumerGroup,
                         String dbUrl, String dbUser, String dbPassword,
                         int concurrentWorkers, boolean tlsEnabled, 
                         String keystorePath, String keystorePassword) throws Exception {
        
        this.concurrentWorkers = concurrentWorkers;
        this.kafkaConsumer = new SecureKafkaConsumer(brokers, topic, consumerGroup, 
                                                      tlsEnabled, keystorePath, keystorePassword);
        this.cleaningEngine = new DataCleaningEngine(concurrentWorkers);
        this.persistence = new PostgresPersistence(dbUrl, dbUser, dbPassword, 20);
        this.loadBalancer = new LoadBalancer(LoadBalancer.LoadBalancingStrategy.LEAST_CONNECTIONS);
        this.running = new AtomicBoolean(false);
        this.batchCounter = new AtomicLong(0);
        this.processingQueue = new LinkedBlockingQueue<>(100);
        this.executorService = Executors.newFixedThreadPool(concurrentWorkers);
        this.monitoringService = Executors.newScheduledThreadPool(2);

        // Register workers with load balancer
        for (int i = 0; i < concurrentWorkers; i++) {
            loadBalancer.registerWorker("worker-" + i, 1);
        }

        logger.info("DataCleanerApp initialized with {} workers", concurrentWorkers);
    }

    /**
     * Start the pipeline.
     */
    public void start() throws Exception {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("App already running");
        }

        logger.info("Starting Data Cleaner Application");

        // Start Kafka consumer (polling in background)
        kafkaConsumer.streamToQueue(processingQueue, 1000);

        // Start processing workers
        startProcessingWorkers();

        // Start monitoring
        startMonitoring();

        logger.info("Data Cleaner Application started successfully");
    }

    /**
     * Create worker threads that process batches from queue.
     */
    private void startProcessingWorkers() {
        for (int i = 0; i < concurrentWorkers; i++) {
            executorService.submit(() -> {
                while (running.get()) {
                    try {
                        // Wait for a batch from queue (timeout in case app shutting down)
                        List<DataRecord> batch = processingQueue.poll(5, TimeUnit.SECONDS);
                        if (batch != null) {
                            processBatch(batch);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.debug("Worker interrupted");
                    } catch (Exception e) {
                        logger.error("Error in worker", e);
                    }
                }
            });
        }
        logger.info("Started {} processing workers", concurrentWorkers);
    }

    /**
     * Process a single batch: clean + store + track metrics.
     */
    private void processBatch(List<DataRecord> batch) {
        long batchStart = System.currentTimeMillis();
        long batchNum = batchCounter.incrementAndGet();
        
        LoadBalancer.WorkerNode worker = loadBalancer.selectWorker();
        worker.incrementConnections();
        
        try {
            logger.info("Processing batch {} with {} records", batchNum, batch.size());

            // Clean batch concurrently within the cleaning engine
            List<CleanedRecord> cleanedRecords = cleaningEngine.cleanBatch(batch);

            // Store to database
            persistence.storeBatch(cleanedRecords);
            worker.recordProcessed(cleanedRecords.size());

            // Track metrics
            long processingTime = System.currentTimeMillis() - batchStart;
            persistence.storeMetrics(
                batchNum,
                cleaningEngine.getRecordsProcessed(),
                cleaningEngine.getRecordsCleaned(),
                cleaningEngine.getRecordsSkipped(),
                processingTime
            );

            logger.info("Batch {} completed in {}ms (cleaned: {}, skipped: {})",
                batchNum,
                processingTime,
                cleanedRecords.stream().filter(r -> !r.isDuplicate()).count(),
                cleanedRecords.stream().filter(CleanedRecord::isDuplicate).count()
            );
        } catch (Exception e) {
            logger.error("Error processing batch {}", batchNum, e);
        } finally {
            worker.decrementConnections();
        }
    }

    /**
     * Start periodic monitoring: health checks, metrics.
     */
    private void startMonitoring() {
        // Health checks every 30 seconds
        monitoringService.scheduleAtFixedRate(() -> {
            try {
                PostgresPersistence.CleaningStats stats = persistence.getStatistics();
                logger.info("Cleaner status - Records: {}, Duplicates: {}, Nulls removed: {}",
                    stats.totalRecords, stats.duplicateCount, stats.totalNullRemoved);
                logger.info("Load balancer: {}", loadBalancer.getStatus());
            } catch (Exception e) {
                logger.warn("Monitoring error", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Stop gracefully.
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        logger.info("Stopping Data Cleaner Application");

        try {
            // Signal Kafka consumer to stop
            kafkaConsumer.stopConsuming();
            
            // Wait for workers to finish current batch
            logger.info("Waiting for workers to finish...");
            executorService.shutdown();
            if (!executorService.awaitTermination(2, TimeUnit.MINUTES)) {
                logger.warn("Executor did not terminate gracefully");
                executorService.shutdownNow();
            }

            monitoringService.shutdown();
            if (!monitoringService.awaitTermination(10, TimeUnit.SECONDS)) {
                monitoringService.shutdownNow();
            }

            kafkaConsumer.close();
            persistence.close();

            logger.info("Data Cleaner Application stopped");
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }

    /**
     * Get current status (for dashboards/monitoring).
     */
    public Map<String, Object> getStatus() throws Exception {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("running", running.get());
        status.put("batches_processed", batchCounter.get());
        status.put("cleaning_engine", Map.of(
            "records_processed", cleaningEngine.getRecordsProcessed(),
            "records_cleaned", cleaningEngine.getRecordsCleaned(),
            "records_skipped", cleaningEngine.getRecordsSkipped()
        ));
        status.put("database", persistence.getStatistics());
        status.put("load_balancer", loadBalancer.getStatus());
        status.put("queue_size", processingQueue.size());
        return status;
    }

    public static void main(String[] args) throws Exception {
        // Default config (override with args or environment variables)
        String brokers = "localhost:9092";
        String topic = "raw_data";
        String consumerGroup = "data_cleaner";
        String dbUrl = "jdbc:postgresql://localhost:5432/data_cleaner";
        String dbUser = "postgres";
        String dbPassword = "postgres";
        int workers = 8;
        boolean tlsEnabled = false;

        try {
            DataCleanerApp app = new DataCleanerApp(
                brokers, topic, consumerGroup,
                dbUrl, dbUser, dbPassword,
                workers, tlsEnabled, "", ""
            );

            app.start();

            // Graceful shutdown on Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received");
                app.stop();
            }));

            // Keep application running
            Thread.currentThread().join();
        } catch (Exception e) {
            logger.error("Failed to start application", e);
            System.exit(1);
        }
    }
}
```

**Key flow:**
1. Kafka consumer polls in background thread, queues batches
2. Workers pull from queue
3. Each worker cleans batch concurrently (via cleaningEngine)
4. Results stored to PostgreSQL
5. Metrics tracked for monitoring
6. Graceful shutdown waits for all workers to finish

---

## Part 7: Configuration System

Instead of hardcoding values, use a configuration file.

### Why YAML?

- Human-readable
- Hierarchical structure
- Comments supported
- Jackson can parse it directly

Create `config.yaml` in project root:

```yaml
kafka:
  brokers: "localhost:9092"
  topic: "raw_data"
  consumer_group: "data_cleaner_group"
  batch_size: 1000

database:
  url: "jdbc:postgresql://localhost:5432/data_cleaner"
  user: "postgres"
  password: "postgres"
  pool_size: 20

cleaning:
  concurrent_workers: 8
  batch_size: 1000
  null_value_deletion: true
  repeated_value_deletion: true

tls:
  enabled: false
  keystore_path: "/path/to/keystore.jks"
  keystore_password: "password"
```

Create `src/main/java/com/daetl/cleaner/config/CleanerConfig.java`:

```java
package com.daetl.cleaner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Configuration from config.yaml.
 * Uses Jackson for YAML parsing.
 */
public class CleanerConfig {
    private Map<String, Object> kafka;
    private Map<String, Object> database;
    private Map<String, Object> cleaning;
    private Map<String, Object> tls;

    /**
     * Load configuration from YAML file.
     */
    public static CleanerConfig loadFromYaml(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(filePath), CleanerConfig.class);
    }

    // Getters
    public Map<String, Object> getKafka() { return kafka; }
    public Map<String, Object> getDatabase() { return database; }
    public Map<String, Object> getCleaning() { return cleaning; }
    public Map<String, Object> getTls() { return tls; }

    // Convenience methods for common config values
    public String getKafkaBrokers() {
        return (String) kafka.getOrDefault("brokers", "localhost:9092");
    }

    public String getKafkaTopic() {
        return (String) kafka.getOrDefault("topic", "raw_data");
    }

    public String getConsumerGroup() {
        return (String) kafka.getOrDefault("consumer_group", "cleaner_group");
    }

    public int getConcurrentWorkers() {
        return (Integer) cleaning.getOrDefault("concurrent_workers", 8);
    }

    public String getDatabaseUrl() {
        return (String) database.getOrDefault("url", "jdbc:postgresql://localhost:5432/data_cleaner");
    }

    public String getDatabaseUser() {
        return (String) database.getOrDefault("user", "postgres");
    }

    public String getDatabasePassword() {
        return (String) database.getOrDefault("password", "postgres");
    }

    public boolean isTlsEnabled() {
        return (Boolean) tls.getOrDefault("enabled", false);
    }

    public String getTlsKeyStorePath() {
        return (String) tls.getOrDefault("keystore_path", "");
    }

    public String getTlsKeyStorePassword() {
        return (String) tls.getOrDefault("keystore_password", "");
    }

    @Override
    public String toString() {
        return "CleanerConfig{\n" +
                "kafka=" + kafka + "\n" +
                "database=" + database + "\n" +
                "cleaning=" + cleaning + "\n" +
                "tls=" + tls + "\n" +
                "}";
    }

    // Setters (for Jackson deserialization)
    public void setKafka(Map<String, Object> kafka) { this.kafka = kafka; }
    public void setDatabase(Map<String, Object> database) { this.database = database; }
    public void setCleaning(Map<String, Object> cleaning) { this.cleaning = cleaning; }
    public void setTls(Map<String, Object> tls) { this.tls = tls; }
}
```

Update main app to use config:

```java
public static void main(String[] args) throws Exception {
    String configPath = args.length > 0 ? args[0] : "config.yaml";
    CleanerConfig config = CleanerConfig.loadFromYaml(configPath);

    logger.info("Loaded configuration: {}", config);

    DataCleanerApp app = new DataCleanerApp(
        config.getKafkaBrokers(),
        config.getKafkaTopic(),
        config.getConsumerGroup(),
        config.getDatabaseUrl(),
        config.getDatabaseUser(),
        config.getDatabasePassword(),
        config.getConcurrentWorkers(),
        config.isTlsEnabled(),
        config.getTlsKeyStorePath(),
        config.getTlsKeyStorePassword()
    );
    // ... rest of main
}
```

Now you can change behavior without recompiling: just edit `config.yaml`.

---

## Part 8: JavaFX Dashboard

Build a real-time GUI to monitor the system.

### Concept

Display:
- Status (RUNNING/STOPPED)
- Metrics (records processed/cleaned/skipped)
- Worker status
- Event log
- Start/Stop controls

Create `src/main/java/com/daetl/cleaner/gui/CleanerDashboard.java`:

This is long. I'll show the key parts:

```java
package com.daetl.cleaner.gui;

import com.daetl.cleaner.config.CleanerConfig;
import com.daetl.cleaner.DataCleanerApp;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Real-time monitoring dashboard for data cleaner.
 * Shows metrics, worker status, and event log.
 */
public class CleanerDashboard extends Application {
    private static final Logger logger = LoggerFactory.getLogger(CleanerDashboard.class);
    
    private DataCleanerApp app;
    private Label statusLabel;
    private Label recordsProcessedLabel;
    private Label recordsCleanedLabel;
    private TextArea logArea;
    private Timer updateTimer;

    @Override
    public void start(Stage primaryStage) throws Exception {
        primaryStage.setTitle("Data Cleaner Dashboard");
        primaryStage.setWidth(1000);
        primaryStage.setHeight(700);

        BorderPane root = new BorderPane();
        root.setPadding(new Insets(10));

        // Header with status and controls
        root.setTop(createHeader());

        // Main content: stats and log
        VBox mainContent = new VBox(10);
        mainContent.getChildren().addAll(createStatsPanel(), createLogPanel());
        mainContent.setVgrow(createLogPanel(), Priority.ALWAYS);
        root.setCenter(mainContent);

        Scene scene = new Scene(root);
        primaryStage.setScene(scene);
        primaryStage.show();

        // Initialize and start app
        initializeApp();
        startUpdates();

        primaryStage.setOnCloseRequest(event -> shutdown());
    }

    private HBox createHeader() {
        HBox header = new HBox(15);
        header.setPadding(new Insets(10));
        header.setStyle("-fx-border-color: #cccccc; -fx-border-width: 0 0 1 0;");

        Label title = new Label("Data Cleaner Monitor");
        title.setStyle("-fx-font-size: 18; -fx-font-weight: bold;");

        statusLabel = new Label("STATUS: INITIALIZING");
        statusLabel.setStyle("-fx-font-size: 14; -fx-text-fill: #ff9800;");

        HBox spacer = new HBox();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        Button stopBtn = new Button("Stop");
        stopBtn.setOnAction(e -> {
            if (app != null) app.stop();
            updateStatus("STOPPED");
        });

        header.getChildren().addAll(title, spacer, statusLabel, stopBtn);
        return header;
    }

    private VBox createStatsPanel() {
        VBox panel = new VBox(10);
        panel.setPadding(new Insets(10));
        panel.setStyle("-fx-border-color: #dddddd; -fx-border-width: 1;");

        recordsProcessedLabel = new Label("Records Processed: 0");
        recordsProcessedLabel.setStyle("-fx-font-size: 14;");

        recordsCleanedLabel = new Label("Records Cleaned: 0");
        recordsCleanedLabel.setStyle("-fx-font-size: 14;");

        Label recordsSkipped = new Label("Records Skipped: 0");
        recordsSkipped.setStyle("-fx-font-size: 14;");

        panel.getChildren().addAll(recordsProcessedLabel, recordsCleanedLabel, recordsSkipped);
        return panel;
    }

    private VBox createLogPanel() {
        VBox panel = new VBox(5);
        panel.setPadding(new Insets(10));

        Label title = new Label("Event Log");
        title.setStyle("-fx-font-size: 14; -fx-font-weight: bold;");

        logArea = new TextArea();
        logArea.setEditable(false);
        logArea.setWrapText(true);
        logArea.setPrefRowCount(15);
        logArea.setStyle("-fx-font-family: 'Courier New'; -fx-font-size: 11;");

        panel.getChildren().addAll(title, logArea);
        VBox.setVgrow(logArea, Priority.ALWAYS);
        return panel;
    }

    private void initializeApp() {
        try {
            CleanerConfig config = CleanerConfig.loadFromYaml("config.yaml");
            app = new DataCleanerApp(
                config.getKafkaBrokers(),
                config.getKafkaTopic(),
                config.getConsumerGroup(),
                config.getDatabaseUrl(),
                config.getDatabaseUser(),
                config.getDatabasePassword(),
                config.getConcurrentWorkers(),
                config.isTlsEnabled(),
                config.getTlsKeyStorePath(),
                config.getTlsKeyStorePassword()
            );
            app.start();
            updateStatus("RUNNING");
            appendLog("Application started successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize app", e);
            updateStatus("ERROR");
            appendLog("Error: " + e.getMessage());
        }
    }

    private void startUpdates() {
        updateTimer = new Timer();
        updateTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Platform.runLater(() -> {
                    if (app != null) {
                        try {
                            Map<String, Object> status = app.getStatus();
                            updateMetrics(status);
                        } catch (Exception e) {
                            logger.warn("Error updating dashboard", e);
                        }
                    }
                });
            }
        }, 0, 2000);  // Update every 2 seconds
    }

    @SuppressWarnings("unchecked")
    private void updateMetrics(Map<String, Object> status) {
        boolean running = (boolean) status.get("running");
        updateStatus(running ? "RUNNING" : "STOPPED");

        Map<String, Object> engine = (Map<String, Object>) status.get("cleaning_engine");
        long processed = (long) engine.get("records_processed");
        long cleaned = (long) engine.get("records_cleaned");
        long skipped = (long) engine.get("records_skipped");

        recordsProcessedLabel.setText("Records Processed: " + processed);
        recordsCleanedLabel.setText("Records Cleaned: " + cleaned);
    }

    private void updateStatus(String status) {
        String color = switch (status) {
            case "RUNNING" -> "#4caf50";
            case "STOPPED" -> "#f44336";
            default -> "#ff9800";
        };
        statusLabel.setText("STATUS: " + status);
        statusLabel.setStyle("-fx-font-size: 14; -fx-text-fill: " + color + ";");
    }

    private void appendLog(String message) {
        String timestamp = java.time.LocalDateTime.now()
            .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        logArea.appendText("[" + timestamp + "] " + message + "\n");
    }

    private void shutdown() {
        if (updateTimer != null) updateTimer.cancel();
        if (app != null) app.stop();
        Platform.exit();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
```

Run the dashboard:

```bash
java -cp target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout) \
  --add-modules javafx.controls,javafx.fxml \
  com.daetl.cleaner.gui.CleanerDashboard
```

You'll see a live dashboard showing processing metrics.

---

## Part 9: Testing & Validation

### Test Data Generator

Create `src/main/java/com/daetl/cleaner/test/SampleDataProducer.java`:

```java
package com.daetl.cleaner.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Generate sample test data for Kafka.
 * Creates records with some nulls and duplicates.
 */
public class SampleDataProducer {
    private static final Logger logger = LoggerFactory.getLogger(SampleDataProducer.class);
    
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public SampleDataProducer(String bootstrapServers) {
        this.objectMapper = new ObjectMapper();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Generate test data with some nulls and duplicates.
     */
    public void generateTestData(String topic, int recordCount) throws InterruptedException {
        logger.info("Generating {} test records", recordCount);
        
        // Create one duplicate record to repeat
        Map<String, Object> duplicateRecord = new LinkedHashMap<>();
        duplicateRecord.put("id", "dup-1");
        duplicateRecord.put("name", "John Duplicate");
        duplicateRecord.put("email", "john@example.com");
        duplicateRecord.put("age", 30);

        for (int i = 0; i < recordCount; i++) {
            Map<String, Object> record;
            
            // 10% of records are exact duplicates
            if (i % 10 == 0) {
                record = new LinkedHashMap<>(duplicateRecord);
                record.put("id", "dup-" + (i / 10));
            } else {
                // Generate unique record
                record = new LinkedHashMap<>();
                record.put("id", "rec-" + i);
                record.put("name", randomName());
                record.put("email", Math.random() > 0.1 ? randomEmail() : null);  // 10% null
                record.put("age", Math.random() > 0.05 ? new Random().nextInt(80) + 18 : null);  // 5% null
                record.put("city", Math.random() > 0.2 ? randomCity() : "");  // 20% empty
            }

            try {
                String json = objectMapper.writeValueAsString(record);
                ProducerRecord<String, String> kafkaRecord = 
                    new ProducerRecord<>(topic, record.get("id").toString(), json);
                
                producer.send(kafkaRecord);
                
                if (i % 1000 == 0) {
                    logger.info("Sent {} records", i);
                }
            } catch (Exception e) {
                logger.error("Error sending record {}", i, e);
            }
        }

        producer.flush();
        logger.info("Finished sending {} records", recordCount);
    }

    private String randomName() {
        String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve"};
        return names[new Random().nextInt(names.length)];
    }

    private String randomEmail() {
        return randomName().toLowerCase() + "@example.com";
    }

    private String randomCity() {
        String[] cities = {"NYC", "LA", "Chicago", "Houston", "Phoenix"};
        return cities[new Random().nextInt(cities.length)];
    }

    public static void main(String[] args) throws Exception {
        String brokers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "raw_data";
        int recordCount = args.length > 2 ? Integer.parseInt(args[2]) : 1000;

        SampleDataProducer producer = new SampleDataProducer(brokers);
        producer.generateTestData(topic, recordCount);
    }
}
```

### End-to-End Test

```bash
# 1. Make sure Kafka and PostgreSQL are running

# 2. Build project
mvn clean package

# 3. Start the app
java -cp target/data-cleaner-1.0.0.jar com.daetl.cleaner.DataCleanerApp config.yaml &

# 4. In another terminal, generate test data
java -cp target/data-cleaner-1.0.0.jar com.daetl.cleaner.test.SampleDataProducer localhost:9092 raw_data 5000

# 5. Query results
psql -U postgres -d data_cleaner -c \
  "SELECT COUNT(*) as total, 
          SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) as duplicates,
          SUM(null_values_removed) as nulls_removed
   FROM cleaned_records;"
```

Expected output:
```
 total | duplicates | nulls_removed
-------+------------+---------------
  4500 |        500 |          ~1000
```

The 500 duplicates (10% of 5000) were removed. Some nulls were deleted from each record.

---

## Part 10: Docker & Deployment

### Dockerfile

Create `Dockerfile` at project root:

```dockerfile
# Build stage
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build
COPY pom.xml .
COPY src/ src/

RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy built JAR from builder
COPY --from=builder /build/target/data-cleaner-1.0.0.jar .

# Copy config template
COPY config.yaml .

# Health check
HEALTHCHECK --interval=30s CMD java -version

EXPOSE 8080

# Default command
CMD ["java", "-Xms512m", "-Xmx2g", "-jar", "data-cleaner-1.0.0.jar", "config.yaml"]
```

### Docker Compose

Create `docker-compose.yml` at project root:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: data_cleaner
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data

  data-cleaner:
    build: .
    depends_on:
      - kafka
      - postgres
    environment:
      KAFKA_BROKERS: kafka:29092
      DB_URL: jdbc:postgresql://postgres:5432/data_cleaner
    volumes:
      - ./config.yaml:/app/config.yaml

volumes:
  postgres-data:
```

### Build and Run

```bash
# Build image
docker-compose build

# Start all services
docker-compose up -d

# Generate test data
docker exec data-cleaner java -cp data-cleaner-1.0.0.jar \
  com.daetl.cleaner.test.SampleDataProducer kafka:29092 raw_data 5000

# Check results
docker exec postgres psql -U postgres -d data_cleaner -c \
  "SELECT COUNT(*) FROM cleaned_records;"

# View logs
docker-compose logs -f data-cleaner
```

---

## Summary

You now have a complete, production-grade data cleaning pipeline:

### Architecture
- Concurrent Kafka consumer (TLS-enabled)
- Thread-safe cleaning engine (null removal + deduplication)
- PostgreSQL persistence (HikariCP pooling, batch inserts)
- Load balancer (distributes work across 8 workers)
- Configuration system (YAML-based)
- Real-time GUI dashboard
- Comprehensive logging

### Performance
- **Throughput**: 5k-10k records/sec per machine
- **Latency**: 100-3000ms end-to-end
- **Scalability**: Horizontal (Kafka consumer groups), vertical (thread pool)

### Key Takeaways

1. **Concurrency**: ReentrantReadWriteLock for read-heavy dedup checks
2. **Backpressure**: BlockingQueue prevents unbounded memory growth
3. **Batching**: 1000 records per INSERT beats 1000 individual INSERTs
4. **Connection pooling**: HikariCP reuses connections (no create/destroy overhead)
5. **Graceful shutdown**: Proper resource cleanup on exit
6. **Monitoring**: Real-time metrics and dashboards for observability

This is a real system you could put into production. Every component serves a purpose; nothing is boilerplate.

---

## Next Steps

1. **Build it**: `mvn clean package`
2. **Run it locally**: `docker-compose up -d`
3. **Generate test data**: Send 5000 records with nulls and duplicates
4. **Query results**: See cleaned records in PostgreSQL
5. **Monitor**: Watch the GUI dashboard update in real-time
6. **Customize**: Add your own Kafka source, database sink, or cleaning rules

Good luck! 🚀

---

## Part 11: Troubleshooting Common Issues

### Problem: "Connection refused" (Kafka or PostgreSQL)

**Symptoms:**
```
org.apache.kafka.common.errors.TimeoutException: Timed out during connection
```

**Causes:**
- Kafka/PostgreSQL not running
- Wrong hostname/port
- Firewall blocking connection

**Solutions:**

Check if services are running:
```bash
# Kafka
nc -zv localhost 9092

# PostgreSQL  
psql -U postgres -d postgres -c "SELECT 1;"

# If using Docker
docker-compose ps
docker-compose logs kafka
docker-compose logs postgres
```

Fix config.yaml:
```yaml
kafka:
  brokers: "localhost:9092"  # Not 127.0.0.1 or wrong port

database:
  url: "jdbc:postgresql://localhost:5432/data_cleaner"  # Exact URL
```

If using Docker Compose:
```yaml
kafka:
  brokers: "kafka:29092"  # Not localhost; use service name

database:
  url: "jdbc:postgresql://postgres:5432/data_cleaner"  # Service name
```

### Problem: "No such table: cleaned_records"

**Symptoms:**
```
ERROR: relation "cleaned_records" does not exist
```

**Cause:** Database schema wasn't initialized.

**Solution:** Ensure PostgreSQL extension exists:
```bash
psql -U postgres -d data_cleaner -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
```

Delete and recreate the database:
```bash
dropdb data_cleaner
createdb data_cleaner
psql -U postgres -d data_cleaner -c "CREATE EXTENSION pgcrypto;"

# Then restart the app (schema initializes on first run)
```

### Problem: "OutOfMemoryError: Java heap space"

**Symptoms:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Causes:**
- Dedup cache growing too large
- Too many connections in pool
- Queue accumulating records

**Solutions:**

Increase heap size:
```bash
java -Xms1g -Xmx4g -jar target/data-cleaner-1.0.0.jar config.yaml
```

Or in docker-compose.yml:
```yaml
environment:
  JAVA_OPTS: "-Xms1g -Xmx4g"
```

Reduce concurrent workers:
```yaml
cleaning:
  concurrent_workers: 4  # Was 8
```

Reduce batch size:
```yaml
kafka:
  batch_size: 500  # Was 1000
```

Clear dedup cache periodically (or switch to Bloom filter for large datasets).

### Problem: "HikariPool - Connection is not available"

**Symptoms:**
```
org.postgresql.util.PSQLException: 
Connection pool exhausted; queue timeout reached
```

**Cause:** All database connections are in use; workers not finishing quickly enough.

**Solutions:**

Increase pool size:
```yaml
database:
  pool_size: 50  # Was 20
```

Check for slow queries:
```bash
psql -U postgres -d data_cleaner -c \
  "SELECT query, mean_exec_time FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 5;"
```

Add indexes if missing:
```bash
psql -U postgres -d data_cleaner -c \
  "CREATE INDEX IF NOT EXISTS idx_cleaned_status ON cleaned_records(status);"
```

### Problem: "Duplicate hash collisions" (false positives on dedup)

**Symptoms:** Legitimate different records marked as duplicates.

**Likelihood:** ~1 in 2^32 (extremely rare)

**Solutions:**

If happening, switch to better hash (future enhancement):
```java
// Current: XOR (32-bit)
// Better: MurmurHash3 (128-bit)
```

Or use Bloom filter for probabilistic dedup:
```java
BloomFilter<CharSequence> filter = BloomFilter.create(
    Funnels.stringFunnel(Charsets.UTF_8),
    100000,
    0.01  // 1% false positive rate
);
```

---

## Part 12: Performance Tuning

### Baseline Measurements

Before tuning, measure current performance:

```bash
# Start app with verbose logging
java -jar target/data-cleaner-1.0.0.jar config.yaml 2>&1 | tee app.log

# In another terminal, generate load
java -cp target/data-cleaner-1.0.0.jar \
  com.daetl.cleaner.test.SampleDataProducer localhost:9092 raw_data 10000

# Monitor
watch "psql -U postgres -d data_cleaner -c \
  \"SELECT COUNT(*) FROM cleaned_records;\""
```

Measure:
- **Time to process 10k records**: `grep "Batch.*completed" app.log`
- **Throughput**: Records / time
- **CPU usage**: `top` or `docker stats`
- **Memory**: `jps -m` or `docker stats`

### Tuning Strategy

```
Bottleneck → Cause → Fix

Slow Cleaning → Null removal or dedup → More workers
Slow Database → Network/I/O → Batch size, pool size
High Memory → Dedup cache or queue → Reduce, or use Bloom filter
High CPU → Hashing or streams → Optimize hash function
```

### Common Tuning Knobs

#### For High Throughput (10k+ rec/sec)

```yaml
cleaning:
  concurrent_workers: 16      # More parallelism (was 8)
  batch_size: 5000            # Bigger batches (was 1000)

database:
  pool_size: 50               # More connections (was 20)

kafka:
  batch_size: 5000            # Fetch more per poll (was 1000)
```

Trade-off: Higher latency (batches take longer to accumulate).

#### For Low Latency (<200ms)

```yaml
cleaning:
  concurrent_workers: 4       # Fewer, faster startup (was 8)
  batch_size: 100             # Small batches (was 1000)

database:
  pool_size: 10               # Enough connections (was 20)

kafka:
  batch_size: 100             # Poll frequently (was 1000)
```

Trade-off: Lower throughput (one-by-one processing is slow).

#### For Small Memory Footprint

```yaml
cleaning:
  concurrent_workers: 2       # Fewer threads (was 8)

database:
  pool_size: 5                # Fewer connections (was 20)

# Queue capacity (in code)
BlockingQueue<List<DataRecord>> processingQueue = 
    new LinkedBlockingQueue<>(10);  // Was 100
```

#### For Cost Optimization (cloud)

```yaml
cleaning:
  concurrent_workers: 8
  batch_size: 5000            # Fewer, bigger batches = fewer machines

database:
  pool_size: 30

# Run 3 instances vs 1 instance with 24 workers
```

### Profiling

#### CPU Profiling

Which part is slow?

```bash
# Run with profiler
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
  -jar target/data-cleaner-1.0.0.jar config.yaml
```

Or use async-profiler:
```bash
# Download: https://github.com/async-profiler/async-profiler
./async-profiler.sh start jps  # Get Java PID
./async-profiler.sh stop jps -o flamegraph
# View flamegraph.html
```

#### Memory Profiling

What's using memory?

```bash
# Enable GC logging
java -Xms1g -Xmx4g \
  -XX:+PrintGCDetails \
  -XX:+PrintGCTimeStamps \
  -Xloggc:gc.log \
  -jar target/data-cleaner-1.0.0.jar config.yaml

# Analyze
cat gc.log | grep "Pause Young"  # Frequency and duration of GC
```

#### Throughput Benchmarking

```bash
# Generate 100k records, time it
time java -cp target/data-cleaner-1.0.0.jar \
  com.daetl.cleaner.test.SampleDataProducer localhost:9092 raw_data 100000

# Check how many stored in DB
psql -U postgres -d data_cleaner -c \
  "SELECT COUNT(*) FROM cleaned_records;"
```

Calculate:
```
Throughput = Records / Time (seconds)
```

Example:
- Time: 10 seconds
- Records: 100,000
- **Throughput: 10,000 records/sec**

### JVM Tuning

#### GC Tuning

G1GC (good for mixed workloads):
```bash
java -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -jar target/data-cleaner-1.0.0.jar config.yaml
```

ZGC (low-latency, requires Java 21):
```bash
java -XX:+UseZGC \
  -XX:ZUncommitDelay=300 \
  -jar target/data-cleaner-1.0.0.jar config.yaml
```

#### Thread Tuning

Threads cost memory (~1MB each). Don't create more than CPU cores:

```bash
# Check CPU cores
grep -c ^processor /proc/cpuinfo

# If 8 cores, use:
concurrent_workers: 8
```

### Database Tuning

#### PostgreSQL Configuration

Edit `/etc/postgresql/15/main/postgresql.conf`:

```ini
# Larger shared buffers for caching
shared_buffers = 256MB

# More work memory per operation
work_mem = 20MB

# Parallel operations
max_parallel_workers_per_gather = 4
```

Restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

#### Batch Insert Optimization

Current: 1000 records per INSERT

Try larger:
```java
if (batch.size() >= 5000) {  // Was 1000
    queue.put(batch);
}
```

Or prepare batch insert differently:
```java
// Instead of addBatch() per record
StringBuilder sql = new StringBuilder("INSERT INTO cleaned_records (...) VALUES");
for (int i = 0; i < records.size(); i++) {
    if (i > 0) sql.append(",");
    sql.append("(?, ?, ?, ?, ?)");
}
sql.append(" ON CONFLICT DO UPDATE SET updated_at = CURRENT_TIMESTAMP");
```

---

## Part 13: Extending the System

### Add Custom Cleaning Rules

Example: Uppercase all names, trim whitespace

Create `src/main/java/com/daetl/cleaner/engine/CustomCleaner.java`:

```java
package com.daetl.cleaner.engine;

import java.util.*;

/**
 * Custom cleaning rules beyond null removal and dedup.
 */
public class CustomCleaner {
    
    /**
     * Apply all custom rules to a record.
     */
    public static Map<String, Object> applyCustomRules(Map<String, Object> fields) {
        Map<String, Object> cleaned = new HashMap<>(fields);
        
        // Rule 1: Uppercase names
        if (cleaned.containsKey("name")) {
            String name = (String) cleaned.get("name");
            if (name != null) {
                cleaned.put("name", name.toUpperCase());
            }
        }
        
        // Rule 2: Trim all strings
        cleaned.replaceAll((key, value) -> {
            if (value instanceof String) {
                return ((String) value).trim();
            }
            return value;
        });
        
        // Rule 3: Normalize email (lowercase)
        if (cleaned.containsKey("email")) {
            String email = (String) cleaned.get("email");
            if (email != null) {
                cleaned.put("email", email.toLowerCase());
            }
        }
        
        // Rule 4: Validate age range
        if (cleaned.containsKey("age")) {
            Integer age = (Integer) cleaned.get("age");
            if (age != null && (age < 0 || age > 150)) {
                cleaned.remove("age");  // Invalid age
            }
        }
        
        return cleaned;
    }
}
```

Integrate into cleaning engine:

```java
// In DataCleaningEngine.cleanRecord()
Map<String, Object> cleanedFields = removeNullValues(record.getFields());
cleanedFields = CustomCleaner.applyCustomRules(cleanedFields);  // Add this line
```

### Add Regex Validation

```java
public class RegexCleaner {
    private static final String EMAIL_REGEX = "^[A-Za-z0-9+_.-]+@(.+)$";
    private static final String PHONE_REGEX = "^\\+?[1-9]\\d{1,14}$";
    
    public static boolean isValidEmail(String email) {
        return email != null && email.matches(EMAIL_REGEX);
    }
    
    public static boolean isValidPhone(String phone) {
        return phone != null && phone.matches(PHONE_REGEX);
    }
    
    public static Map<String, Object> validateAndClean(Map<String, Object> fields) {
        Map<String, Object> cleaned = new HashMap<>(fields);
        
        // Remove invalid emails
        if (cleaned.containsKey("email")) {
            String email = (String) cleaned.get("email");
            if (!isValidEmail(email)) {
                cleaned.remove("email");
            }
        }
        
        // Remove invalid phones
        if (cleaned.containsKey("phone")) {
            String phone = (String) cleaned.get("phone");
            if (!isValidPhone(phone)) {
                cleaned.remove("phone");
            }
        }
        
        return cleaned;
    }
}
```

### Add Schema Validation (Avro)

For structured data, use Avro schema:

```java
// src/main/resources/user-schema.avsc
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null},
    {"name": "age", "type": ["null", "int"], "default": null}
  ]
}
```

Validate records:
```java
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaValidator {
    private final Schema schema;
    
    public SchemaValidator(String schemaPath) throws Exception {
        this.schema = new Schema.Parser().parse(
            new File(schemaPath)
        );
    }
    
    public boolean isValid(Map<String, Object> record) {
        // Validate against schema
        // Returns true if matches schema
        return true;  // Simplified
    }
}
```

---

## Part 14: Production Deployment

### Pre-Deployment Checklist

Before going live:

- [ ] Test with production data volume (10M+ records)
- [ ] Run for 24+ hours, check for memory leaks
- [ ] Configure database backups
- [ ] Set up monitoring/alerting
- [ ] Document runbooks for common issues
- [ ] Test graceful shutdown
- [ ] Load test (ramp up from 0 → peak load)
- [ ] Verify TLS certificates (if enabled)
- [ ] Set resource limits (CPU, memory)

### Kubernetes Deployment

Scale to multiple instances:

Create `k8s-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-cleaner
spec:
  replicas: 3
  selector:
    matchLabels:
      app: data-cleaner
  template:
    metadata:
      labels:
        app: data-cleaner
    spec:
      containers:
      - name: cleaner
        image: data-cleaner:1.0
        env:
        - name: KAFKA_BROKERS
          value: "kafka-cluster:9092"
        - name: DB_URL
          value: "jdbc:postgresql://postgres-service:5432/data_cleaner"
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: password
        - name: JAVA_OPTS
          value: "-Xms512m -Xmx2g"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: data-cleaner-service
spec:
  selector:
    app: data-cleaner
  ports:
  - port: 8080
    targetPort: 8080
  type: ClusterIP
```

Deploy:
```bash
kubectl apply -f k8s-deployment.yaml

# Check status
kubectl get pods -l app=data-cleaner

# View logs
kubectl logs -l app=data-cleaner -f
```

### Monitoring (Prometheus)

Add Prometheus metrics export:

```java
// In DataCleanerApp
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;

public class MetricsExporter {
    private final PrometheusMeterRegistry registry;
    
    public MetricsExporter() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }
    
    public void registerMetrics(DataCleaningEngine engine) {
        registry.gauge("cleaner.records.processed", () -> engine.getRecordsProcessed());
        registry.gauge("cleaner.records.cleaned", () -> engine.getRecordsCleaned());
        registry.gauge("cleaner.records.skipped", () -> engine.getRecordsSkipped());
    }
    
    public String scrape() {
        return registry.scrape();
    }
}
```

Expose `/metrics` endpoint:
```java
// Add simple HTTP server
com.sun.net.httpserver.HttpServer server = 
    com.sun.net.httpserver.HttpServer.create(
        new java.net.InetSocketAddress(8080), 0
    );
server.createContext("/metrics", exchange -> {
    String response = metricsExporter.scrape();
    exchange.getResponseHeaders().set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(200, response.getBytes().length);
    exchange.getResponseBody().write(response.getBytes());
    exchange.close();
});
server.start();
```

### Alerting Rules (Prometheus)

Create `prometheus-rules.yaml`:

```yaml
groups:
- name: cleaner
  rules:
  - alert: CleanerStalled
    expr: rate(cleaner_records_processed[5m]) == 0
    for: 5m
    annotations:
      summary: "Data cleaner has stopped processing"
  
  - alert: HighDuplicateRate
    expr: cleaner_records_skipped / cleaner_records_processed > 0.2
    for: 5m
    annotations:
      summary: "Duplicate rate above 20%"
  
  - alert: DatabaseConnectionPoolExhausted
    expr: hikari_connections_active == hikari_connections_max
    for: 1m
    annotations:
      summary: "Database connection pool at capacity"
```

---

## Part 15: Common Pitfalls & Best Practices

### Pitfall 1: Shared Mutable State

**Bad:**
```java
// Don't do this in concurrent code!
Map<String, Integer> stats = new HashMap<>();
stats.put("processed", 0);

executorService.submit(() -> {
    stats.put("processed", stats.get("processed") + 1);  // Race condition!
});
```

**Good:**
```java
AtomicLong processed = new AtomicLong(0);

executorService.submit(() -> {
    processed.incrementAndGet();  // Thread-safe
});
```

**Lesson:** Use atomic types or synchronization for shared mutable state.

### Pitfall 2: Blocking Operations

**Bad:**
```java
// Blocking call in critical path!
kafkaConsumer.poll(Duration.ofSeconds(30));  // Waits 30s
```

**Good:**
```java
// Non-blocking poll with timeout
kafkaConsumer.poll(Duration.ofMillis(1000));  // 1s timeout
```

**Lesson:** Don't block threads; use timeouts or async patterns.

### Pitfall 3: Resource Leaks

**Bad:**
```java
public void processData() {
    Connection conn = dataSource.getConnection();
    // Do work
    // Oops, forgot conn.close()!
}
```

**Good:**
```java
try (Connection conn = dataSource.getConnection()) {
    // Do work
}  // Auto-closes
```

**Lesson:** Always use try-with-resources for cleanup.

### Pitfall 4: Configuration Hardcoded

**Bad:**
```java
String brokers = "localhost:9092";  // Won't work in production!
```

**Good:**
```java
String brokers = System.getenv("KAFKA_BROKERS");
if (brokers == null) brokers = "localhost:9092";  // Fallback
```

**Lesson:** Always externalize configuration (YAML, env vars, properties files).

### Pitfall 5: Ignoring Exceptions

**Bad:**
```java
try {
    processRecord(record);
} catch (Exception e) {
    // Silently swallow exception!
}
```

**Good:**
```java
try {
    processRecord(record);
} catch (Exception e) {
    logger.warn("Failed to process record: {}", record.getId(), e);
    metrics.recordFailure();
    // Continue processing other records
}
```

**Lesson:** Log errors, track failures, continue gracefully.

### Pitfall 6: Premature Optimization

**Bad:**
```java
// Over-complex optimization before profiling!
// Custom thread pool, lock-free structures, etc.
```

**Good:**
```java
// 1. Make it work
// 2. Measure (profile, benchmark)
// 3. Optimize bottleneck
```

**Lesson:** Profile first; optimize what matters.

### Best Practice 1: Health Checks

Expose health status:

```java
public HealthStatus getHealth() {
    try {
        persistence.getStatistics();  // DB check
        return new HealthStatus("HEALTHY", "All systems operational");
    } catch (Exception e) {
        return new HealthStatus("UNHEALTHY", e.getMessage());
    }
}
```

Used for Kubernetes probes.

### Best Practice 2: Structured Logging

**Bad:**
```java
logger.info("Processed records: " + processed);
```

**Good:**
```java
logger.info("Batch processing complete", 
    "batch_id", batchId, 
    "records_processed", processed,
    "processing_time_ms", elapsed);
```

Enables structured log analysis (ELK, Datadog, etc.).

### Best Practice 3: Graceful Degradation

**Bad:**
```java
if (database_slow) {
    throw new Exception("DB is slow!");
}
```

**Good:**
```java
if (database_slow) {
    logger.warn("Database slow; buffering records locally");
    // Continue processing, store to local queue
    // Flush when DB recovers
}
```

**Lesson:** Keep system running even when parts fail.

### Best Practice 4: Circuit Breaker

For external dependencies (Kafka, DB):

```java
public class CircuitBreaker {
    enum State { CLOSED, OPEN, HALF_OPEN }
    private State state = State.CLOSED;
    private int failureCount = 0;
    private static final int FAILURE_THRESHOLD = 5;
    
    public void call(Runnable operation) throws Exception {
        if (state == State.OPEN) {
            throw new Exception("Circuit breaker is OPEN");
        }
        
        try {
            operation.run();
            failureCount = 0;  // Reset on success
        } catch (Exception e) {
            failureCount++;
            if (failureCount >= FAILURE_THRESHOLD) {
                state = State.OPEN;  // Trip breaker
                logger.error("Circuit breaker opened");
            }
            throw e;
        }
    }
}
```

Prevents cascading failures.

---

## Part 16: Development Workflow

### Local Testing Loop

```bash
# 1. Modify code
vim src/main/java/com/daetl/cleaner/engine/DataCleaningEngine.java

# 2. Compile
mvn clean compile

# 3. Run unit tests
mvn test

# 4. Package
mvn package

# 5. Start app
java -jar target/data-cleaner-1.0.0.jar config.yaml &

# 6. Generate test data
java -cp target/data-cleaner-1.0.0.jar \
  com.daetl.cleaner.test.SampleDataProducer localhost:9092 raw_data 1000

# 7. Check results
psql -U postgres -d data_cleaner -c "SELECT COUNT(*) FROM cleaned_records;"

# 8. Stop app
kill %1
```

### Debugging

#### Print Statements

```java
System.out.println("DEBUG: Record hash = " + hash);
logger.debug("Record {} -> {}", id, fields);
```

Enable debug logging:
```java
// src/main/resources/logback.xml
<root level="DEBUG">
    <appender-ref ref="CONSOLE" />
</root>
```

#### Remote Debugging

```bash
# Start with debug port
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
  -jar target/data-cleaner-1.0.0.jar config.yaml

# Connect IDE: Run → Debug Configurations → Remote → Connect to localhost:5005
```

#### Breakpoints in IDE

IntelliJ IDEA / Eclipse: Set breakpoint, debug as above.

---

## Part 17: Quick Reference

### Common Commands

```bash
# Build
mvn clean package

# Run app
java -jar target/data-cleaner-1.0.0.jar config.yaml

# Run with custom config
java -jar target/data-cleaner-1.0.0.jar /path/to/config.yaml

# Run with JVM options
java -Xms2g -Xmx4g -XX:+UseG1GC \
  -jar target/data-cleaner-1.0.0.jar config.yaml

# Run GUI dashboard
java -cp target/data-cleaner-1.0.0.jar \
  --add-modules javafx.controls,javafx.fxml \
  com.daetl.cleaner.gui.CleanerDashboard

# Generate test data
java -cp target/data-cleaner-1.0.0.jar \
  com.daetl.cleaner.test.SampleDataProducer brokers topic count

# Docker
docker build -t data-cleaner:1.0 .
docker-compose up -d
docker-compose logs -f data-cleaner

# Database
psql -U postgres -d data_cleaner -c "SELECT COUNT(*) FROM cleaned_records;"
```

### Database Queries

```sql
-- Total stats
SELECT COUNT(*) as total, 
       SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) as duplicates,
       SUM(null_values_removed) as nulls_removed
FROM cleaned_records;

-- By status
SELECT status, COUNT(*) 
FROM cleaned_records 
GROUP BY status;

-- Recent records
SELECT record_id, null_values_removed, is_duplicate, created_at 
FROM cleaned_records 
ORDER BY created_at DESC 
LIMIT 10;

-- Processing speed
SELECT batch_number, 
       records_processed, 
       records_cleaned, 
       processing_time_ms,
       records_cleaned::float / processing_time_ms * 1000 as records_per_sec
FROM cleaning_metrics 
ORDER BY batch_number DESC 
LIMIT 10;

-- Average throughput
SELECT AVG(records_cleaned::float / processing_time_ms * 1000) as avg_records_per_sec
FROM cleaning_metrics;
```

### Configuration Presets

#### Development (Local)
```yaml
kafka:
  brokers: "localhost:9092"
database:
  url: "jdbc:postgresql://localhost:5432/data_cleaner"
cleaning:
  concurrent_workers: 4
```

#### Staging (Medium load)
```yaml
kafka:
  brokers: "kafka-staging:9092"
  batch_size: 2000
database:
  url: "jdbc:postgresql://postgres-staging:5432/data_cleaner"
  pool_size: 30
cleaning:
  concurrent_workers: 8
```

#### Production (High throughput)
```yaml
kafka:
  brokers: "kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092"
  batch_size: 5000
database:
  url: "jdbc:postgresql://postgres-prod:5432/data_cleaner"
  pool_size: 50
cleaning:
  concurrent_workers: 16
tls:
  enabled: true
  keystore_path: "/etc/secrets/keystore.jks"
```

---

## Conclusion

You now have everything needed to:

1. **Build** a complete data cleaning pipeline from scratch
2. **Understand** every architectural decision
3. **Test** locally with sample data
4. **Optimize** for your workload
5. **Deploy** to production safely
6. **Monitor** and troubleshoot issues
7. **Extend** with custom rules
8. **Scale** horizontally and vertically

### Key Skills You've Learned

- **Concurrency**: Threads, locks, atomic types, thread-safe data structures
- **Data Systems**: Streaming (Kafka), persistence (PostgreSQL), connection pooling
- **Architecture**: Decoupled components, backpressure, graceful shutdown
- **Performance**: Batching, indexing, profiling, optimization
- **DevOps**: Docker, compose, Kubernetes, monitoring
- **Production Readiness**: Error handling, health checks, configuration, logging

### Next Challenges

1. Build it for real data (10M+ records)
2. Add custom cleaning rules for your domain
3. Deploy to Kubernetes cluster
4. Set up Prometheus + Grafana dashboards
5. Implement Bloom filter for large dedup caches
6. Add Spark distributed processing
7. Create REST API for queries
8. Contribute improvements back to community

---

Good luck building! 🚀
