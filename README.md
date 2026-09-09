# Raiden — Data Preprocessing Platform for Students

---

## Table of Contents
1. [What This Project Is](#1-what-this-project-is)
2. [Tech Stack](#2-tech-stack)
3. [Project Structure](#3-project-structure)
4. [The Two Big Parts](#4-the-two-big-parts)
5. [Database Schema](#5-database-schema)
6. [Entities & Relationships](#6-entities--relationships)
7. [Enums](#7-enums)
8. [Repository Layer](#8-repository-layer)
9. [Service Layer — All Methods](#9-service-layer--all-methods)
10. [Download Strategy](#10-download-strategy)
11. [Concurrent Download Engine — How It Works](#11-concurrent-download-engine--how-it-works)
12. [Preprocessing Pipeline](#12-preprocessing-pipeline)
13. [Database Writers](#13-database-writers)
14. [Full Request Flow — End to End](#14-full-request-flow--end-to-end)
15. [Validation Rules](#15-validation-rules)
16. [Exception Handling](#16-exception-handling)
17. [application.properties Configuration](#17-applicationproperties-configuration)
18. [Week 1 vs Week 2 Scope](#18-week-1-vs-week-2-scope)
19. [Common Mistakes to Avoid](#19-common-mistakes-to-avoid)
20. [Why This Architecture](#20-why-this-architecture)

---

## 1. What This Project Is

A **self-contained Java GUI application** that helps AI/Data Science students work with large datasets from Kaggle without bandwidth or storage constraints.

**Core Problem:** A 30GB dataset from Kaggle takes hours to download on slow networks, consumes massive disk space, and is often overkill for practice work. Students need just 1GB of clean, preprocessed data.

**Solution:** Download only what's needed (e.g., 1GB from 30GB), clean it automatically, and store it in a database they can query and export. All from a simple desktop application.

**Core Goals:**
- Download large Kaggle datasets in chunks, respecting user-specified size limits
- Use concurrent downloads to maximize bandwidth utilization
- Clean datasets automatically (handle nulls, type conversions, duplicates)
- Store cleaned data in Postgres (or CSV/Excel for MVP)
- Show progress in a GUI so the student isn't staring at a blank screen
- Let students export cleaned data back to CSV/Parquet/database

---

## 2. Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Java 17+ |
| Framework | Spring Boot |
| GUI | JavaFX or Swing |
| Database | PostgreSQL (MVP), SQLite/CSV (fallback) |
| ORM | Spring Data JPA / Hibernate |
| HTTP Client | OkHttp (for concurrent downloads) |
| External APIs | Kaggle Hub (dataset metadata + download), GCS (via redirect) |
| Build Tool | Maven or Gradle |
| Utilities | Lombok, Apache Commons CSV |

**Why OkHttp over raw HttpURLConnection?** Built-in connection pooling, retry logic, and easy request interceptors for Range headers. Handles the concurrency complexity for you.

---

## 3. Project Structure

```
src/main/java/com/raiden/
│
├── entity/
│   ├── DatasetJobEntity.java       // tracks download job status
│   ├── DownloadChunkEntity.java    // stores which chunks completed
│   ├── PreprocessingLogEntity.java // records what was cleaned
│   ├── DatabaseConfigEntity.java   // user's chosen DB connection
│   └── enums/
│       ├── JobStatus.java          // IDLE, DOWNLOADING, PREPROCESSING, COMPLETE
│       ├── CleaningAction.java     // NULL_FILLED, DUPLICATES_REMOVED, TYPE_CONVERTED
│       └── DatabaseType.java       // POSTGRES, SQLITE, CSV
│
├── repository/
│   ├── DatasetJobRepository.java
│   ├── DownloadChunkRepository.java
│   ├── PreprocessingLogRepository.java
│   └── DatabaseConfigRepository.java
│
├── service/
│   ├── KaggleMetadataService.java   // get dataset size, file info
│   ├── ChunkedDownloadService.java  // orchestrate parallel downloads
│   ├── GcsUrlResolverService.java   // handle Kaggle → GCS redirect
│   ├── PreprocessingService.java    // clean and validate rows
│   ├── DatabaseWriterService.java   // write to Postgres/SQLite/CSV
│   ├── ProgressTrackerService.java  // UI progress updates
│   └── DatasetJobService.java       // orchestrator (mirrors GatewayService)
│
├── gui/
│   ├── MainWindow.java             // JavaFX main window
│   ├── DownloadPanel.java          // Kaggle link + size limit input
│   ├── ProgressPanel.java          // download % + rows processed
│   ├── DatabaseConfigPanel.java    // Postgres/SQLite/CSV setup
│   └── ExportPanel.java            // export cleaned data
│
├── dto/
│   ├── KaggleDatasetDTO.java       // metadata from Kaggle API
│   ├── DownloadTaskDTO.java        // one chunk's start/end bytes
│   ├── PreprocessingResultDTO.java // how many rows cleaned, what actions
│   ├── DatabaseConfigDTO.java      // connection string, credentials
│   └── ProgressUpdateDTO.java      // for GUI: bytes done, rows done
│
├── config/
│   ├── KaggleConfig.java           // API credentials from env
│   ├── OkHttpConfig.java           // client config, retry policy
│   └── DatabaseConfig.java         // JPA config
│
└── exception/
    ├── GlobalExceptionHandler.java
    ├── DownloadFailedException.java
    ├── PreprocessingFailedException.java
    └── DatabaseConnectionException.java
```

---

## 4. The Two Big Parts

### Part 1 — Download Engine
Gets data from Kaggle to disk as efficiently as possible.

**Step by step:**
1. User pastes Kaggle dataset link (e.g., `kaggle.com/datasets/user/dataset-name`)
2. Query Kaggle's API to find the actual file and its total size (30GB)
3. User specifies limit (1GB)
4. Resolve the GCS URL (follow Kaggle's redirect)
5. Split 1GB into chunks (10MB each = 100 chunks)
6. Launch 4 concurrent threads, each downloading one chunk via Range requests
7. Write chunks to disk in the correct order (chunk 0 at position 0, chunk 1 at position 10MB, etc.)
8. Track progress: X bytes downloaded / 1GB target

### Part 2 — Preprocessing & Storage Engine
Takes the downloaded file, cleans it, and stores it where the student wants.

**Step by step:**
1. Open the downloaded file (CSV, Parquet, JSON, etc.)
2. Stream-parse rows one by one (don't load everything into memory)
3. For each row: handle nulls (fill with default, skip, or drop), type-convert, remove duplicates
4. Write cleaned rows to the chosen output (Postgres, SQLite, CSV)
5. Track progress: Y rows processed / Z total rows
6. Log what was cleaned (e.g., "removed 50 rows with >50% nulls", "converted 1000 date strings to timestamps")

---

## 5. Database Schema

```sql
-- Tracks each download/preprocessing job the user starts
CREATE TABLE dataset_job (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    kaggle_dataset_url VARCHAR(500) NOT NULL,
    target_size_bytes BIGINT NOT NULL,
    actual_size_downloaded_bytes BIGINT DEFAULT 0,
    status VARCHAR(50),                    -- IDLE, DOWNLOADING, PREPROCESSING, COMPLETE
    total_rows_processed INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- Track which chunks have been downloaded (for resume on failure)
CREATE TABLE download_chunk (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id BIGINT NOT NULL,
    chunk_index INT NOT NULL,             -- 0, 1, 2, ...
    start_byte BIGINT NOT NULL,
    end_byte BIGINT NOT NULL,
    downloaded BOOLEAN DEFAULT FALSE,
    retry_count INT DEFAULT 0,
    FOREIGN KEY (job_id) REFERENCES dataset_job(id),
    UNIQUE(job_id, chunk_index)
);

-- Log what preprocessing actions were taken
CREATE TABLE preprocessing_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id BIGINT NOT NULL,
    action VARCHAR(100),                  -- NULL_FILLED, DUPLICATES_REMOVED, TYPE_CONVERTED
    column_name VARCHAR(255),
    rows_affected INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES dataset_job(id)
);

-- Store connection details for different databases
CREATE TABLE database_config (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id BIGINT NOT NULL,
    database_type VARCHAR(50),            -- POSTGRES, SQLITE, CSV
    connection_string VARCHAR(500),       -- for Postgres: jdbc:postgresql://host:port/db
    username VARCHAR(255),
    password VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES dataset_job(id)
);
```

---

## 6. Entities & Relationships

- **DatasetJobEntity** — one job per Kaggle dataset the student downloads. One-to-many with `DownloadChunkEntity` and `PreprocessingLogEntity`.
- **DownloadChunkEntity** — each row represents one chunk (e.g., bytes 0–10MB, 10–20MB). Tracks whether it was successfully downloaded (for resume logic).
- **PreprocessingLogEntity** — each row records one cleaning action (e.g., "filled 50 NULLs in age column"). Helps the student understand what happened to their data.
- **DatabaseConfigEntity** — the connection parameters the student chose. Separate row per job so they can run multiple jobs with different outputs.

---

## 7. Enums

```
JobStatus {
    IDLE               // user configured, waiting to start
    DOWNLOADING        // chunks being fetched from GCS
    PREPROCESSING      // rows being cleaned
    COMPLETE           // all done, data stored
    FAILED             // something went wrong
}

CleaningAction {
    NULL_FILLED        // replaced NULL with a default value
    DUPLICATES_REMOVED // dropped exact row duplicates
    TYPE_CONVERTED     // parsed string → date, string → int, etc.
    ROWS_SKIPPED       // dropped rows with too many NULLs
}

DatabaseType {
    POSTGRES           // full relational database
    SQLITE             // single-file database (good for laptops)
    CSV                // export to CSV file
}
```

---

## 8. Repository Layer

**DatasetJobRepository**
- `findById(Long jobId)` — get job details
- `findByStatusAndCreatedAtAfter(JobStatus status, LocalDateTime since)` — find active jobs

**DownloadChunkRepository**
- `findByJobIdAndDownloadedFalse(Long jobId)` — find which chunks still need downloading (for resume)
- `updateDownloadedByJobIdAndChunkIndex(Long jobId, int index, boolean downloaded)` — mark chunk as done

**PreprocessingLogRepository**
- `findByJobId(Long jobId)` — get all cleaning actions for a job, for the report

**DatabaseConfigRepository**
- `findByJobId(Long jobId)` — get where this job should write its output

All follow Spring Data JPA naming conventions. No complex queries needed at MVP scale.

---

## 9. Service Layer — All Methods

**KaggleMetadataService**
- `getDatasetInfo(String kaggleUrl)` → returns file name, total size in bytes, file type (CSV, Parquet, etc.)
- Calls Kaggle's API or scrapes basic info; doesn't download yet, just inspects

**GcsUrlResolverService**
- `resolveDownloadUrl(String kaggleDatasetUrl)` → returns the actual GCS URL that Kaggle redirects to
- Follows HTTP 301/302 redirect from Kaggle API to get the GCS storage location

**ChunkedDownloadService**
- `downloadWithSizeLimit(String gcsUrl, long limitBytes, int concurrency)` → orchestrates the parallel download
- Creates `DownloadChunkEntity` rows for each chunk
- Launches 4 concurrent tasks (OkHttp), each with a Range request
- Writes chunks to disk in the correct order using `RandomAccessFile`
- Updates job status and byte counters as chunks complete
- Supports resume: on restart, only downloads chunks marked `downloaded=false`

**PreprocessingService**
- `processRows(File inputFile, PreprocessingConfig config)` → streams rows, cleans each one
- Config specifies: how to handle NULLs (fill/skip/drop), which columns to type-convert, etc.
- Returns `PreprocessingResultDTO` with: total rows, rows skipped, actions taken
- Never loads the full file into memory; streams it

**DatabaseWriterService**
- `writeToPostgres(List<Row> rows, DatabaseConfigDTO config)` → creates table, inserts rows in batches
- `writeToSqlite(List<Row> rows, DatabaseConfigDTO config)` → similar, but SQLite specific
- `writeToCSV(List<Row> rows, File outputFile)` → export to CSV
- Handles connection pooling, transaction management, error recovery

**ProgressTrackerService**
- `recordProgress(Long jobId, ProgressUpdateDTO update)` → updates byte counters and row counters
- `notifyGui(ProgressUpdateDTO)` → sends event to GUI thread so progress bar updates in real time
- Bridge between background download/preprocessing threads and UI

**DatasetJobService** (the orchestrator)
- `startJob(String kaggleUrl, long limitBytes, DatabaseConfigDTO outputConfig)` → wraps everything together
- Calls `KaggleMetadataService` → `GcsUrlResolverService` → `ChunkedDownloadService` → `PreprocessingService` → `DatabaseWriterService` in sequence
- Handles exceptions at each stage, rolls back partial work if needed
- Updates `DatasetJobEntity.status` through each stage

---

## 10. Download Strategy

**Why you can't just download the whole file:**
- Network interruptions mid-download mean starting over (wasted bandwidth)
- 30GB takes 10+ hours on slow connections; can't ask student to wait
- Concurrent downloads maximize throughput (4 threads on a 4Mbps connection ≈ 16Mbps effective)

**The approach:**

1. **Get GCS URL** — Kaggle redirects you to the actual storage location on Google Cloud Storage. Store this in the `DatasetJobEntity`.

2. **HEAD request** — Ask GCS "how many bytes total?" via a HEAD request. Returns `Content-Length: 30000000000` (30GB).

3. **Chunk calculation** — Split your target (1GB) into pieces:
   - 1GB = 1,024 MB = 1,048,576 KB
   - Chunk size = 10MB = 10,240 KB
   - Number of chunks = 1,048,576 / 10,240 ≈ 102 chunks
   - Create 102 `DownloadChunkEntity` rows with start/end byte ranges

4. **Launch concurrent tasks** — Each task:
   - Picks one chunk that's marked `downloaded=false`
   - Sends HTTP GET with `Range: bytes=0-10485759` (for chunk 0)
   - Writes response body to disk at position 0 using `RandomAccessFile.seek(0)`
   - Marks chunk as `downloaded=true`
   - Signals progress tracker: +10MB done

5. **Out-of-order assembly** — Chunks complete at different times (some threads faster than others). Each chunk knows its offset, so writes go to the right place regardless of completion order. RandomAccessFile is thread-safe for this (via synchronized block).

6. **Resume on failure** — If download crashes after 50 chunks, restart: query for chunks where `downloaded=false` and continue from there. No duplicate downloads.

---

## 11. Concurrent Download Engine — How It Works

**The threading model:**

```
Main thread (GUI)
    ↓
Calls ChunkedDownloadService.downloadWithSizeLimit()
    ↓
Launches ExecutorService with 4 worker threads
    ↓
Worker 1: downloads chunk 0 (bytes 0-10MB)
Worker 2: downloads chunk 1 (bytes 10-20MB)
Worker 3: downloads chunk 2 (bytes 20-30MB)
Worker 4: downloads chunk 3 (bytes 30-40MB)
Worker 1 finishes chunk 0 → picks up chunk 4
Worker 2 finishes chunk 1 → picks up chunk 5
... continues until all chunks marked downloaded
    ↓
All workers finish → return total bytes to main thread
    ↓
GUI shows "Download complete"
```

**Why 4 threads?** Most networks saturate around 4 concurrent connections. More threads just waste system resources; fewer leave bandwidth on the table. Make it configurable in `application.properties`.

**Synchronization:**
- Each worker is independent; they don't share state except for:
  - Updating `DownloadChunkEntity.downloaded` in the database (use optimistic locking or a simple `synchronized` block on the file write position)
  - Writing to `RandomAccessFile` at different positions (thread-safe as long as each thread writes to its own offset range)
- Progress updates go through `ProgressTrackerService`, which uses a thread-safe queue to accumulate updates and fire them to the GUI in batches (every 1 second) rather than 1000 times per second.

**Connection pooling:**
- Configure OkHttp with a shared `ConnectionPool` so workers reuse connections to GCS instead of opening 4 new ones repeatedly.
- Reduces TLS handshake overhead and keeps network utilization smooth.

---

## 12. Preprocessing Pipeline

**The flow:**

1. **Input** — Downloaded file (CSV, Parquet, etc.)
2. **Stream parser** — Read rows one at a time using Apache Commons CSV or a similar library. Never load the full file into memory.
3. **Per-row validation** — For each row:
   - Count non-NULL columns; if < 50%, skip the row (log the action)
   - For columns with missing values, fill with a default (0 for numeric, "UNKNOWN" for strings, null for optional columns)
   - Parse string columns to their intended types (e.g., "2024-01-15" → java.time.LocalDate)
4. **Deduplication** — If the row is an exact duplicate of a previously seen row, skip it (log the action)
5. **Output** — Write clean row to the database or CSV

**Configuration:**
- Student specifies cleaning rules in the GUI:
  - "Fill NULLs in age with 0" or "Skip rows with NULL age"
  - "Convert birth_date from string to date"
  - "Remove exact duplicates"
- Store config in `PreprocessingConfig` object (part of `ProgressUpdateDTO` or separate entity)

**Memory efficiency:**
- Process one row at a time; don't buffer all rows in memory
- Write to database in batches (e.g., every 100 rows) to balance insert speed vs. memory usage
- This scales to files larger than available RAM

**Output:**
- `PreprocessingResultDTO`: total rows input, rows output, rows skipped, list of actions taken
- Log each action to `PreprocessingLogEntity` so student can review what happened

---

## 13. Database Writers

**Why separate per-database?**
- Postgres, SQLite, and CSV have different APIs and transaction models
- Postgres needs `createTable()` then batch inserts; SQLite uses same API but file path; CSV is just file I/O

**DatabaseWriterService methods:**

`writeToPostgres(List<Row> rows, DatabaseConfigDTO config)`
- Connect using the provided `connectionString`, `username`, `password`
- Check if table exists; if not, infer schema from row 1 (column names and types)
- Create table with appropriate column types
- Batch insert rows (e.g., 1000 at a time) to avoid roundtrip overhead
- Commit transaction on success; rollback on any error

`writeToSqlite(List<Row> rows, DatabaseConfigDTO config)`
- Same as Postgres but use SQLite JDBC driver
- File lives on disk at the path specified in `connectionString`
- Automatically creates the database file if it doesn't exist

`writeToCSV(List<Row> rows, File outputFile)`
- Stream rows to a CSV file using Apache Commons CSV
- Write header from column names
- One row per line, comma-separated values, quote strings with embedded commas

**Batch insert logic:**
- Don't insert one row at a time; collect 1000 rows, build a single INSERT statement with 1000 value tuples, execute it once
- Reduces network roundtrips to the database from potentially millions to thousands
- Trade-off: batch size must fit in memory and in the database's max statement size (usually not a problem for 1000 rows)

---

## 14. Full Request Flow — End to End

**User perspective:**
1. Opens Raiden GUI
2. Pastes `https://www.kaggle.com/datasets/username/dataset-name` into "Dataset URL" field
3. Enters `1` and selects `GB` (meaning 1 gigabyte limit)
4. Selects "PostgreSQL" and enters connection details (host, port, database, username, password)
5. Clicks "Start Download"

**Behind the scenes:**

1. **Validation** — Check that the Kaggle URL is well-formed, database config is valid (try a test connection)

2. **Create job** — Insert `DatasetJobEntity` with status=`IDLE`, save to database

3. **Metadata lookup** — `KaggleMetadataService.getDatasetInfo()` calls Kaggle API, gets file name ("dataset.csv"), total size (30GB)

4. **Resolve GCS URL** — `GcsUrlResolverService` follows the redirect from Kaggle → Google Cloud Storage, gets the actual download URL

5. **Update job** — Set status to `DOWNLOADING`, store GCS URL in the entity

6. **Create chunks** — Split 1GB into 102 chunks, create `DownloadChunkEntity` rows for each

7. **Launch downloads** — `ChunkedDownloadService` creates 4 worker threads, each grabs an unclaimed chunk and downloads via Range request

   - Each worker:
     - Queries for one chunk with `downloaded=false`
     - Sends GET request to GCS with `Range: bytes=START-END`
     - Writes response body to temp file at the right offset
     - Marks chunk as `downloaded=true` in DB
     - Calls `ProgressTrackerService.recordProgress(+chunkSize)`

   - Main thread polls the database every 1 second: "how many bytes done?" → updates GUI progress bar

8. **Temp file is complete** — All chunks downloaded, temp file is now 1GB of raw data

9. **Preprocessing** — `PreprocessingService.processRows()` streams the temp file row by row:
   - Fill NULLs
   - Type-convert strings to dates
   - Skip duplicates
   - Log each action to `PreprocessingLogEntity`
   - Accumulate clean rows in a list

10. **Write to database** — `DatabaseWriterService.writeToPostgres()`:
    - Create a Postgres connection
    - Infer schema from first row (columns and types)
    - CREATE TABLE IF NOT EXISTS
    - Batch insert all cleaned rows in groups of 1000
    - Commit transaction

11. **Mark job complete** — Update `DatasetJobEntity.status = COMPLETE`, set `completedAt` timestamp

12. **GUI notification** — `ProgressTrackerService` sends "download and preprocessing finished" event to GUI thread

13. **User can now query** — Opens a database client (pgAdmin, etc.), connects to their Postgres instance, runs `SELECT * FROM dataset LIMIT 10` to see cleaned data

---

## 15. Validation Rules

- Kaggle URL must match pattern `kaggle.com/datasets/[username]/[dataset-name]`
- Size limit must be > 0 and ≤ total file size
- Database connection string must be valid JDBC URL for the chosen database type
- For Postgres: test connection before allowing download to start
- Row must have at least 50% non-NULL columns to be kept (configurable)
- Chunk size must be between 1MB and 100MB (configurable, default 10MB)
- Concurrency level must be 1–16 (too many wastes resources or hits connection limits)

---

## 16. Exception Handling

**Scope of failures:**

| Stage | Failure | Recovery |
|-------|---------|----------|
| Metadata lookup | Kaggle API down | Retry 3x with exponential backoff; show user "Kaggle temporarily unreachable" |
| GCS redirect | GCS returns 403 Forbidden | User's Kaggle API key may be invalid; show error, prompt for re-auth |
| Download | Network timeout mid-chunk | Retry just that chunk up to 3x; if all fail, pause job, let user resume later |
| Download | GCS returns 416 (Range not satisfiable) | Chunk request is malformed; log and skip that chunk (data loss risk, but rare) |
| Preprocessing | Can't parse a row (malformed CSV) | Skip that row, log the error, continue |
| Database write | Connection fails | Retry connection; if it persists, pause and offer "retry" or "save to CSV instead" |
| Database write | Duplicate key error | If table already exists with same columns, append new rows instead of failing |

**Global exception handler:**
- Catch any uncaught exception in the orchestrator (`DatasetJobService`)
- Update job status to `FAILED`
- Send error message to GUI thread
- Clean up partial work (delete temp files, mark half-downloaded chunks for re-attempt)

---

## 17. application.properties Configuration

```properties
# Kaggle API
kaggle.api.key=YOUR_API_KEY
kaggle.api.username=YOUR_USERNAME

# Download settings
download.chunk.size.mb=10
download.concurrent.threads=4
download.retry.max-attempts=3
download.retry.backoff-ms=1000

# Preprocessing
preprocessing.null-threshold=0.5
preprocessing.batch-insert-size=1000

# Database defaults (for Postgres)
spring.datasource.url=jdbc:postgresql://localhost:5432/raiden_metadata
spring.datasource.username=root
spring.datasource.password=password
spring.jpa.hibernate.ddl-auto=update
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQLDialect

# GUI
gui.progress-update-interval-ms=1000

# Logging
logging.level.com.raiden=INFO
```

---

## 18. Week 1 vs Week 2 Scope

### Week 1 — MVP Foundation
- [ ] Database entities (DatasetJob, DownloadChunk, PreprocessingLog, DatabaseConfig)
- [ ] Repositories for all entities
- [ ] KaggleMetadataService (Kaggle API inspection, no download yet)
- [ ] GcsUrlResolverService (follow redirects, get GCS URL)
- [ ] Basic validation rules
- [ ] Exception handling scaffold
- [ ] JavaFX GUI skeleton (input fields, buttons, basic layout)
- [ ] Tests for entity/repo/basic service logic

### Week 2 — Full Engine
- [ ] ChunkedDownloadService (parallel downloads, Range requests, resume)
- [ ] PreprocessingService (row streaming, NULL filling, deduplication)
- [ ] DatabaseWriterService (Postgres, SQLite, CSV writers)
- [ ] ProgressTrackerService (progress events to GUI)
- [ ] DatasetJobService (orchestrator, ties everything together)
- [ ] GUI panels (download progress, preprocessing progress, success/error screens)
- [ ] End-to-end testing (mock Kaggle + GCS, full pipeline)
- [ ] Error recovery and resume logic
- [ ] Documentation and usage guide

---

## 19. Common Mistakes to Avoid

| Mistake | Why It Breaks | Fix |
|---------|---------------|-----|
| Loading entire file into memory for preprocessing | Will crash on files > RAM | Stream rows one at a time, never buffer all |
| No resume logic for downloads | Restart = redownload everything wasted bandwidth | Track completed chunks, skip them on resume |
| Using `ExecutorService` without shutdown | Threads hang, app won't exit | Always call `executor.shutdown()` and wait with `awaitTermination()` |
| Writing chunks to file without synchronization | Concurrent threads overwrite each other's data | Use `RandomAccessFile` + synchronized block or atomic operations |
| Hardcoding Kaggle API key in code | Security + key rotation nightmare | Always use `application.properties` + environment variables |
| Updating GUI from non-GUI threads | Swing/JavaFX crashes or shows nothing | Always use `Platform.runLater()` (JavaFX) or `SwingUtilities.invokeLater()` (Swing) for UI updates |
| No batch inserts to database | One row = one network roundtrip, 100x slower | Collect 1000 rows, one INSERT with 1000 tuples |
| Trusting row count from the downloaded file | Row count in CSV header is often inaccurate | Count actual rows during preprocessing, log the discrepancy |
| No null checks before type conversion | NullPointerException on unexpected nulls | Always check before converting; have a default fallback |
| Assuming all files are CSV | Kaggle has Parquet, JSON, Excel, Feather | Detect file type, pick the right parser |

---

## 20. Why This Architecture

**Separation of concerns:**
- `KaggleMetadataService` does one thing: inspect without downloading
- `ChunkedDownloadService` does one thing: parallel downloads with chunks
- `PreprocessingService` does one thing: clean rows
- `DatabaseWriterService` does one thing: persist clean data
- `DatasetJobService` orchestrates them in sequence

This makes each service testable in isolation and replaceable (e.g., "swap Postgres writer for Parquet writer" without touching download logic).

**Concurrent downloads are necessary because:**
- Kaggle's CDN/GCS has ample bandwidth, but a single connection is limited by your local network speed
- 4 concurrent connections × your ISP speed ≈ 4x effective throughput (up to the server's limits)
- This is the difference between 10 hours and 2.5 hours for a large dataset

**Streaming preprocessing is necessary because:**
- Students don't have 30GB RAM to load the full dataset
- Streaming keeps memory usage constant (1 row at a time, plus a small batch buffer)

**Chunked downloads are resumable because:**
- Network failures are common on slow connections
- Restarting the whole download is user-hostile and wasteful
- Tracking which chunks are done lets you resume from exactly where you left off

---

## 21. Future Improvements

- **Parquet/Arrow support** — detect and parse Parquet files natively (faster than CSV for large datasets)
- **Profiling before preprocessing** — scan first 10% of rows to infer column types automatically, so user doesn't have to specify
- **Cloud database support** — write directly to AWS RDS, GCP Cloud SQL (skip local Postgres)
- **Retry with exponential backoff** — for flaky networks, don't give up after 3 tries immediately
- **Incremental preprocessing** — if a job fails partway through, resume cleaning from the last successfully processed row (not from the start)
- **Data profiling report** — after preprocessing, show student a summary: "X columns, Y rows, Z nulls found and filled, W duplicates removed"
- **Scheduled downloads** — user can queue multiple datasets to download overnight, process them in sequence
- **Multi-user job queue** — if Raiden runs as a server (not just desktop app), multiple students can download simultaneously without blocking each other

---
