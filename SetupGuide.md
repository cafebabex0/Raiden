Yes. **You can do the entire project on Windows 11 without WSL.**

One important correction from my previous answer: **Docker Desktop on Windows normally uses WSL 2, but Docker Desktop also supports a Hyper-V backend.** If you explicitly do not want WSL, use **Hyper-V**. This requires Windows 11 Pro/Enterprise/Education; Windows Home cannot use the Hyper-V backend. ([Docker Documentation][1])

For this project, you do **not** need to install Kafka or PostgreSQL directly on Windows. Docker will run them.

# Windows 11 Setup Guide

## What you will have at the end
```text
Windows 11
│
├── Git
├── Maven
├── Java              ← already installed
├── Docker Desktop
│    │
│    ├── Kafka
│    │    └── localhost:9092
│    │
│    └── PostgreSQL
│         └── localhost:5432
│
└── Your Java project
     ├── JavaFX
     ├── Kafka Client
     ├── PostgreSQL JDBC
     ├── HikariCP
     └── SLF4J/Logback
```

JavaFX itself does **not** need a separate installation. Maven can download the JavaFX modules and native Windows libraries automatically. 

---

# 0. Check your Windows version

Open **PowerShell**:

```powershell
winver
```

You want Windows 11.

Now check the edition:

```powershell
(Get-ComputerInfo).WindowsProductName
```

You need:

```text
Windows 11 Pro
```

or

```text
Windows 11 Enterprise
```

or

```text
Windows 11 Education
```

if you want the **Hyper-V Docker backend**. Docker documents Windows Home as limited to Linux containers, while Hyper-V is available on Pro/Enterprise configurations. ([Docker Documentation][1])

If you have **Windows 11 Home**, tell me before proceeding. The no-WSL requirement changes the setup substantially.

---

# 1. Install Git

You need Git for:

* cloning projects
* version control
* GitHub
* branches/commits

You can install Git for Windows from the official project:

[Git for Windows](https://gitforwindows.org/?utm_source=chatgpt.com)

Or use `winget`.

Check whether `winget` exists:

```powershell
winget --version
```

Then:

```powershell
winget install --id Git.Git -e
```

Microsoft documents `winget install --id ... -e` as the standard exact-package installation syntax. ([Microsoft Learn][2])

Close and reopen PowerShell.

Verify:

```powershell
git --version
```

You should get something like:

```text
git version 2.55.0
```

Git for Windows also provides Git Bash, but **you don't need Git Bash for this project**. PowerShell is sufficient. ([Git for Windows][3])

---

# 2. Install Docker Desktop

This is the important part.

Download:

[Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/?utm_source=chatgpt.com)

Docker's current Windows installer supports different backends, including WSL 2 and Hyper-V. ([Docker Documentation][1])

## Before installing

Check whether virtualization is enabled.

Open:

```text
Task Manager
→ Performance
→ CPU
→ Virtualization
```

You want:

```text
Virtualization: Enabled
```

If it says disabled, enable Intel VT-x/AMD-V in your BIOS/UEFI.

---

# 3. Enable Hyper-V

Since you don't want WSL, enable Hyper-V.

Open **PowerShell as Administrator**.

Run:

```powershell
Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All
```

Windows will probably ask for a restart.

Restart.

After reboot:

```powershell
Get-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V
```

You want:

```text
State : Enabled
```

You can also check:

```powershell
Get-WindowsOptionalFeature -Online -FeatureName Containers
```

---

# 4. Install Docker Desktop

Run the Docker Desktop installer.

During installation, **do not select WSL 2**.

If the installer gives you a backend choice, select:

```text
Hyper-V
```

Docker's documentation states that Hyper-V is one of the supported Windows backends. ([Docker Documentation][1])

Start Docker Desktop.

Wait until Docker says:

```text
Docker Desktop is running
```

Then open PowerShell:

```powershell
docker --version
```

Then:

```powershell
docker compose version
```

You should get something similar to:

```text
Docker version ...
Docker Compose version ...
```

Docker Compose is what we'll use to start Kafka and PostgreSQL together.

---

# 5. Test Docker

Run:

```powershell
docker run --rm hello-world
```

You should see:

```text
Hello from Docker!
```

If this works, Docker is correctly installed.

---

# 6. Install Maven

You already have Java, so Maven is the next requirement.

Check Java first:

```powershell
java -version
```

Also:

```powershell
javac -version
```

And:

```powershell
$env:JAVA_HOME
```

If Java works but `JAVA_HOME` is empty, we should fix that before Maven.

Maven requires a JDK and its `bin` directory needs to be available through `PATH`. ([Apache Maven][4])

## Install Maven with winget

Try:

```powershell
winget search Apache.Maven
```

Then install the package it shows.

If the package ID is available as expected:

```powershell
winget install --id Apache.Maven -e
```

If that package isn't available in your `winget` source, use the official Maven ZIP installation instead.

Official Maven page:

[Apache Maven installation](https://maven.apache.org/install.html?utm_source=chatgpt.com)

### Manual installation

Download the binary ZIP.

Extract somewhere such as:

```text
C:\Tools\apache-maven
```

You should end up with:

```text
C:\Tools\apache-maven\bin
```

Add this to your **User PATH**:

```text
C:\Tools\apache-maven\bin
```

Don't put Maven inside your project directory.

Open a **new PowerShell**:

```powershell
mvn -version
```

Expected:

```text
Apache Maven 3.9.x
Java version: ...
Java home: ...
```

Maven's official Windows instructions specifically require the Maven `bin` directory to be on `PATH`. ([Apache Maven][5])

---

# 7. PostgreSQL — DON'T INSTALL IT DIRECTLY

For this project, **do not install PostgreSQL Server on Windows**.

We're going to run it inside Docker.

This gives you:

```text
Windows
   ↓
Docker
   ↓
PostgreSQL container
```

This is much cleaner for your project.

The official PostgreSQL Docker image supports running PostgreSQL through Docker Compose. ([Docker Hub][6])

---

# 8. Kafka — DON'T INSTALL IT DIRECTLY EITHER

Same principle.

Don't install Kafka manually on Windows.

We'll run:

```text
Windows
   ↓
Docker
   ↓
Kafka container
```

Apache Kafka officially provides Docker images. The current Kafka documentation provides the `apache/kafka` image and exposes Kafka on port `9092`. ([Apache Kafka][7])

---

# 9. Create your project directory

For example:

```powershell
mkdir E:\Projects\DataCleaner
cd E:\Projects\DataCleaner
```

If you prefer another drive, use that.

Create:

```text
DataCleaner
│
├── docker-compose.yml
├── pom.xml
├── src
│   ├── main
│   │   └── java
│   └── test
│       └── java
│
└── data
```

Create the directories:

```powershell
mkdir src
mkdir src\main
mkdir src\main\java
mkdir src\test
mkdir src\test\java
mkdir data
```

---

# 10. Create Docker Compose

Create:

```text
docker-compose.yml
```

Put this inside:

```yaml
services:

  kafka:
    image: apache/kafka:4.2.0
    container_name: data-cleaner-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

  postgres:
    image: postgres:18
    container_name: data-cleaner-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: data_cleaner
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

This gives you:

```text
Kafka
localhost:9092

PostgreSQL
localhost:5432
Database: data_cleaner
Username: postgres
Password: postgres
```

For local development, that's fine.

**Don't use this password in production.**

---

# 11. Start Kafka + PostgreSQL

From:

```text
E:\Projects\DataCleaner
```

run:

```powershell
docker compose up -d
```

Check:

```powershell
docker compose ps
```

You should see approximately:

```text
NAME                    STATUS
data-cleaner-kafka      Up
data-cleaner-postgres   Up
```

Then:

```powershell
docker ps
```

---

# 12. Test PostgreSQL

Run:

```powershell
docker exec -it data-cleaner-postgres psql -U postgres -d data_cleaner
```

You should enter the PostgreSQL shell:

```text
data_cleaner=#
```

Run:

```sql
SELECT version();
```

Then:

```sql
SELECT current_database();
```

You should see:

```text
data_cleaner
```

Exit:

```sql
\q
```

---

# 13. Test Kafka

Check the Kafka logs:

```powershell
docker logs data-cleaner-kafka
```

You should eventually see Kafka running without fatal errors.

Then create your topic:

```powershell
docker exec data-cleaner-kafka /opt/kafka/bin/kafka-topics.sh `
    --create `
    --topic raw_data `
    --bootstrap-server localhost:9092 `
    --partitions 3 `
    --replication-factor 1
```

Check:

```powershell
docker exec data-cleaner-kafka /opt/kafka/bin/kafka-topics.sh `
    --list `
    --bootstrap-server localhost:9092
```

You should see:

```text
raw_data
```

---

# 14. Test Kafka producer/consumer manually

Start a consumer:

```powershell
docker exec -it data-cleaner-kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic raw_data `
    --bootstrap-server localhost:9092
```

Leave that terminal running.

Open another PowerShell.

Run:

```powershell
docker exec -it data-cleaner-kafka /opt/kafka/bin/kafka-console-producer.sh `
    --topic raw_data `
    --bootstrap-server localhost:9092
```

Type:

```text
hello
```

Then:

```text
hello kafka
```

The consumer terminal should receive:

```text
hello
hello kafka
```

Now Kafka is actually working.

---

# 15. JavaFX — DON'T INSTALL AN SDK

This is important.

**Do not download a JavaFX SDK manually.**

Since we're using Maven, JavaFX can be declared in `pom.xml` and Maven downloads the correct Windows libraries automatically. OpenJFX explicitly documents this approach. 

We'll use:

```text
Java
 +
Maven
 +
JavaFX Maven dependencies
```

---

# 16. Create the Maven project

Inside:

```text
E:\Projects\DataCleaner
```

create:

```text
pom.xml
```

For the first setup test, use:

```xml
<?xml version="1.0" encoding="UTF-8"?>

<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="
           http://maven.apache.org/POM/4.0.0
           https://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>

    <groupId>com.daetl</groupId>
    <artifactId>data-cleaner</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <javafx.version>21</javafx.version>
    </properties>

    <dependencies>

        <!-- JavaFX -->
        <dependency>
            <groupId>org.openjfx</groupId>
            <artifactId>javafx-controls</artifactId>
            <version>${javafx.version}</version>
        </dependency>

        <dependency>
            <groupId>org.openjfx</groupId>
            <artifactId>javafx-fxml</artifactId>
            <version>${javafx.version}</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>

            <plugin>
                <groupId>org.openjfx</groupId>
                <artifactId>javafx-maven-plugin</artifactId>
                <version>0.0.8</version>

                <configuration>
                    <mainClass>
                        com.daetl.cleaner.Main
                    </mainClass>
                </configuration>
            </plugin>

        </plugins>
    </build>

</project>
```

JavaFX 21 is available through Maven Central, and the OpenJFX Maven documentation shows the same general dependency/plugin approach. ([Maven Central][8])

**Important:** I used Java 21 here assuming your installed JDK is 21. If your JDK is 17, 22, etc., tell me the output of:

```powershell
java -version
```

before we finalize the `pom.xml`.

---

# 17. Add Kafka dependency

Later, add the Kafka client:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>4.0.0</version>
</dependency>
```

We'll choose the exact client version when we build the project.

You **do not need Kafka's Java installation** separately.

The Kafka broker is Docker.

Your application uses the Kafka Java client library through Maven.

---

# 18. Add PostgreSQL JDBC

Add:

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.7</version>
</dependency>
```

Your Java program will connect to:

```text
jdbc:postgresql://localhost:5432/data_cleaner
```

---

# 19. Add HikariCP

For connection pooling:

```xml
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>6.3.3</version>
</dependency>
```

This gives:

```text
Java application
       ↓
HikariCP
       ↓
PostgreSQL
```

rather than creating a new database connection for every batch.

---

# 20. Add logging

Use SLF4J + Logback:

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>2.0.17</version>
</dependency>

<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.5.18</version>
</dependency>
```

Then you'll be able to write:

```java
logger.info("Processing batch {}", batchNumber);
```

instead of:

```java
System.out.println(...)
```

---

# 21. Your final technology stack

You will have:

| Technology        | Where   | Purpose                |
| ----------------- | ------- | ---------------------- |
| Java              | Windows | Application            |
| Maven             | Windows | Build/dependencies     |
| JavaFX            | Maven   | GUI                    |
| Kafka             | Docker  | Streaming              |
| PostgreSQL        | Docker  | Storage                |
| Kafka Java Client | Maven   | Java → Kafka           |
| PostgreSQL JDBC   | Maven   | Java → PostgreSQL      |
| HikariCP          | Maven   | DB connection pool     |
| SLF4J             | Maven   | Logging                |
| Logback           | Maven   | Logging implementation |
| Git               | Windows | Version control        |
| Docker Desktop    | Windows | Infrastructure         |

---

# 22. Optional: Prometheus + Grafana

**Do not install these yet.**

They are useful later for:

```text
Kafka
   ↓
Application
   ↓
Metrics
   ↓
Prometheus
   ↓
Grafana
```

But you have one week.

Your first dashboard should be JavaFX.

You can add Prometheus/Grafana after the pipeline actually works.

---

# 23. Final verification checklist

Run these commands one by one.

### Java

```powershell
java -version
```

### Maven

```powershell
mvn -version
```

### Git

```powershell
git --version
```

### Docker

```powershell
docker --version
```

### Docker Compose

```powershell
docker compose version
```

### PostgreSQL

```powershell
docker exec data-cleaner-postgres psql -U postgres -d data_cleaner -c "SELECT 1;"
```

Expected:

```text
 ?column?
----------
        1
```

### Kafka

```powershell
docker exec data-cleaner-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Expected:

```text
raw_data
```

### Infrastructure

```powershell
docker compose ps
```

Expected:

```text
data-cleaner-kafka       Up
data-cleaner-postgres   Up
```

---

# Your actual development environment

After all this, **you don't need WSL at all**.

Your workflow will simply be:

```text
PowerShell
    │
    ├── mvn clean javafx:run
    │
    ├── git
    │
    └── docker compose
             │
             ├── Kafka :9092
             │
             └── PostgreSQL :5432
```

The only caveat is **Docker Desktop + Hyper-V requires a suitable Windows edition**. If you're on Windows 11 Home, stop before installing Docker and tell me your Windows edition; I would change the infrastructure approach rather than sneaking WSL into the setup. ([Docker Documentation][1])

### Official references

* [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/?utm_source=chatgpt.com)
* [Apache Kafka Docker documentation](https://kafka.apache.org/41/getting-started/docker/?utm_source=chatgpt.com)
* [Apache Maven installation](https://maven.apache.org/install?utm_source=chatgpt.com)
* [Git for Windows](https://gitforwindows.org/?utm_source=chatgpt.com)
* [OpenJFX Maven documentation](https://openjfx.io/openjfx-docs/maven?utm_source=chatgpt.com)
* [PostgreSQL official Docker image](https://hub.docker.com/_/postgres?utm_source=chatgpt.com)

[1]: https://docs.docker.com/desktop/setup/install/windows-install/?utm_source=chatgpt.com "Install Docker Desktop on Windows | Docker Docs"
[2]: https://learn.microsoft.com/en-us/windows/package-manager/winget/install?utm_source=chatgpt.com "`install` Command | Microsoft Learn"
[3]: https://gitforwindows.org/?utm_source=chatgpt.com "Git for Windows"
[4]: https://maven.apache.org/install?utm_source=chatgpt.com "Installation – Maven"
[5]: https://maven.apache.org/guides/getting-started/windows-prerequisites.html?utm_source=chatgpt.com "Maven on Windows – Maven"
[6]: https://hub.docker.com/_/postgres?utm_source=chatgpt.com "postgres - Official Image | Docker Hub"
[7]: https://kafka.apache.org/41/getting-started/docker/?utm_source=chatgpt.com "Docker | Apache Kafka"
[8]: https://central.sonatype.com/artifact/org.openjfx/javafx/21?utm_source=chatgpt.com "Maven Central: org.openjfx:javafx:21"
