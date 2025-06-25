# Distributed File Storage System

A lightweight, scalable, and fault-tolerant distributed file storage system built in Java.

This project simulates a cloud-like storage architecture with the following components:

* A central **Controller** for orchestration and metadata management
* Multiple **Dstores** for file storage
* A CLI-based **Client** (provided separately) for interacting with the system

> All components run locally and communicate over TCP. The design simulates a real-world, multi-node distributed environment.

## Features

* File replication with configurable replication factor
* Concurrent client support
* Dynamic Dstore joining and automatic rebalancing
* TCP-based communication protocol
* Graceful handling of Dstore failures
* Custom indexing and file state management
* Minimal external dependencies (pure Java SE)

## Getting Started

### 1. Build the Project

```bash
javac *.java
```

### 2. Launch Components

#### Start the Controller

```bash
java Controller <controller_port> <replication_factor> <timeout_ms> <rebalance_interval_sec>
```

#### Start Each Dstore

```bash
java Dstore <dstore_port> <controller_port> <timeout_ms> <data_folder>
```

> Each Dstore must use a unique port and its own file directory (ensure the folder already exists).

#### Start the Client

```bash
java -jar client.jar <controller_port> <timeout_ms>
```

## Example Usage

```bash
# Terminal 1 – Controller
java Controller 5000 2 3000 20

# Terminal 2 – Dstore 1
java Dstore 6001 5000 3000 ./dstore1/

# Terminal 3 – Dstore 2
java Dstore 6002 5000 3000 ./dstore2/

# Terminal 4 – Client
java -jar client.jar 5000 3000
```

## Supported Operations

### STORE

1. Client sends `STORE filename filesize` to Controller
2. Controller replies with `STORE_TO` and ports of Dstores
3. Client sends file to each Dstore
4. Dstores send `STORE_ACK` to Controller
5. Controller replies with `STORE_COMPLETE`

### LOAD

1. Client sends `LOAD filename` to Controller
2. Controller replies with `LOAD_FROM port filesize`
3. Client requests file from one Dstore

If the Dstore fails:

* Client sends `RELOAD filename`
* Controller provides a new Dstore
* If all fail: `ERROR_LOAD`

### REMOVE

1. Client sends `REMOVE filename`
2. Controller sends `REMOVE filename` to Dstores
3. Dstores respond with `REMOVE_ACK`
4. Controller confirms with `REMOVE_COMPLETE`

### LIST

* Client sends `LIST`
* Controller responds with space-separated list of stored files

## Rebalance

Triggered:

* Periodically (via rebalance interval)
* When a new Dstore joins

Steps:

1. Controller requests file lists from Dstores
2. Computes new assignments to maintain:

   * Replication factor `R`
   * Even distribution across Dstores
3. Sends `REBALANCE` command with files to move and remove
4. Dstores transfer files and send `REBALANCE_COMPLETE`

## Design Highlights

* Thread-safe Controller index with file states:
  `store in progress`, `store complete`, `remove in progress`
* Files hidden during incomplete operations
* Rebalance defers client requests until completion
* No state saved to disk — fresh startup every run

## Project Structure

```
.
├── Controller.java
├── Dstore.java
├── (other helper Java files)
├── client.jar   # Provided externally
└── README.md
```

## Configuration

| Component  | Parameter          | Description                         |
| ---------- | ------------------ | ----------------------------------- |
| Controller | `cport`            | Port to listen for clients          |
|            | `R`                | Replication factor                  |
|            | `timeout`          | Timeout for network operations (ms) |
|            | `rebalance_period` | Seconds between rebalances          |
| Dstore     | `port`             | Port to listen on                   |
|            | `cport`            | Controller's port                   |
|            | `timeout`          | Timeout in ms                       |
|            | `file_folder`      | Directory to store files            |
| Client     | `cport`            | Controller port                     |
|            | `timeout`          | Timeout in ms                       |

## Failure Handling

* Missing or malformed messages are ignored
* STORE timeout → file removed from index
* LOAD retries other replicas if one Dstore fails
* REMOVE timeout → file remains in “remove in progress”
* Dstore crash → removed from active set

## Future Enhancements

* Persistent metadata storage for Controller
* TLS for secure communication
* Docker-based deployment
* REST API for external integrations
* Web dashboard for monitoring
