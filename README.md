# HandyQuorumDB

HandyQuorumDB is a distributed key-value store designed for improved availability and durability guarantees. It incorporates features inpired by [Amazon's DynamoDB](https://dl.acm.org/doi/abs/10.1145/1323293.1294281) such as sloppy quorum and hinted handoff, to enhance robustness and scalability.

It was developed by Marcin Praski and Me for the "Cloud Database Systems" course at TUM.

- LSM Tree Storage Layer: Implements a data storage layer with configurable compaction intervals using memtable and sstable.
- Sloppy Quorum: Allows operations to proceed with a subset of nodes responding, providing flexibility compared to traditional quorum systems.
- Hinted Handoff: Handles temporary failures by storing hints about missed updates and replaying them once the node becomes available again.

## Project Structure
Here is a brief overview of the structure:
```
/
├── cmd/             -- Contains executable files
│   ├── benchmark/
│   ├── client/
│   ├── ecs/
│   ├── quorum-client/
│   └── server/
├── ecs/             -- External Configuration Service (ECS)
├── protocol/        -- TCP-based communication protocols
├── server/
│   ├── data/
│   ├── ecs/         -- Contains state machine for the synchronization with ECS
│   ├── log/         -- Append-only log
│   ├── memtable/    -- Memtable for LSM tree (in-memory data structure)
│   ├── replication/ -- Replication manager to handle scheduling & reconcilitation for replicas
│   ├── sstable/     -- SSTable for LSM tree (on-disk data structure)
│   ├── store/
│   ├── util/
│   └── web/         -- TCP server for the database
└── tcp/
```

## Building

Setup the Go toolchain and install `golangci-lint` (See [instructions](https://golangci-lint.run/usage/install/)).

### Compiling
```
go get ./...
make
```

### Deploy
In order to deploy locally, use the Makefile's `run-client`, `run-server1` etc. commands.
