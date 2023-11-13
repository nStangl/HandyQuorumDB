# Repository Overview

The system implements sloppy quorum and hinted handoff inspired by ([Amazon's dynamo database](https://dl.acm.org/doi/abs/10.1145/1323293.1294281)).
It can handle temporary failures and provides improved availability and durability guarantees.

## Project structure
```
/
├── cmd/             -- where we keep the executable files
│   ├── benchmark/
│   ├── client/
│   ├── ecs/
│   ├── quorum-client/
│   └── server/
├── ecs/             -- our External Configuration Service (ECS)
├── protocol/        -- TCP based communication protocols
├── server/
│   ├── data/
│   ├── ecs/         -- contains state machine for the synchronizatoin with ECS
│   ├── log/         -- append-only log
│   ├── memtable/    -- memtable for LSM tree (in-memory data structure)
│   ├── replication/ -- replication manager to handle scheduling & reconcilitation for replicas
│   ├── sstable/     -- sstable for LSM tree (on-disk data structure)
│   ├── store/
│   ├── util/
│   └── web/         -- TCP server for the database
└── tcp/
```

## Building

Make sure to install the Go toolchain and set the Go path.
Also install `golangci-lint` (See [instructions](https://golangci-lint.run/usage/install/)).
```
go get ./...
make
```