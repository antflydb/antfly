# Antfly

Antfly is a distributed key-value store and vector search engine built on [etcd's raft library](https://github.com/etcd-io/raft). It provides a REST API for managing key-value tables, their indexes, and performing hybrid search operations against these full-text and vector similarity indexes.

[![Build status](https://github.com/antflydb/antfly/actions/workflows/antfly-go.yml/badge.svg)](https://github.com/antflydb/antfly/actions)

![Quickstart](https://cdn.antfly.io/quickstart.gif)

## Features

- **Distributed Architecture**: Built on Raft consensus for high availability and fault tolerance
- **Hybrid Search**: Combines full-text search (BM25) with vector similarity search
- **Multiple Deployment Modes**: Run as separate metadata/storage nodes or in swarm mode
- **Vector Indexes**: Support for multiple embedding models and vector search
- **Schema Support**: Define table schemas with typed fields
- **Backup & Restore**: Built-in backup and restore functionality
- **Horizontal Scaling**: Automatic sharding and replication
- **Multimodal Support**: Handle images, audio, and video data through summarization or multimodal embedding models
- **Extensible Embedding Models**: Ollama, OpenAI, Bedrock, Google, etc.
- **CLI Tool**: Command-line interface for managing clusters and data
- **Kubernetes Operator**: Deploy and manage Antfly clusters in Kubernetes
- **Observability**: Metrics and logging for monitoring cluster health and performance
- **MCP Server**: Easy integration with LLMs

## Documentation

Full documentation is available at [https://docs.antfly.io](https://antfly.io/docs).

## Architecture

Antfly uses a multi-raft architecture with separate consensus groups:

- **Metadata Raft Group**: Manages table schemas, shard assignments, and cluster topology
- **Storage Raft Groups**: One per shard, handles data storage and replication

## License

Elastic License 2.0 (ELv2) - See [LICENSE](LICENSE) file for details.

---
