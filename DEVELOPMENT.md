## Development

### Prerequisites

- Go 1.21 or later
- [goreman](https://github.com/mattn/goreman) for running multiple processes

### Building

```sh
go build -o antfly ./cmd/antfly/main.go
```

For FAISS vector index support:
```sh
goto https://github.com/blevesearch/go-faiss/blob/master/README.md
brew install cmake llvm gflags
CGO_LDFLAGS="-Wl,-rpath,/usr/local/lib -L/usr/local/lib" go test -tags=vectors ./lib/vectorindex/...
```

### Configuration

Create a `config.json` file:

```json
{
  "metadata_url": "http://localhost:12277",
  "replication_factor": 3,
  "max_shard_size_bytes": 67108864,
  "disable_shard_alloc": false
}
```

### Components

1. **Metadata Server**: Coordinates cluster operations, manages table metadata
2. **Storage Nodes**: Store data shards, handle queries and indexes
3. **Load Balancer/Reverse Proxy**: Routes requests to appropriate nodes
4. **CLI**: Command-line interface for cluster management
5. **MCP**: MCP server for Antfly
6. **Termite**: Caching layer for embeddings when testing

### Running

Antfly can run in several modes:

#### 1. Swarm Mode (Single-node for Development)

Swarm mode runs metadata, storage, and Termite nodes in a single process, perfect for development and testing:

```sh
go run ./cmd/antfly swarm
```

#### 2. Distributed Mode

You can also test a distributed setup locally using [goreman](https://github.com/mattn/goreman):

```sh
goreman -f Procfile start
```

## Releasing

```
export GITHUB_TOKEN=your_token_here
export AWS_ACCESS_KEY_ID=your_key_here
export AWS_SECRET_ACCESS_KEY=your_secret_here
go tool goreleaser release --clean
```

## Contributing

- `go get -u`
- `go test ./... | tee test.log`
- `go run github.com/Antonboom/testifylint@latest --fix ./...`
- `go run github.com/securego/gosec/v2/cmd/gosec@latest ./...`
- `go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run --fix ./...`
- `go mod tidy && go mod verify`
- `go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...`
