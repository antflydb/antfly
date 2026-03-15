# Metadata Server Port Consolidation

## Status

✅ **Implemented** - Consolidated from 3 servers to 2 servers

## Context

The metadata server originally ran **three separate HTTP servers**:

1. **Raft Transport Server** - Peer-to-peer Raft communication (conf.RaftURL)
2. **Internal API Server** - Management endpoints for store registration, peer changes, KV batch operations (conf.ApiURL)
3. **Public API Server** - User-facing table/query/RAG endpoints (leaderPublicApiURL)

### Problems

- **Port management complexity**: Three listening ports to configure and manage
- **Synchronization overhead**: Two channels (`internalAPIReady`, `publicAPIReady`) to coordinate startup
- **Configuration confusion**: Storage nodes needed separate `orchestration_urls` pointing to internal API
- **Deployment complexity**: Three ports to expose, firewall rules for each

## Decision

**Consolidate to two HTTP servers**:

1. **Raft Transport Server** (UNCHANGED)
   - Remains on separate port (conf.RaftURL)
   - Security requirement: peer-only network access
   - Handles Raft message passing and snapshots

2. **Combined API Server** (NEW)
   - Single port (leaderPublicApiURL)
   - Public endpoints: `/api/v1/*`
   - Internal endpoints: `/_internal/v1/*`
   - Frontend assets: `/antfarm/*`

### Internal Endpoints on `/_internal/v1/`

- `POST /store` - Store node registration
- `DELETE /store/{store}` - Store node deregistration
- `POST /peer/{peer}` - Add peer to metadata Raft group
- `DELETE /peer/{peer}` - Remove peer from metadata Raft group
- `POST /batch` - Batch write to metadata KV store
- `POST /reallocate` - Trigger shard reallocation (debug/admin)

### Architecture Diagram

```
┌──────────────────────────────────────────┐
│  Raft Transport Server (SEPARATE)        │
│  Port: conf.RaftURL (e.g. :9000)         │
│  Purpose: Raft peer communication only   │
└──────────────────────────────────────────┘

┌──────────────────────────────────────────┐
│  Combined API Server (NEW)               │
│  Port: leaderPublicApiURL (e.g. :8080)   │
├──────────────────────────────────────────┤
│  /api/v1/*                               │ → Public API
│  /_internal/v1/*                         │ → Internal API
│  /antfarm/*                              │ → Frontend
└──────────────────────────────────────────┘
```

## Implementation

### Combined Server Structure

```go
// In src/metadata/runner.go
func (ln *LeaderNode) RunAsMetadataServer(...) error {
    // 1. Raft transport stays separate (unchanged)
    go rs.Start()

    // 2. Combined API server (replaces both old servers)
    eg.Go(func() error {
        // Internal API routes
        internalMux := http.NewServeMux()
        internalMux.HandleFunc("POST /store", ln.handleStoreRegistration)
        internalMux.HandleFunc("DELETE /store/{store}", ln.handleStoreDeregistration)
        internalMux.HandleFunc("POST /peer/{peer}", api.HandleConfChange)
        internalMux.HandleFunc("DELETE /peer/{peer}", api.HandleConfChange)
        internalMux.HandleFunc("POST /batch", api.HandleBatch)
        internalMux.HandleFunc("POST /reallocate", ln.handleReallocateShards)

        // Public API routes
        publicMux := ln.publicApiRoutes()

        // Combined router
        apiRoutes := http.NewServeMux()
        apiRoutes.Handle("/api/v1/", http.StripPrefix("/api/v1", publicMux))
        apiRoutes.Handle("/_internal/v1/", http.StripPrefix("/_internal/v1", internalMux))
        addAntfarmRoutes(apiRoutes)

        // Single HTTP server
        srv := http.Server{
            Addr:    u.Host,
            Handler: corsMiddleware(apiRoutes),
        }

        // Single readyC signal
        if readyC != nil {
            close(readyC)
        }

        return srv.Serve(listener)
    })
}
```

### Security Consideration

Internal endpoints are protected by middleware checking for shared secret header:

```go
func internalOnlyMiddleware(config *common.Config) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            if strings.HasPrefix(r.URL.Path, "/_internal/") {
                if r.Header.Get("X-Internal-Secret") != config.InternalSecret {
                    http.Error(w, "Forbidden", http.StatusForbidden)
                    return
                }
            }
            next.ServeHTTP(w, r)
        })
    }
}
```

## Consequences

### Benefits

- ✅ **Simpler deployment**: Single public-facing port instead of two
- ✅ **Reduced coordination**: One readyC channel instead of two
- ✅ **Cleaner configuration**: Storage nodes use same base URL with `/_internal/v1/` prefix
- ✅ **Easier testing**: Single base URL for all API endpoints
- ✅ **Consistent middleware**: CORS, auth, logging apply uniformly
- ✅ **Fewer port conflicts**: Easier local development with goreman
- ✅ **Clearer separation**: Raft transport (peer-only) vs API operations (public + internal)

### Trade-offs

- ⚠️ **Security exposure**: Internal endpoints now on public-facing port (mitigated by auth middleware)
- ⚠️ **URL changes**: Storage node registration endpoint moved (breaking change)

### Breaking Changes

**For Storage Nodes**:
```diff
# Before
-orchestration_urls:
-  b: "http://127.0.0.1:9001"  # Separate internal API port

# After
+orchestration_urls:
+  b: "http://127.0.0.1:8080/_internal/v1"  # Includes prefix on public port
```

**For Deployment**:
- Only expose single port (8080) instead of two ports (8080 + 9001)
- Update firewall rules to remove internal API port
- Update service discovery configurations

### Migration Strategy

**Big Bang Deployment** (recommended for small clusters):
1. Stop all nodes (metadata + storage)
2. Update all binaries with new endpoint handling
3. Update all configs with new `orchestration_urls`
4. Restart metadata nodes first
5. Restart storage nodes (register to new endpoint)

## Verification

After implementation, verify:
- ✅ Metadata server starts on single port
- ✅ Storage nodes register via `/_internal/v1/store`
- ✅ Public API endpoints work at `/api/v1/*`
- ✅ Antfarm UI loads correctly
- ✅ Graceful shutdown works
- ✅ No port conflicts in local development
- ✅ Integration tests pass

## References

- Implementation: `src/metadata/runner.go` (RunAsMetadataServer)
- Metadata KV API: `src/metadatakv/store.go` (MetadataStoreAPI)
- Store registration: `src/store/runner.go` (registerWithLeader)
- Raft transport: `src/raft/multiraft.go` (Start method)
