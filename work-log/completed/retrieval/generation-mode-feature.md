# Generation Mode Feature - Work Log

## Date: 2024-12-04

## Summary
Added `without_generation` boolean field to Answer Agent API to skip AI answer generation and return search results only.

## API Changes Implemented

### Request Schema (`AnswerAgentRequest`)
- Added `without_generation` boolean field (default: `false`)
  - `false` (default): Run classification, retrieval, and answer generation
  - `true`: Skip AI answer generation, return search results only

### Response Schema (`AnswerAgentResult`)
- No changes - when `without_generation: true`, the response simply contains `query_results` without an `answer`

### SSE Events (Streaming Mode)
- When `without_generation: true`, streams `hits_start`, `hit`, `hits_end`, and `done` events (no answer-related events)

## TypeScript Types (ts/)
After running `make generate`, the TypeScript client types are auto-generated:
```typescript
// Updated request interface
export interface AnswerAgentRequest {
  // ... existing fields
  without_generation?: boolean;
}
```

## Proxy Service Integration

For graceful degradation at the proxy level:

1. **Proxy intercepts** `/agents/answer` requests
2. **Check account quota** before forwarding
3. If over quota: Forward with `without_generation: true` added to request
4. If within quota: Forward request as-is

Example proxy transformation:
```typescript
// Proxy middleware
async function checkQuotaMiddleware(req, res, next) {
  const accountQuota = await getAccountQuota(req.accountId);

  if (accountQuota.exceeded) {
    req.body.without_generation = true;
  }

  next();
}
```

## Files Modified

- `src/metadata/api.yaml` - OpenAPI schema
- `src/metadata/api.gen.go` - Generated Go types
- `src/metadata/api_ai.go` - Handler implementation

## Testing Notes

To test the new functionality:

```bash
# Test without_generation mode
curl -X POST http://localhost:8080/agents/answer \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is machine learning?",
    "without_generation": true,
    "queries": [{"table": "docs", "limit": 10}]
  }'

# Expected response includes:
# "query_results": [...]
# "answer": "" (empty or omitted)
```
