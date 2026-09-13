# Antfly Work Log

This directory tracks major features and architectural changes in Antfly. Each document preserves design decisions and implementation context for future reference.

Go-era design documents whose content now lives in the Zig design docs were removed from this directory; see [`zig/ROADMAP.md`](../zig/ROADMAP.md) as the index for those.

## Completed Features

### Ingestion

| Feature | Document | Summary |
|---------|----------|---------|
| DOCX/PPTX & Google Docs/Slides Support | [ppt-docx.md](completed/ingestion/ppt-docx.md) | Structured extraction for Office and Google Workspace document formats in docsaf, using only the standard library |
| Reader Interface (OCR/Vision) | [reader-integration.md](completed/ingestion/reader-integration.md) | A reusable `Reader` interface for OCR/vision integrations, replacing ad-hoc per-app implementations |

## Planned Features

| Feature | Document | Summary |
|---------|----------|---------|
| Agentic Warehouse Memory | [agentic-warehouse-memory.md](planned/agentic-warehouse-memory.md) | Antfly as an agentic memory layer over BigQuery/Snowflake warehouses |
| Operator Standalone Mode | [operator-standalone-mode.md](planned/operator-standalone-mode.md) | Explicit operator-managed standalone mode for `AntflyCluster`, without breaking the existing clustered topology |
| Pipelined Query API | [pipelined-query-api.md](planned/pipelined-query-api.md) | Multi-stage query pipelines supporting delete-by-query, update-by-query, and cross-table joins |
| Query Sampler Feature | [query-samplers.md](planned/query-samplers.md) | Named query samplers that capture query embeddings and results for ML training |

## Quick Links

- **Completed Features**: [completed/](completed/)
- **Planned Features**: [planned/](planned/)
- **Main Documentation**: [../CLAUDE.md](../CLAUDE.md)
- **API Specification**: [../specs/openapi/](../specs/openapi/)
