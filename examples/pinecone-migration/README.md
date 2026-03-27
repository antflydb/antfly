# Pinecone to Antfly Migration

Migrate your vector embeddings from Pinecone to Antfly.

This example also covers the Antfly-side pattern for other systems that already
have vectors, such as Milvus, Weaviate, pgvector exports, or custom offline
embedding pipelines. Only the fetch step in `main.py` is Pinecone-specific.

## Use Cases

- **Cost reduction**: Move from cloud-based Pinecone to local Antfly
- **Local development**: Run your vector search locally without API calls
- **Sunsetting Pinecone**: Full migration with preserved embeddings

## Prerequisites

- A running Antfly server (`antfly swarm`)
- Python 3.9+
- Your Pinecone API key

```bash
pip install -r requirements.txt
```

## Vector Compatibility

Your source index stores embeddings at a specific **dimension** and uses a
specific **distance metric**. To preserve search behavior in Antfly, create an
external embeddings index with the same dimension and metric.

### Check Your Pinecone Dimension

In the Pinecone dashboard, find your index and note its dimension. Or via API:

```python
from pinecone import Pinecone
pc = Pinecone(api_key="...")
stats = pc.Index("your-index").describe_index_stats()
print(f"Dimension: {stats['dimension']}")
```

### What must match

- `dimension`: Vector length (e.g. 768, 1024, 1536)
- `distance_metric`: Usually `cosine`, `inner_product`, or `l2_squared`

This example creates an Antfly embeddings index with:
- `external: true`
- the source vector `dimension`
- the configured `distance_metric`

No `embedder`, `field`, or `template` is used for this migration index, because
the vectors are already computed upstream.

## Schemaless Storage

Antfly tables are **schemaless**. When migrating from Pinecone:

- Each vector's **metadata** becomes top-level document fields
- The **id** becomes the document key
- Pre-computed embeddings go in the `_embeddings` field for an `external: true` index

Example:
```
Pinecone:                          Antfly:
  id: "doc-123"                      _id: "doc-123"
  values: [0.1, 0.2, ...]    →       text: "Hello"
  metadata: {text: "Hello"}          _embeddings: {nomic_index: [0.1, ...]}
```

## Configuration

Update these values in `main.py` for your migration:

<!-- include: main.py#config -->

## Running the Migration

```bash
export PINECONE_API_KEY="your-key-here"
python main.py
```

## How It Works

### 1. Initialize Clients

<!-- include: main.py#init_clients -->

### 2. Create Table

Tables are schemaless - no schema definition needed:

<!-- include: main.py#create_table -->

### 3. Create Vector Index

The index is created before inserting data so Antfly knows where to store imported vectors.
This is an **external** index, meaning Antfly will accept vectors written through
`_embeddings` and will not try to generate them from document fields:

<!-- include: main.py#create_index -->

### 4. Fetch Vectors from Pinecone

Uses a zero-vector query with high `top_k` - a common pattern since Pinecone doesn't have a "list all" API:

<!-- include: main.py#fetch_vectors -->

### 5. Upsert to Antfly

Vectors are batch-upserted with the `_embeddings` field:

<!-- include: main.py#upsert_vectors -->

### 6. Verify

<!-- include: main.py#verify -->

The verification query uses one of the imported vectors directly via the query
`embeddings` field. This keeps the example consistent with `external: true`
indexes, which do not have an embedder for text-to-vector conversion.

## Troubleshooting

### "manual _embeddings are not allowed"

The Antfly index was probably created as a managed index. For imported vectors,
create the index with `external: true` and do not set `field`, `template`, or
`embedder`.

### "Hit 1000 limit"

Increase `top_k` in `fetch_all_vectors()` or implement pagination for larger datasets.

### Adapting this example for Milvus / Weaviate / pgvector

Reuse the same Antfly-side steps:
- create the table
- create an `external: true` embeddings index
- write vectors via `_embeddings`
- verify with a query-time `embeddings` vector

Only the export/fetch step changes based on your source system.
