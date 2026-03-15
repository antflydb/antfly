# Pinecone to Antfly Migration

Migrate your vector embeddings from Pinecone to Antfly.

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

## Embedding Model Compatibility

Your Pinecone index stores embeddings at a specific **dimension** (e.g., 768 for `nomic-embed-text`). To enable semantic search in Antfly, configure an embedder that produces vectors of the **same dimension**.

### Check Your Pinecone Dimension

In the Pinecone dashboard, find your index and note its dimension. Or via API:

```python
from pinecone import Pinecone
pc = Pinecone(api_key="...")
stats = pc.Index("your-index").describe_index_stats()
print(f"Dimension: {stats['dimension']}")
```

### Common Models and Dimensions

| Model | Dimension | Provider |
|-------|-----------|----------|
| nomic-embed-text | 768 | Ollama |
| mxbai-embed-large | 1024 | Ollama |
| text-embedding-3-small | 1536 | OpenAI |
| all-minilm | 384 | Ollama |

## Schemaless Storage

Antfly tables are **schemaless**. When migrating from Pinecone:

- Each vector's **metadata** becomes top-level document fields
- The **id** becomes the document key
- Pre-computed embeddings go in the `_embeddings` field

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

The index is created before inserting data so Antfly knows where to store embeddings:

<!-- include: main.py#create_index -->

### 4. Fetch Vectors from Pinecone

Uses a zero-vector query with high `top_k` - a common pattern since Pinecone doesn't have a "list all" API:

<!-- include: main.py#fetch_vectors -->

### 5. Upsert to Antfly

Vectors are batch-upserted with the `_embeddings` field:

<!-- include: main.py#upsert_vectors -->

### 6. Verify

<!-- include: main.py#verify -->

## Troubleshooting

### "No matching embedder"

Install the matching model. For Ollama:
```bash
ollama pull nomic-embed-text
```

### "Hit 1000 limit"

Increase `top_k` in `fetch_all_vectors()` or implement pagination for larger datasets.
