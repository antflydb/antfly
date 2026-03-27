"""
Pinecone to Antfly Migration Script
====================================

Migrates pre-computed vectors from Pinecone to Antfly.

The Antfly-side pattern in this example also applies to vector exports from
Milvus, Weaviate, pgvector, and other systems where you already have vectors.
Only the fetch step in this script is Pinecone-specific.

Usage:
    python main.py

Environment Variables:
    PINECONE_API_KEY  - Required. Your Pinecone API key.
    ANTFLY_BASE_URL   - Optional. Defaults to http://localhost:8080/api/v1
"""

from dotenv import load_dotenv
import os
import sys
from pinecone import Pinecone
import time
import httpx

from antfly import AntflyClient
from antfly.exceptions import AntflyException

# ANCHOR: config
# --- Configuration ---
# Customize these values for your migration
PINECONE_INDEX_NAME = "my-pinecone-index" # Your Pinecone index name
TABLE_NAME = PINECONE_INDEX_NAME          # Antfly table name (using same name)
INDEX_NAME = "nomic_index"                # Name for the vector index in Antfly
DISTANCE_METRIC = "cosine"                # Match the metric used by your source index

ANTFLY_BASE_URL = os.getenv("ANTFLY_BASE_URL", "http://localhost:8080/api/v1")
# ANCHOR_END: config


# ANCHOR: init_clients
def main():
    load_dotenv()

    # --- Initialize Pinecone ---
    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    if not pinecone_api_key:
        print("FATAL: PINECONE_API_KEY not found in environment.", file=sys.stderr)
        sys.exit(1)

    pc = Pinecone(api_key=pinecone_api_key)
    pinecone_index = pc.Index(PINECONE_INDEX_NAME)
    print(f"Connected to Pinecone index '{PINECONE_INDEX_NAME}'.")

    # --- Initialize Antfly ---
    client = AntflyClient(base_url=ANTFLY_BASE_URL)
    try:
        print(f"Connecting to Antfly at {ANTFLY_BASE_URL}...")
        client.list_tables()
        print("Connected to Antfly.")
    except Exception as e:
        print(f"Could not connect to Antfly: {e}")
        print("Ensure the Antfly server is running.")
        sys.exit(1)
# ANCHOR_END: init_clients

    # ANCHOR: create_table
    # --- Create Antfly table ---
    # Tables are schemaless - no schema definition required
    try:
        client.drop_table(TABLE_NAME)
        print(f"Deleted existing table '{TABLE_NAME}'")
    except AntflyException:
        pass

    try:
        client.create_table(name=TABLE_NAME)
        print(f"Created table '{TABLE_NAME}'")
    except Exception as e:
        print(f"Error creating table: {e}")
        sys.exit(1)

    print("Waiting for table shards to initialize...")
    time.sleep(5)
    # ANCHOR_END: create_table

    # ANCHOR: fetch_stats
    # --- Get Pinecone index information ---
    print("Fetching index statistics...")
    try:
        stats = pinecone_index.describe_index_stats()
        namespaces = list(stats.get('namespaces', {}).keys())
        total_vectors = stats.get('total_vector_count', 'N/A')
        dimension = stats['dimension']
        print(f"  -> Found {len(namespaces)} namespaces, {total_vectors} vectors, dimension {dimension}")

        if not namespaces:
            print("No namespaces found. Nothing to migrate.", file=sys.stderr)
            sys.exit(0)
    except Exception as e:
        print(f"FATAL: Could not fetch index stats: {e}", file=sys.stderr)
        sys.exit(1)
    # ANCHOR_END: fetch_stats

    # ANCHOR: create_index
    # --- Create vector index BEFORE inserting data ---
    # This tells Antfly where to store pre-computed embeddings
    try:
        print(f"\nCreating vector index '{INDEX_NAME}' with dimension {dimension}...")
        index_config = {
            "name": INDEX_NAME,
            "type": "embeddings",
            "external": True,
            "dimension": dimension,
            "distance_metric": DISTANCE_METRIC,
        }
        print(f"  -> Creating external index for imported vectors (metric: {DISTANCE_METRIC})")

        resp = httpx.post(
            f"{ANTFLY_BASE_URL}/tables/{TABLE_NAME}/indexes/{INDEX_NAME}",
            json=index_config,
            timeout=30.0
        )
        if resp.status_code in (200, 201):
            print("Index created successfully.")
        else:
            print(f"Index creation response: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"Error creating index: {e}")

    print("Waiting for index to propagate...")
    time.sleep(5)
    # ANCHOR_END: create_index

    # ANCHOR: fetch_vectors
    def fetch_all_vectors(pc_index, namespace: str, dim: int):
        """
        Fetch all vectors from a Pinecone namespace.

        Uses a zero-vector query with high top_k - a common pattern
        since Pinecone doesn't have a direct "list all" API.
        """
        print(f"Fetching vectors from namespace: '{namespace}'...")

        # Query with zero vector to get all vectors
        query_response = pc_index.query(
            namespace=namespace,
            vector=[0.0] * dim,
            top_k=1000,
            include_values=True,
            include_metadata=True
        )

        vectors = query_response.get('matches', [])
        if len(vectors) == 1000:
            print("  WARNING: Hit 1000 limit - may have more vectors.", file=sys.stderr)

        print(f"  -> Found {len(vectors)} vectors.")
        return vectors

    # Fetch from all namespaces
    all_vectors = {}
    for namespace in namespaces:
        vectors = fetch_all_vectors(pinecone_index, namespace, dimension)

        if not vectors:
            continue

        all_vectors[namespace] = []
        for v in vectors:
            # Flatten: metadata becomes top-level fields
            entry = {
                'id': v['id'],
                'values': v['values'],
                **v.get('metadata', {}),
                'namespace': namespace
            }
            all_vectors[namespace].append(entry)
    # ANCHOR_END: fetch_vectors

    # ANCHOR: upsert_vectors
    # --- Upsert into Antfly ---
    for namespace, items in all_vectors.items():
        try:
            print(f"Upserting {len(items)} vectors for namespace '{namespace}'...")

            inserts = {}
            for item in items:
                key = item['id']
                properties = {k: v for k, v in item.items() if k != 'id'}

                # Use _embeddings format for pre-computed embeddings
                # Format: {"_embeddings": {"<index_name>": [vector values]}}
                if 'values' in properties:
                    embedding_vector = properties.pop('values')
                    properties['_embeddings'] = {INDEX_NAME: embedding_vector}

                inserts[key] = properties

            resp = httpx.post(
                f"{ANTFLY_BASE_URL}/tables/{TABLE_NAME}/batch",
                json={"inserts": inserts},
                timeout=60.0
            )
            if resp.status_code in (200, 201):
                print(f"  -> Successfully upserted {len(items)} vectors.")
            else:
                print(f"  -> Batch failed: {resp.status_code} - {resp.text}")

        except Exception as e:
            print(f"  -> ERROR: {e}")
            break

    print("\nMigration complete. Waiting for indexing...")
    time.sleep(10)
    # ANCHOR_END: upsert_vectors

    # ANCHOR: verify
    # --- Verify migration ---
    print("\n=== Verifying Migration ===")

    try:
        resp = httpx.post(
            f"{ANTFLY_BASE_URL}/tables/{TABLE_NAME}/query",
            json={"limit": 200},
            timeout=30.0
        )
        if resp.status_code == 200:
            data = resp.json()
            hits = data.get('responses', [{}])[0].get('hits', {}).get('hits', [])
            print(f"Total records in Antfly: {len(hits)}")

            if hits:
                print("\nSample records:")
                for hit in hits[:3]:
                    key = hit.get('_id', 'unknown')
                    source = hit.get('_source', {})
                    text = source.get('text', '')[:80] if source else ''
                    print(f"  - {key}: {text}...")
    except Exception as e:
        print(f"Verification failed: {e}")

    # --- Test vector search using one of the imported vectors ---
    sample_key = None
    sample_vector = None
    for items in all_vectors.values():
        if items:
            sample_key = items[0]['id']
            sample_vector = items[0]['values']
            break

    if sample_vector is not None:
        print(f"\nTesting vector search using imported vector from key '{sample_key}'...")
        try:
            resp = httpx.post(
                f"{ANTFLY_BASE_URL}/tables/{TABLE_NAME}/query",
                json={"embeddings": {INDEX_NAME: sample_vector}, "limit": 3},
                timeout=30.0
            )
            if resp.status_code == 200:
                data = resp.json()
                hits = data.get('responses', [{}])[0].get('hits', {}).get('hits', [])
                if hits:
                    print("Results:")
                    for hit in hits:
                        score = hit.get('_score', 0)
                        key = hit.get('_id', 'unknown')
                        print(f"  [{score:.4f}] {key}")
                else:
                    print("  (no results)")
            else:
                print(f"  Search failed: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"  Search failed: {e}")
    else:
        print("\nSkipping verification search because no vectors were imported.")

    print("\n=== Done ===")
    print(f"Table: {TABLE_NAME}")
    print(f"Index: {INDEX_NAME} (dimension: {dimension}, metric: {DISTANCE_METRIC})")
    # ANCHOR_END: verify


if __name__ == "__main__":
    main()
