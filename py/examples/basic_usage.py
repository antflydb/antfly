#!/usr/bin/env python3
"""Basic usage example for Antfly SDK."""

from typing import cast
from antfly import AntflyClient
from antfly.client_generated.types import Unset


def main():
    """Demonstrate basic Antfly SDK usage."""
    
    # Initialize client
    client = AntflyClient(
        base_url="http://localhost:8080",
        username="admin",
        password="password"
    )
    
    # List existing tables
    print("Existing tables:")
    tables = client.list_tables()
    for table in tables:
        print(f"  - {table.name}")
    
    # Create a new table
    print("\nCreating 'products' table...")
    client.create_table(
        name="products",
        num_shards=2,
        schema={
            "key": "product_id",
            "document_types": {
                "product": {
                    "fields": {
                        "name": {"type": "string"},
                        "description": {"type": "string"},
                        "price": {"type": "float"},
                        "category": {"type": "keyword"},
                        "in_stock": {"type": "bool"}
                    }
                }
            }
        }
    )
    
    # Insert some products
    print("Inserting products...")
    client.batch(
        table="products",
        inserts={
            "prod:001": {
                "name": "Laptop",
                "description": "High-performance laptop for professionals",
                "price": 1299.99,
                "category": "electronics",
                "in_stock": True
            },
            "prod:002": {
                "name": "Wireless Mouse",
                "description": "Ergonomic wireless mouse with long battery life",
                "price": 29.99,
                "category": "accessories",
                "in_stock": True
            },
            "prod:003": {
                "name": "USB-C Cable",
                "description": "Fast charging USB-C cable, 2 meters",
                "price": 19.99,
                "category": "accessories",
                "in_stock": False
            }
        }
    )
    
    # Query products
    print("\nSearching for 'wireless' products...")
    results = client.query(
        table="products",
        full_text_search={"query": "wireless"},
        limit=5
    )
    
    if not isinstance(results.responses, Unset):
        result = results.responses[0]
        if result.hits and result.hits.hits:
            for hit in result.hits.hits:
                if not isinstance(hit.field_source, Unset):
                    source = cast(dict, hit.field_source)
                    print(f"  Found: {source.get('name')} - ${source.get('price')}")
    
    # Get specific product
    print("\nFetching product 'prod:001'...")
    product = client.get(table="products", key="prod:001")
    print(f"  {product.get('name')}: {product.get('description')}")
    print(f"  Price: ${product.get('price')}")
    
    # Clean up (optional)
    print("\nDropping 'products' table...")
    client.drop_table("products")
    print("Done!")


if __name__ == "__main__":
    main()
