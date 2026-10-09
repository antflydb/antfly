# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Print a native HN table definition for managed or external REST authority."""

import argparse
import json

from ingest import arrow_schema


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse", required=True, help="Dedicated s3:// or gs:// table root"
    )
    parser.add_argument("--source-connection", required=True)
    parser.add_argument("--mode", choices=("managed", "rest"), required=True)
    parser.add_argument("--rest-connection")
    parser.add_argument("--rest-uri")
    parser.add_argument("--rest-namespace", nargs="+", default=["hackernews"])
    parser.add_argument("--rest-name", default="items")
    parser.add_argument("--rest-warehouse")
    args = parser.parse_args()
    if not args.warehouse.startswith(("gs://", "s3://")):
        parser.error("use an object-store table root")
    if args.mode == "rest" and not (args.rest_connection and args.rest_uri):
        parser.error("REST requires --rest-connection and --rest-uri")
    catalog = {"type": args.mode}
    if args.mode == "rest":
        catalog.update(
            connection=args.rest_connection,
            uri=args.rest_uri,
            namespace=args.rest_namespace,
            name=args.rest_name,
        )
        if args.rest_warehouse:
            catalog["warehouse"] = args.rest_warehouse
    import pyarrow as pa

    properties = {
        field.name: {"type": "integer" if pa.types.is_integer(field.type) else "string"}
        for field in arrow_schema()
    }
    print(
        json.dumps(
            {
                "schema": {
                    "storage_mode": "relational",
                    "relational_indexes": [
                        {"name": column + "_idx", "keys": [{"column": column}]}
                        for column in (
                            "hn_id",
                            "created_at",
                            "item_type",
                            "author",
                            "points",
                        )
                    ],
                    "default_type": "row",
                    "enforce_types": True,
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": properties,
                                "additionalProperties": False,
                            }
                        }
                    },
                    "base_source": {
                        "kind": "external",
                        "format": "iceberg",
                        "table_id": "hackernews",
                        "uri": args.warehouse,
                        "credentials": {"ref": args.source_connection},
                        "schema_fingerprint": "auto",
                        "write_policy": "iceberg_writer",
                        "catalog": catalog,
                    },
                },
                "indexes": {"body_text": {"type": "full_text", "field": "body"}},
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
