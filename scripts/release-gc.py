#!/usr/bin/env python3
"""Delete prerelease (dev, rc, alpha, beta) artifacts from the antfly-releases R2 bucket.

Usage:
    uv run scripts/release-gc.py --endpoint $R2_ENDPOINT          # dry-run (default)
    uv run scripts/release-gc.py --endpoint $R2_ENDPOINT --delete  # actually delete
    uv run scripts/release-gc.py --endpoint $R2_ENDPOINT --delete --prefix antfly/

Environment:
    AWS_ACCESS_KEY_ID      R2 access key
    AWS_SECRET_ACCESS_KEY  R2 secret key
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["boto3"]
# ///

import argparse
import os
import sys

import boto3

BUCKET = "antfly-releases"
PRERELEASE_INDICATORS = ("-dev", "-rc", "-alpha", "-beta")


def get_client(endpoint: str):
    key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if not key_id or not secret:
        print(
            "WARNING: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set for R2 access",
            file=sys.stderr,
        )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="auto",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
    )


def list_version_prefixes(client, bucket: str, prefix: str) -> list[str]:
    """List top-level 'directories' under a prefix (e.g. antfly/v0.0.8-rc2/)."""
    prefixes = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
    return prefixes


def is_prerelease(tag: str) -> bool:
    lower = tag.lower()
    return any(ind in lower for ind in PRERELEASE_INDICATORS)


def extract_tag(version_prefix: str, parent_prefix: str) -> str:
    return version_prefix.removeprefix(parent_prefix).rstrip("/")


def list_all_objects(client, bucket: str, prefix: str) -> list[dict]:
    """List all objects under a prefix."""
    objects = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"]})
    return objects


def delete_objects(client, bucket: str, objects: list[dict]) -> int:
    """Delete objects in batches of 1000. Returns count deleted."""
    deleted = 0
    for i in range(0, len(objects), 1000):
        batch = objects[i : i + 1000]
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": batch, "Quiet": True},
        )
        deleted += len(batch)
    return deleted


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--endpoint", required=True, help="S3-compatible endpoint URL (e.g. https://ACCT.r2.cloudflarestorage.com)")
    parser.add_argument("--bucket", default=BUCKET, help=f"bucket name (default: {BUCKET})")
    parser.add_argument("--delete", action="store_true", help="actually delete objects (default is dry-run)")
    parser.add_argument("--prefix", default="", help="limit to a specific prefix (e.g. 'antfly/' or 'termite/')")
    args = parser.parse_args()

    client = get_client(args.endpoint)

    prefixes = [args.prefix] if args.prefix else ["antfly/", "termite/"]

    total_objects = 0
    total_deleted = 0

    for pfx in prefixes:
        print(f"Scanning {pfx}...")
        versions = list_version_prefixes(client, args.bucket, pfx)

        for ver in versions:
            tag = extract_tag(ver, pfx)
            if not is_prerelease(tag):
                continue

            objects = list_all_objects(client, args.bucket, ver)
            total_objects += len(objects)

            if args.delete:
                count = delete_objects(client, args.bucket, objects)
                total_deleted += count
                print(f"  {ver} ({len(objects)} objects) - DELETED")
            else:
                print(f"  {ver} ({len(objects)} objects) - would delete")

    print()
    if args.delete:
        print(f"Deleted {total_deleted} objects across prerelease versions.")
    else:
        print(f"Dry run: {total_objects} objects across prerelease versions would be deleted.")
        print("Run with --delete to actually remove them.")


if __name__ == "__main__":
    main()
