# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run only against the local privileged fixture ports; prints no credentials."""

import requests
from botocore.exceptions import ClientError
from antfly_lake_maintenance.store import Store

store = Store(
    {
        "s3.endpoint": "http://127.0.0.1:29700",
        "s3.region": "us-west-2",
        "s3.access-key-id": "antfly_qualification",
        "s3.secret-access-key": "antfly_qualification_secret",
    }
)
client = store.s3
try:
    client.create_bucket(Bucket="warehouse")
except ClientError as error:
    if error.response["Error"]["Code"] not in (
        "BucketAlreadyOwnedByYou",
        "BucketAlreadyExists",
    ):
        raise
client.put_bucket_versioning(
    Bucket="warehouse", VersioningConfiguration={"Status": "Enabled"}
)
response = requests.post(
    "http://127.0.0.1:29710/api/catalog/v1/oauth/tokens",
    data={
        "grant_type": "client_credentials",
        "client_id": "root",
        "client_secret": "qualification_only",
        "scope": "PRINCIPAL_ROLE:ALL",
    },
    timeout=20,
)
response.raise_for_status()
headers = {
    "Authorization": "Bearer " + response.json()["access_token"],
    "Polaris-Realm": "POLARIS",
}
root = "http://127.0.0.1:29710/api/management/v1/catalogs"
response = requests.get(root + "/qualification", headers=headers, timeout=20)
if response.status_code == 404:
    response = requests.post(
        root,
        headers=headers,
        json={
            "catalog": {
                "name": "qualification",
                "type": "INTERNAL",
                "properties": {"default-base-location": "s3://warehouse/polaris"},
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": ["s3://warehouse/polaris"],
                    "endpoint": "http://127.0.0.1:29700",
                    "endpointInternal": "http://antfly-maintenance-s3:9000",
                    "pathStyleAccess": True,
                    "region": "us-west-2",
                },
            }
        },
        timeout=20,
    )
response.raise_for_status()
print("Versioned fixture warehouse and Polaris catalog ready")
