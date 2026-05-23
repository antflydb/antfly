"""Main client interface for Antfly SDK."""

import base64
from typing import Any, Optional, cast
from urllib.parse import quote

from httpx import Timeout

from antfly.client_generated import Client
from antfly.client_generated.api.data_operations import (
    batch_write as batch,
)
from antfly.client_generated.api.data_operations import (
    lookup_key,
)
from antfly.client_generated.client import AuthenticatedClient
from antfly.client_generated.models import (
    BatchRequest,
    BatchRequestInserts,
    Error,
)
from antfly.client_generated.types import UNSET

from .exceptions import AntflyException


class AntflyClient:
    """High-level client for interacting with Antfly database."""

    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[tuple[str, str]] = None,
        bearer_token: Optional[str] = None,
        timeout: float = 30.0,
    ):
        """
        Initialize Antfly client.

        Supports three authentication methods (mutually exclusive):
        - Basic auth: provide ``username`` and ``password``
        - API key: provide ``api_key`` as ``(key_id, key_secret)``
        - Bearer token: provide ``bearer_token``

        Args:
            base_url: Base URL of the Antfly server
            username: Username for basic authentication (optional)
            password: Password for basic authentication (optional)
            api_key: Tuple of (key_id, key_secret) for API key authentication (optional)
            bearer_token: Bearer token string for token authentication (optional)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")

        httpx_args: dict[str, Any] = {}

        if api_key is not None:
            key_id, key_secret = api_key
            encoded = base64.b64encode(f"{key_id}:{key_secret}".encode()).decode()
            self._client = AuthenticatedClient(
                base_url=self.base_url,
                token=encoded,
                prefix="ApiKey",
                timeout=Timeout(timeout),
                httpx_args=httpx_args,
            )
        elif bearer_token is not None:
            self._client = AuthenticatedClient(
                base_url=self.base_url,
                token=bearer_token,
                prefix="Bearer",
                timeout=Timeout(timeout),
                httpx_args=httpx_args,
            )
        else:
            if username and password:
                httpx_args["auth"] = (username, password)

            self._client = Client(
                base_url=self.base_url,
                timeout=Timeout(timeout),
                httpx_args=httpx_args,
            )

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make an HTTP request using the underlying httpx client.

        Args:
            method: HTTP method
            path: URL path (relative to base_url)
            **kwargs: Additional arguments passed to httpx

        Returns:
            Parsed JSON response

        Raises:
            AntflyException: If the request fails
        """
        response = self._client.get_httpx_client().request(method, path, **kwargs)
        if response.status_code >= 400:
            try:
                error_body = response.json()
                msg = error_body.get("error", response.text)
            except Exception:
                msg = response.text
            raise AntflyException(f"Request failed ({response.status_code}): {msg}")
        if response.status_code == 204:
            return None
        return response.json()

    # Table operations

    def create_table(
        self,
        name: str,
        num_shards: Optional[int] = None,
        indexes: Optional[dict[str, Any]] = None,
        schema: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Create a new table.

        Args:
            name: Name of the table
            num_shards: Number of shards for the table
            indexes: Index configurations
            schema: Table schema definition

        Returns:
            Created table object as a dictionary

        Raises:
            AntflyException: If table creation fails
        """
        body: dict[str, Any] = {}
        if num_shards is not None:
            body["num_shards"] = num_shards
        if indexes is not None:
            body["indexes"] = indexes
        if schema is not None:
            body["schema"] = schema

        return self._request(
            "POST",
            f"/tables/{quote(name, safe='')}",
            json=body,
        )

    def list_tables(self) -> list[dict[str, Any]]:
        """
        List all tables.

        Returns:
            List of table status objects

        Raises:
            AntflyException: If listing tables fails
        """
        return self._request("GET", "/tables")

    def get_table(self, name: str) -> dict[str, Any]:
        """
        Get table details.

        Args:
            name: Name of the table

        Returns:
            Table status object as a dictionary

        Raises:
            AntflyException: If getting table fails
        """
        return self._request("GET", f"/tables/{quote(name, safe='')}")

    def drop_table(self, name: str) -> None:
        """
        Drop a table.

        Args:
            name: Name of the table to drop

        Raises:
            AntflyException: If dropping table fails
        """
        self._request("DELETE", f"/tables/{quote(name, safe='')}")

    def get(self, table: str, key: str) -> dict[str, Any]:
        """
        Get a single record by key.

        Args:
            table: Table name
            key: Record key

        Returns:
            Record data

        Raises:
            AntflyException: If lookup fails
        """
        response = lookup_key.sync(
            table_name=table,
            key=key,
            client=cast(AuthenticatedClient, self._client),
        )

        if isinstance(response, Error):
            raise AntflyException(f"Failed to get key '{key}' from table '{table}': {response.error}")
        if response is None:
            raise AntflyException(f"Failed to get key '{key}' from table '{table}'")

        return response.to_dict()

    def batch(
        self,
        table: str,
        inserts: Optional[dict[str, dict[str, Any]]] = None,
        deletes: Optional[list[str]] = None,
    ) -> None:
        """
        Perform batch operations on a table.

        Args:
            table: Table name
            inserts: Dictionary of key-value pairs to insert
            deletes: List of keys to delete

        Raises:
            AntflyException: If batch operation fails
        """
        request = BatchRequest(
            inserts=cast(BatchRequestInserts, inserts) if inserts is not None else UNSET,
            deletes=deletes or [],
        )

        response = batch.sync(
            table_name=table,
            client=cast(AuthenticatedClient, self._client),
            body=request,
        )

        if isinstance(response, Error):
            raise AntflyException(f"Batch operation failed for table '{table}': {response.error}")
        if response is None:
            raise AntflyException(f"Batch operation failed for table '{table}'")
