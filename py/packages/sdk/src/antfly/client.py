"""Main client interface for Antfly SDK."""

import base64
import json
from typing import Any, cast
from urllib.parse import quote

from httpx import Timeout

from antfly.client_generated import Client
from antfly.client_generated.api.data_operations import (
    lookup_key,
)
from antfly.client_generated.client import AuthenticatedClient
from antfly.client_generated.models import (
    BatchRequest,
    BatchRequestInserts,
    Error,
    SqlStatementRequest,
    SqlStatementResponse,
)
from antfly.client_generated.types import UNSET

from .exceptions import AntflyException

DEFAULT_WRITE_MAX_REQUEST_BYTES = 64 << 20


def normalize_base_url(base_url: str) -> str:
    """Return the Antfly server root URL for local or CloudAF endpoints."""
    trimmed = base_url.rstrip("/")
    if trimmed.endswith("/db/v1"):
        return trimmed[: -len("/db/v1")]
    if trimmed.endswith("/auth/v1"):
        return trimmed[: -len("/auth/v1")]
    if trimmed.endswith("/ai/v1"):
        return trimmed[: -len("/ai/v1")]
    return trimmed


class AntflyClient:
    """High-level client for interacting with Antfly database."""

    def __init__(
        self,
        base_url: str,
        username: str | None = None,
        password: str | None = None,
        api_key: tuple[str, str] | None = None,
        token: str | None = None,
        timeout: float = 30.0,
        max_write_request_bytes: int = DEFAULT_WRITE_MAX_REQUEST_BYTES,
    ):
        """
        Initialize Antfly client.

        Supports three authentication methods (mutually exclusive):
        - Basic auth: provide ``username`` and ``password``
        - API key: provide ``api_key`` as ``(key_id, key_secret)``
        - Token: provide ``token``

        Args:
            base_url: Base URL of the Antfly server or CloudAF proxy
            username: Username for basic authentication (optional)
            password: Password for basic authentication (optional)
            api_key: Tuple of (key_id, key_secret) for API key authentication (optional)
            token: Token string for token authentication (optional)
            timeout: Request timeout in seconds
            max_write_request_bytes: Maximum encoded JSON bytes for write requests
        """
        self.base_url = normalize_base_url(base_url)
        self.max_write_request_bytes = max_write_request_bytes

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
        elif token is not None:
            self._client = AuthenticatedClient(
                base_url=self.base_url,
                token=token,
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

    def _encode_write_request(self, operation: str, body: dict[str, Any]) -> bytes:
        encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if self.max_write_request_bytes <= 0:
            return encoded
        if len(encoded) > self.max_write_request_bytes:
            raise AntflyException(
                f"{operation} request encoded to {len(encoded)} bytes, exceeding max write request size "
                f"{self.max_write_request_bytes}"
            )
        return encoded

    # Table operations

    def create_table(
        self,
        name: str,
        num_shards: int | None = None,
        indexes: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
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
            f"/db/v1/tables/{quote(name, safe='')}",
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
        return self._request("GET", "/db/v1/tables")

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
        return self._request("GET", f"/db/v1/tables/{quote(name, safe='')}")

    def drop_table(self, name: str) -> None:
        """
        Drop a table.

        Args:
            name: Name of the table to drop

        Raises:
            AntflyException: If dropping table fails
        """
        self._request("DELETE", f"/db/v1/tables/{quote(name, safe='')}")

    def execute_sql(
        self,
        sql: str,
        session_id: int | None = None,
        database: str | None = None,
        namespace: str | None = None,
        read_only: bool | None = None,
    ) -> SqlStatementResponse:
        """
        Execute one SQL statement through Antfly's public SQL ingress.

        Reuse the returned ``session_id`` on later calls when session state
        must persist across statements.
        """
        request = SqlStatementRequest(
            sql=sql,
            session_id=UNSET if session_id is None else session_id,
            database=UNSET if database is None else database,
            namespace=UNSET if namespace is None else namespace,
            read_only=UNSET if read_only is None else read_only,
        )
        response = self._request("POST", "/db/v1/sql", json=request.to_dict())
        return SqlStatementResponse.from_dict(response)

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

    def list_artifact_enrichments(self, table: str) -> dict[str, Any]:
        """
        List table-level generated artifact enrichments.

        Args:
            table: Table name

        Returns:
            Artifact enrichment list response

        Raises:
            AntflyException: If listing artifact enrichments fails
        """
        return self._request("GET", f"/db/v1/tables/{quote(table, safe='')}/artifacts")

    def put_artifact_enrichment(
        self,
        table: str,
        artifact: str,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Register or replace a table-level generated artifact enrichment.

        Args:
            table: Table name
            artifact: Artifact enrichment name
            config: Enrichment configuration

        Returns:
            API response body

        Raises:
            AntflyException: If storing the artifact enrichment fails
        """
        return self._request(
            "PUT",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/enrichment",
            json=config,
        )

    def delete_artifact_enrichment(self, table: str, artifact: str) -> dict[str, Any]:
        """
        Delete a table-level generated artifact enrichment.

        Args:
            table: Table name
            artifact: Artifact enrichment name

        Returns:
            API response body

        Raises:
            AntflyException: If deleting the artifact enrichment fails
        """
        return self._request(
            "DELETE",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/enrichment",
        )

    def list_document_artifacts(
        self,
        table: str,
        key: str,
        detail: str = "summary",
    ) -> dict[str, Any]:
        """
        List generated artifact manifests attached to a document.

        Args:
            table: Table name
            key: Document key
            detail: Response detail level, ``summary`` or ``raw``

        Returns:
            Document artifact manifest list

        Raises:
            AntflyException: If listing document artifacts fails
        """
        return self._request(
            "GET",
            f"/db/v1/tables/{quote(table, safe='')}/documents/{quote(key, safe='')}/artifacts",
            params={"detail": detail},
        )

    def get_document_artifact(
        self,
        table: str,
        key: str,
        artifact: str,
        detail: str = "raw",
    ) -> dict[str, Any]:
        """
        Get a generated artifact manifest attached to a document.

        Args:
            table: Table name
            key: Document key
            artifact: Artifact name
            detail: Response detail level, ``summary`` or ``raw``

        Returns:
            Document artifact manifest

        Raises:
            AntflyException: If getting the document artifact fails
        """
        return self._request(
            "GET",
            f"/db/v1/tables/{quote(table, safe='')}/documents/{quote(key, safe='')}/artifacts/{quote(artifact, safe='')}",
            params={"detail": detail},
        )

    def reprocess_document_artifact(self, table: str, key: str, artifact: str) -> dict[str, Any]:
        """
        Reprocess one generated artifact for one document.

        Args:
            table: Table name
            key: Document key
            artifact: Artifact name

        Returns:
            Reprocess response

        Raises:
            AntflyException: If reprocessing fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/documents/{quote(key, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess",
        )

    def reprocess_artifact_range(
        self,
        table: str,
        artifact: str,
        request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Run one bounded table-wide reprocess pass for an artifact.

        Args:
            table: Table name
            artifact: Artifact name
            request: Optional reprocess request body

        Returns:
            Reprocess response

        Raises:
            AntflyException: If reprocessing fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess",
            json=request or {},
        )

    def start_artifact_reprocess_job(
        self,
        table: str,
        artifact: str,
        request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Start a durable table-wide artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            request: Optional job start request body

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If starting the job fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs",
            json=request or {},
        )

    def get_artifact_reprocess_job(self, table: str, artifact: str, job_id: str) -> dict[str, Any]:
        """
        Get a durable artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            job_id: Job identifier

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If loading the job fails
        """
        return self._request(
            "GET",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs/{quote(job_id, safe='')}",
        )

    def advance_artifact_reprocess_job(self, table: str, artifact: str, job_id: str) -> dict[str, Any]:
        """
        Advance a durable artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            job_id: Job identifier

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If advancing the job fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs/{quote(job_id, safe='')}/advance",
        )

    def cancel_artifact_reprocess_job(self, table: str, artifact: str, job_id: str) -> dict[str, Any]:
        """
        Cancel a durable artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            job_id: Job identifier

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If canceling the job fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs/{quote(job_id, safe='')}/cancel",
        )

    def batch(
        self,
        table: str,
        inserts: dict[str, dict[str, Any]] | None = None,
        deletes: list[str] | None = None,
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
        batch_inserts = BatchRequestInserts.from_dict(inserts) if inserts is not None else UNSET
        request = BatchRequest(inserts=batch_inserts, deletes=deletes or [])
        encoded = self._encode_write_request("Batch", request.to_dict())

        self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/batch",
            content=encoded,
            headers={"Content-Type": "application/json"},
        )
