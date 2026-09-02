from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.batch_request import BatchRequest
from ...models.error import Error
from ...models.idempotent_batch_error import IdempotentBatchError
from ...models.idempotent_batch_response import IdempotentBatchResponse
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: BatchRequest,
    idempotency_key: str,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    headers["Idempotency-Key"] = idempotency_key

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/idempotent-batch".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | IdempotentBatchError | IdempotentBatchResponse | None:
    if response.status_code == 200:
        response_200 = IdempotentBatchResponse.from_dict(response.json())

        return response_200

    if response.status_code == 201:
        response_201 = IdempotentBatchResponse.from_dict(response.json())

        return response_201

    if response.status_code == 202:
        response_202 = IdempotentBatchResponse.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = IdempotentBatchError.from_dict(response.json())

        return response_409

    if response.status_code == 413:
        response_413 = Error.from_dict(response.json())

        return response_413

    if response.status_code == 429:
        response_429 = IdempotentBatchError.from_dict(response.json())

        return response_429

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = IdempotentBatchError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | IdempotentBatchError | IdempotentBatchResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: BatchRequest,
    idempotency_key: str,
) -> Response[Error | IdempotentBatchError | IdempotentBatchResponse]:
    """Perform a durably idempotent batch operation on a table

     Executes a batch under a durable, payload-sealed idempotency receipt. The separate endpoint is
    rolling-upgrade safe because older servers reject it instead of silently ignoring the idempotency
    contract. Keys are scoped to the authenticated principal and table and may be replayed after
    timeouts, lost responses, topology changes, or process restarts. Receipts use the configured
    transaction-session retention period; callers must not reuse a key after that period. Distributed
    deployments fail closed unless the receipt store is cluster-shared and supports atomic owner
    fencing.

    Args:
        table_name (str):
        idempotency_key (str):
        body (BatchRequest): Batch insert, delete, and transform operations in a single request.

            **Atomicity**:
            - **Single shard**: Operations are atomic within shard boundaries
            - **Multiple shards**: Uses distributed 2-phase commit (2PC) for atomic cross-shard writes

            **How distributed transactions work**:
            1. Metadata server allocates HLC timestamp and selects coordinator shard
            2. Coordinator writes transaction record, participants write intents
            3. After all intents succeed, coordinator commits transaction
            4. Participants are notified asynchronously to resolve intents
            5. Recovery loop ensures notifications complete even after coordinator failure

            **Performance**:
            - Single-shard batches: < 5ms latency
            - Cross-shard transactions: ~20ms latency
            - Intent resolution: < 30 seconds worst-case (via recovery loop)

            **Guarantees**:
            - All writes succeed or all fail (atomicity across all shards)
            - Coordinator failure is recoverable (new leader resumes notifications)
            - Idempotent resolution (duplicate notifications are safe)

            **Benefits**:
            - Reduces network overhead compared to individual requests
            - More efficient indexing (updates are batched)
            - Automatic distributed transactions when operations span shards

            The inserts are upserts - existing keys are overwritten, new keys are created.
             Example: {'inserts': {'user:123': {'name': 'John Doe', 'email': 'john@example.com',
            'age': 30, 'tags': ['customer', 'premium']}, 'user:456': {'name': 'Jane Smith', 'email':
            'jane@example.com', 'age': 25, 'tags': ['customer']}}, 'deletes': ['user:789',
            'user:old_account']}.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | IdempotentBatchError | IdempotentBatchResponse]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        idempotency_key=idempotency_key,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: BatchRequest,
    idempotency_key: str,
) -> Error | IdempotentBatchError | IdempotentBatchResponse | None:
    """Perform a durably idempotent batch operation on a table

     Executes a batch under a durable, payload-sealed idempotency receipt. The separate endpoint is
    rolling-upgrade safe because older servers reject it instead of silently ignoring the idempotency
    contract. Keys are scoped to the authenticated principal and table and may be replayed after
    timeouts, lost responses, topology changes, or process restarts. Receipts use the configured
    transaction-session retention period; callers must not reuse a key after that period. Distributed
    deployments fail closed unless the receipt store is cluster-shared and supports atomic owner
    fencing.

    Args:
        table_name (str):
        idempotency_key (str):
        body (BatchRequest): Batch insert, delete, and transform operations in a single request.

            **Atomicity**:
            - **Single shard**: Operations are atomic within shard boundaries
            - **Multiple shards**: Uses distributed 2-phase commit (2PC) for atomic cross-shard writes

            **How distributed transactions work**:
            1. Metadata server allocates HLC timestamp and selects coordinator shard
            2. Coordinator writes transaction record, participants write intents
            3. After all intents succeed, coordinator commits transaction
            4. Participants are notified asynchronously to resolve intents
            5. Recovery loop ensures notifications complete even after coordinator failure

            **Performance**:
            - Single-shard batches: < 5ms latency
            - Cross-shard transactions: ~20ms latency
            - Intent resolution: < 30 seconds worst-case (via recovery loop)

            **Guarantees**:
            - All writes succeed or all fail (atomicity across all shards)
            - Coordinator failure is recoverable (new leader resumes notifications)
            - Idempotent resolution (duplicate notifications are safe)

            **Benefits**:
            - Reduces network overhead compared to individual requests
            - More efficient indexing (updates are batched)
            - Automatic distributed transactions when operations span shards

            The inserts are upserts - existing keys are overwritten, new keys are created.
             Example: {'inserts': {'user:123': {'name': 'John Doe', 'email': 'john@example.com',
            'age': 30, 'tags': ['customer', 'premium']}, 'user:456': {'name': 'Jane Smith', 'email':
            'jane@example.com', 'age': 25, 'tags': ['customer']}}, 'deletes': ['user:789',
            'user:old_account']}.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | IdempotentBatchError | IdempotentBatchResponse
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
        idempotency_key=idempotency_key,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: BatchRequest,
    idempotency_key: str,
) -> Response[Error | IdempotentBatchError | IdempotentBatchResponse]:
    """Perform a durably idempotent batch operation on a table

     Executes a batch under a durable, payload-sealed idempotency receipt. The separate endpoint is
    rolling-upgrade safe because older servers reject it instead of silently ignoring the idempotency
    contract. Keys are scoped to the authenticated principal and table and may be replayed after
    timeouts, lost responses, topology changes, or process restarts. Receipts use the configured
    transaction-session retention period; callers must not reuse a key after that period. Distributed
    deployments fail closed unless the receipt store is cluster-shared and supports atomic owner
    fencing.

    Args:
        table_name (str):
        idempotency_key (str):
        body (BatchRequest): Batch insert, delete, and transform operations in a single request.

            **Atomicity**:
            - **Single shard**: Operations are atomic within shard boundaries
            - **Multiple shards**: Uses distributed 2-phase commit (2PC) for atomic cross-shard writes

            **How distributed transactions work**:
            1. Metadata server allocates HLC timestamp and selects coordinator shard
            2. Coordinator writes transaction record, participants write intents
            3. After all intents succeed, coordinator commits transaction
            4. Participants are notified asynchronously to resolve intents
            5. Recovery loop ensures notifications complete even after coordinator failure

            **Performance**:
            - Single-shard batches: < 5ms latency
            - Cross-shard transactions: ~20ms latency
            - Intent resolution: < 30 seconds worst-case (via recovery loop)

            **Guarantees**:
            - All writes succeed or all fail (atomicity across all shards)
            - Coordinator failure is recoverable (new leader resumes notifications)
            - Idempotent resolution (duplicate notifications are safe)

            **Benefits**:
            - Reduces network overhead compared to individual requests
            - More efficient indexing (updates are batched)
            - Automatic distributed transactions when operations span shards

            The inserts are upserts - existing keys are overwritten, new keys are created.
             Example: {'inserts': {'user:123': {'name': 'John Doe', 'email': 'john@example.com',
            'age': 30, 'tags': ['customer', 'premium']}, 'user:456': {'name': 'Jane Smith', 'email':
            'jane@example.com', 'age': 25, 'tags': ['customer']}}, 'deletes': ['user:789',
            'user:old_account']}.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | IdempotentBatchError | IdempotentBatchResponse]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        idempotency_key=idempotency_key,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: BatchRequest,
    idempotency_key: str,
) -> Error | IdempotentBatchError | IdempotentBatchResponse | None:
    """Perform a durably idempotent batch operation on a table

     Executes a batch under a durable, payload-sealed idempotency receipt. The separate endpoint is
    rolling-upgrade safe because older servers reject it instead of silently ignoring the idempotency
    contract. Keys are scoped to the authenticated principal and table and may be replayed after
    timeouts, lost responses, topology changes, or process restarts. Receipts use the configured
    transaction-session retention period; callers must not reuse a key after that period. Distributed
    deployments fail closed unless the receipt store is cluster-shared and supports atomic owner
    fencing.

    Args:
        table_name (str):
        idempotency_key (str):
        body (BatchRequest): Batch insert, delete, and transform operations in a single request.

            **Atomicity**:
            - **Single shard**: Operations are atomic within shard boundaries
            - **Multiple shards**: Uses distributed 2-phase commit (2PC) for atomic cross-shard writes

            **How distributed transactions work**:
            1. Metadata server allocates HLC timestamp and selects coordinator shard
            2. Coordinator writes transaction record, participants write intents
            3. After all intents succeed, coordinator commits transaction
            4. Participants are notified asynchronously to resolve intents
            5. Recovery loop ensures notifications complete even after coordinator failure

            **Performance**:
            - Single-shard batches: < 5ms latency
            - Cross-shard transactions: ~20ms latency
            - Intent resolution: < 30 seconds worst-case (via recovery loop)

            **Guarantees**:
            - All writes succeed or all fail (atomicity across all shards)
            - Coordinator failure is recoverable (new leader resumes notifications)
            - Idempotent resolution (duplicate notifications are safe)

            **Benefits**:
            - Reduces network overhead compared to individual requests
            - More efficient indexing (updates are batched)
            - Automatic distributed transactions when operations span shards

            The inserts are upserts - existing keys are overwritten, new keys are created.
             Example: {'inserts': {'user:123': {'name': 'John Doe', 'email': 'john@example.com',
            'age': 30, 'tags': ['customer', 'premium']}, 'user:456': {'name': 'Jane Smith', 'email':
            'jane@example.com', 'age': 25, 'tags': ['customer']}}, 'deletes': ['user:789',
            'user:old_account']}.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | IdempotentBatchError | IdempotentBatchResponse
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
            idempotency_key=idempotency_key,
        )
    ).parsed
