from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.query_retained_dataset_body import QueryRetainedDatasetBody
from ...models.query_retained_dataset_response_200 import QueryRetainedDatasetResponse200
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    body: QueryRetainedDatasetBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/datasets/{dataset_name}/query".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            dataset_name=quote(str(dataset_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | QueryRetainedDatasetResponse200 | None:
    if response.status_code == 200:
        response_200 = QueryRetainedDatasetResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 410:
        response_410 = cast(Any, None)
        return response_410

    if response.status_code == 413:
        response_413 = cast(Any, None)
        return response_413

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | QueryRetainedDatasetResponse200]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    client: AuthenticatedClient,
    body: QueryRetainedDatasetBody,
) -> Response[Any | QueryRetainedDatasetResponse200]:
    """Query retained summary buckets

     Reads a half-open range aligned to the summary's bucket width and origin. Follow next using the same
    range and dimensions until null. Each page captures fresh authority; active data may change between
    pages. Rows are ordered by bucket time and a stable dimension hash. Requires read permission on the
    qualified dataset.

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        body (QueryRetainedDatasetBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryRetainedDatasetResponse200]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        dataset_name=dataset_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    client: AuthenticatedClient,
    body: QueryRetainedDatasetBody,
) -> Any | QueryRetainedDatasetResponse200 | None:
    """Query retained summary buckets

     Reads a half-open range aligned to the summary's bucket width and origin. Follow next using the same
    range and dimensions until null. Each page captures fresh authority; active data may change between
    pages. Rows are ordered by bucket time and a stable dimension hash. Requires read permission on the
    qualified dataset.

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        body (QueryRetainedDatasetBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryRetainedDatasetResponse200
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        dataset_name=dataset_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    client: AuthenticatedClient,
    body: QueryRetainedDatasetBody,
) -> Response[Any | QueryRetainedDatasetResponse200]:
    """Query retained summary buckets

     Reads a half-open range aligned to the summary's bucket width and origin. Follow next using the same
    range and dimensions until null. Each page captures fresh authority; active data may change between
    pages. Rows are ordered by bucket time and a stable dimension hash. Requires read permission on the
    qualified dataset.

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        body (QueryRetainedDatasetBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | QueryRetainedDatasetResponse200]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        dataset_name=dataset_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    client: AuthenticatedClient,
    body: QueryRetainedDatasetBody,
) -> Any | QueryRetainedDatasetResponse200 | None:
    """Query retained summary buckets

     Reads a half-open range aligned to the summary's bucket width and origin. Follow next using the same
    range and dimensions until null. Each page captures fresh authority; active data may change between
    pages. Rows are ordered by bucket time and a stable dimension hash. Requires read permission on the
    qualified dataset.

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        body (QueryRetainedDatasetBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | QueryRetainedDatasetResponse200
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            dataset_name=dataset_name,
            client=client,
            body=body,
        )
    ).parsed
