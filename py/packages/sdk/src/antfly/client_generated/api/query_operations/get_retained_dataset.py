from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.retained_dataset_info import RetainedDatasetInfo
from ...types import UNSET, Response, Unset


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    after_group: str | Unset = UNSET,
    max_owners: int | Unset = 16,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["after_group"] = after_group

    params["max_owners"] = max_owners

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/datasets/{dataset_name}".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            dataset_name=quote(str(dataset_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | RetainedDatasetInfo | None:
    if response.status_code == 200:
        response_200 = RetainedDatasetInfo.from_dict(response.json())

        return response_200

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | RetainedDatasetInfo]:
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
    after_group: str | Unset = UNSET,
    max_owners: int | Unset = 16,
) -> Response[Any | RetainedDatasetInfo]:
    """Get retained summary status

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        after_group (str | Unset):
        max_owners (int | Unset):  Default: 16.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | RetainedDatasetInfo]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        dataset_name=dataset_name,
        after_group=after_group,
        max_owners=max_owners,
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
    after_group: str | Unset = UNSET,
    max_owners: int | Unset = 16,
) -> Any | RetainedDatasetInfo | None:
    """Get retained summary status

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        after_group (str | Unset):
        max_owners (int | Unset):  Default: 16.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | RetainedDatasetInfo
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        dataset_name=dataset_name,
        client=client,
        after_group=after_group,
        max_owners=max_owners,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    client: AuthenticatedClient,
    after_group: str | Unset = UNSET,
    max_owners: int | Unset = 16,
) -> Response[Any | RetainedDatasetInfo]:
    """Get retained summary status

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        after_group (str | Unset):
        max_owners (int | Unset):  Default: 16.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | RetainedDatasetInfo]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        dataset_name=dataset_name,
        after_group=after_group,
        max_owners=max_owners,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    dataset_name: str,
    *,
    client: AuthenticatedClient,
    after_group: str | Unset = UNSET,
    max_owners: int | Unset = 16,
) -> Any | RetainedDatasetInfo | None:
    """Get retained summary status

    Args:
        database_name (str):
        namespace_name (str):
        dataset_name (str):
        after_group (str | Unset):
        max_owners (int | Unset):  Default: 16.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | RetainedDatasetInfo
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            dataset_name=dataset_name,
            client=client,
            after_group=after_group,
            max_owners=max_owners,
        )
    ).parsed
