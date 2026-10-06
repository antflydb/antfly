from http import HTTPStatus
from typing import Any, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.store_root_enrollment_identity import StoreRootEnrollmentIdentity
from ...models.store_root_enrollment_request import StoreRootEnrollmentRequest
from ...types import Response


def _get_kwargs(
    *,
    body: StoreRootEnrollmentRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/store-roots/enroll",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | StoreRootEnrollmentIdentity | None:
    if response.status_code == 200:
        response_200 = StoreRootEnrollmentIdentity.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 426:
        response_426 = cast(Any, None)
        return response_426

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | StoreRootEnrollmentIdentity]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: StoreRootEnrollmentRequest,
) -> Response[Any | StoreRootEnrollmentIdentity]:
    """Approve a physical store-root signing identity

     Cluster-administrator-only approval of a locally signed proof bound to the metadata cluster, node,
    store and physical root. Ordinary node registration does not grant retirement authority. The request
    is not automatically retried; observe the enrollment after an ambiguous 503.

    Args:
        body (StoreRootEnrollmentRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | StoreRootEnrollmentIdentity]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: StoreRootEnrollmentRequest,
) -> Any | StoreRootEnrollmentIdentity | None:
    """Approve a physical store-root signing identity

     Cluster-administrator-only approval of a locally signed proof bound to the metadata cluster, node,
    store and physical root. Ordinary node registration does not grant retirement authority. The request
    is not automatically retried; observe the enrollment after an ambiguous 503.

    Args:
        body (StoreRootEnrollmentRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | StoreRootEnrollmentIdentity
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: StoreRootEnrollmentRequest,
) -> Response[Any | StoreRootEnrollmentIdentity]:
    """Approve a physical store-root signing identity

     Cluster-administrator-only approval of a locally signed proof bound to the metadata cluster, node,
    store and physical root. Ordinary node registration does not grant retirement authority. The request
    is not automatically retried; observe the enrollment after an ambiguous 503.

    Args:
        body (StoreRootEnrollmentRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | StoreRootEnrollmentIdentity]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: StoreRootEnrollmentRequest,
) -> Any | StoreRootEnrollmentIdentity | None:
    """Approve a physical store-root signing identity

     Cluster-administrator-only approval of a locally signed proof bound to the metadata cluster, node,
    store and physical root. Ordinary node registration does not grant retirement authority. The request
    is not automatically retried; observe the enrollment after an ambiguous 503.

    Args:
        body (StoreRootEnrollmentRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | StoreRootEnrollmentIdentity
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
