from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_predictors_response import InferencePredictorsResponse
from ...types import Response


def _get_kwargs() -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/ml/v1/models",
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferencePredictorsResponse | None:
    if response.status_code == 200:
        response_200 = InferencePredictorsResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferencePredictorsResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[InferenceError | InferencePredictorsResponse]:
    """List Traditional ML predictors

     Returns the Traditional ML predictor catalog for `/ml/v1/predict`.
    Predictors are loaded from `<ml_dir>/<name>/` and exposed separately
    from the AI model catalog.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferencePredictorsResponse]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
) -> InferenceError | InferencePredictorsResponse | None:
    """List Traditional ML predictors

     Returns the Traditional ML predictor catalog for `/ml/v1/predict`.
    Predictors are loaded from `<ml_dir>/<name>/` and exposed separately
    from the AI model catalog.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferencePredictorsResponse
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[InferenceError | InferencePredictorsResponse]:
    """List Traditional ML predictors

     Returns the Traditional ML predictor catalog for `/ml/v1/predict`.
    Predictors are loaded from `<ml_dir>/<name>/` and exposed separately
    from the AI model catalog.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferencePredictorsResponse]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
) -> InferenceError | InferencePredictorsResponse | None:
    """List Traditional ML predictors

     Returns the Traditional ML predictor catalog for `/ml/v1/predict`.
    Predictors are loaded from `<ml_dir>/<name>/` and exposed separately
    from the AI model catalog.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferencePredictorsResponse
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
