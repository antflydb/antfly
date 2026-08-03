from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_predict_request import InferencePredictRequest
from ...models.inference_predict_response import InferencePredictResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferencePredictRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/predict",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferencePredictResponse | None:
    if response.status_code == 200:
        response_200 = InferencePredictResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 413:
        response_413 = InferenceError.from_dict(response.json())

        return response_413

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferencePredictResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferencePredictRequest,
) -> Response[InferenceError | InferencePredictResponse]:
    """Run a traditional ML predictor

     Run a tabular predictor (tree ensemble, linear, or SVM) on a batch of
    feature vectors. Models are loaded from `<ml_dir>/<name>/` and
    identified by name. Use `/ml/v1/models` for the list of available
    predictors and their feature schemas.

    Args:
        body (InferencePredictRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferencePredictResponse]
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
    client: AuthenticatedClient | Client,
    body: InferencePredictRequest,
) -> InferenceError | InferencePredictResponse | None:
    """Run a traditional ML predictor

     Run a tabular predictor (tree ensemble, linear, or SVM) on a batch of
    feature vectors. Models are loaded from `<ml_dir>/<name>/` and
    identified by name. Use `/ml/v1/models` for the list of available
    predictors and their feature schemas.

    Args:
        body (InferencePredictRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferencePredictResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferencePredictRequest,
) -> Response[InferenceError | InferencePredictResponse]:
    """Run a traditional ML predictor

     Run a tabular predictor (tree ensemble, linear, or SVM) on a batch of
    feature vectors. Models are loaded from `<ml_dir>/<name>/` and
    identified by name. Use `/ml/v1/models` for the list of available
    predictors and their feature schemas.

    Args:
        body (InferencePredictRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferencePredictResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferencePredictRequest,
) -> InferenceError | InferencePredictResponse | None:
    """Run a traditional ML predictor

     Run a tabular predictor (tree ensemble, linear, or SVM) on a batch of
    feature vectors. Models are loaded from `<ml_dir>/<name>/` and
    identified by name. Use `/ml/v1/models` for the list of available
    predictors and their feature schemas.

    Args:
        body (InferencePredictRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferencePredictResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
