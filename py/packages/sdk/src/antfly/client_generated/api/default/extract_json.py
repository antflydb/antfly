from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.termite_error import TermiteError
from ...models.termite_extract_request import TermiteExtractRequest
from ...models.termite_extract_response import TermiteExtractResponse
from ...types import Response


def _get_kwargs(
    *,
    body: TermiteExtractRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ml/v1/extract",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> TermiteError | TermiteExtractResponse | None:
    if response.status_code == 200:
        response_200 = TermiteExtractResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = TermiteError.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = TermiteError.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = TermiteError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = TermiteError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[TermiteError | TermiteExtractResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteExtractRequest,
) -> Response[TermiteError | TermiteExtractResponse]:
    r"""Extract structured data from text

     Extracts structured data from text using GLiNER2 models.
    Field names in the schema are treated as NER labels, and the model's
    span extraction pipeline populates field values.

    ## Schema Format

    The schema maps structure names to arrays of field definitions:
    ```json
    {
      \"person\": [\"name::str\", \"age::str\", \"skills::list\"]
    }
    ```

    Field types:
    - `::str` - Keep only the top-scoring span (default if no type specified)
    - `::list` - Keep all extracted spans as an array
    - `::[opt1|opt2]::str` - Choice field, classified against options

    ## Example

    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"texts\": [\"John Smith is 30 years old and works at Google.\"],
      \"schema\": {
        \"person\": [\"name::str\", \"age::str\", \"company::str\"]
      }
    }
    ```

    Response:
    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"results\": [
        {
          \"person\": [
            {
              \"name\": {\"value\": \"John Smith\"},
              \"age\": {\"value\": \"30\"},
              \"company\": {\"value\": \"Google\"}
            }
          ]
        }
      ]
    }
    ```

    Args:
        body (TermiteExtractRequest): Exactly one of `texts` or `images` must be provided.
            When using `images`, the server selects a compatible reader internally
            and processes the request as: read document text -> run structured extraction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteExtractResponse]
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
    body: TermiteExtractRequest,
) -> TermiteError | TermiteExtractResponse | None:
    r"""Extract structured data from text

     Extracts structured data from text using GLiNER2 models.
    Field names in the schema are treated as NER labels, and the model's
    span extraction pipeline populates field values.

    ## Schema Format

    The schema maps structure names to arrays of field definitions:
    ```json
    {
      \"person\": [\"name::str\", \"age::str\", \"skills::list\"]
    }
    ```

    Field types:
    - `::str` - Keep only the top-scoring span (default if no type specified)
    - `::list` - Keep all extracted spans as an array
    - `::[opt1|opt2]::str` - Choice field, classified against options

    ## Example

    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"texts\": [\"John Smith is 30 years old and works at Google.\"],
      \"schema\": {
        \"person\": [\"name::str\", \"age::str\", \"company::str\"]
      }
    }
    ```

    Response:
    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"results\": [
        {
          \"person\": [
            {
              \"name\": {\"value\": \"John Smith\"},
              \"age\": {\"value\": \"30\"},
              \"company\": {\"value\": \"Google\"}
            }
          ]
        }
      ]
    }
    ```

    Args:
        body (TermiteExtractRequest): Exactly one of `texts` or `images` must be provided.
            When using `images`, the server selects a compatible reader internally
            and processes the request as: read document text -> run structured extraction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteExtractResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteExtractRequest,
) -> Response[TermiteError | TermiteExtractResponse]:
    r"""Extract structured data from text

     Extracts structured data from text using GLiNER2 models.
    Field names in the schema are treated as NER labels, and the model's
    span extraction pipeline populates field values.

    ## Schema Format

    The schema maps structure names to arrays of field definitions:
    ```json
    {
      \"person\": [\"name::str\", \"age::str\", \"skills::list\"]
    }
    ```

    Field types:
    - `::str` - Keep only the top-scoring span (default if no type specified)
    - `::list` - Keep all extracted spans as an array
    - `::[opt1|opt2]::str` - Choice field, classified against options

    ## Example

    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"texts\": [\"John Smith is 30 years old and works at Google.\"],
      \"schema\": {
        \"person\": [\"name::str\", \"age::str\", \"company::str\"]
      }
    }
    ```

    Response:
    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"results\": [
        {
          \"person\": [
            {
              \"name\": {\"value\": \"John Smith\"},
              \"age\": {\"value\": \"30\"},
              \"company\": {\"value\": \"Google\"}
            }
          ]
        }
      ]
    }
    ```

    Args:
        body (TermiteExtractRequest): Exactly one of `texts` or `images` must be provided.
            When using `images`, the server selects a compatible reader internally
            and processes the request as: read document text -> run structured extraction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[TermiteError | TermiteExtractResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: TermiteExtractRequest,
) -> TermiteError | TermiteExtractResponse | None:
    r"""Extract structured data from text

     Extracts structured data from text using GLiNER2 models.
    Field names in the schema are treated as NER labels, and the model's
    span extraction pipeline populates field values.

    ## Schema Format

    The schema maps structure names to arrays of field definitions:
    ```json
    {
      \"person\": [\"name::str\", \"age::str\", \"skills::list\"]
    }
    ```

    Field types:
    - `::str` - Keep only the top-scoring span (default if no type specified)
    - `::list` - Keep all extracted spans as an array
    - `::[opt1|opt2]::str` - Choice field, classified against options

    ## Example

    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"texts\": [\"John Smith is 30 years old and works at Google.\"],
      \"schema\": {
        \"person\": [\"name::str\", \"age::str\", \"company::str\"]
      }
    }
    ```

    Response:
    ```json
    {
      \"model\": \"fastino/gliner2-base-v1\",
      \"results\": [
        {
          \"person\": [
            {
              \"name\": {\"value\": \"John Smith\"},
              \"age\": {\"value\": \"30\"},
              \"company\": {\"value\": \"Google\"}
            }
          ]
        }
      ]
    }
    ```

    Args:
        body (TermiteExtractRequest): Exactly one of `texts` or `images` must be provided.
            When using `images`, the server selects a compatible reader internally
            and processes the request as: read document text -> run structured extraction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        TermiteError | TermiteExtractResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
