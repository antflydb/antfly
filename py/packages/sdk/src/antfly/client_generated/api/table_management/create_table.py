from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_table_request import CreateTableRequest
from ...models.error import Error
from ...models.table import Table
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: CreateTableRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | Table | None:
    if response.status_code == 200:
        response_200 = Table.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 422:
        response_422 = Error.from_dict(response.json())

        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Error | Table]:
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
    body: CreateTableRequest,
) -> Response[Error | Table]:
    r"""Create a new table

     Creates a new table with optional schema definition, indexes, and configuration.

    ## Use Cases

    **Simple table for unstructured data:**
    ```json
    {
      \"num_shards\": 1
    }
    ```

    **Table with full-text search:**
    ```json
    {
      \"num_shards\": 3,
      \"schema\": {
        \"document_schemas\": {
          \"article\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"title\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"body\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                }
              },
              \"x-antfly-include-in-all\": [\"title\", \"body\"]
            }
          }
        },
        \"default_type\": \"article\"
      },
      \"indexes\": {
        \"search_idx\": {
          \"type\": \"full_text\"
        }
      }
    }
    ```

    **Table with vector similarity search:**
    ```json
    {
      \"num_shards\": 5,
      \"description\": \"Product catalog with semantic search\",
      \"schema\": {
        \"document_schemas\": {
          \"product\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"product_id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"name\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"description\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                },
                \"price\": {
                  \"type\": \"number\",
                  \"x-antfly-types\": [\"numeric\"]
                }
              },
              \"x-antfly-include-in-all\": [\"name\", \"description\"]
            }
          }
        },
        \"default_type\": \"product\"
      },
      \"indexes\": {
        \"semantic_idx\": {
          \"type\": \"embeddings\",
          \"field\": \"description\",
          \"embedder\": {
            \"provider\": \"ollama\",
            \"model\": \"all-minilm\",
            \"url\": \"http://localhost:11434\"
          }
        }
      }
    }
    ```

    ## Best Practices

    - Define schema for core fields to improve performance
    - Start with fewer shards for small datasets (1-3)
    - Use meaningful table names (e.g., \"products\", \"users\", \"articles\")
    - Consider adding both full-text and vector indexes for hybrid search

    Args:
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Table]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateTableRequest,
) -> Error | Table | None:
    r"""Create a new table

     Creates a new table with optional schema definition, indexes, and configuration.

    ## Use Cases

    **Simple table for unstructured data:**
    ```json
    {
      \"num_shards\": 1
    }
    ```

    **Table with full-text search:**
    ```json
    {
      \"num_shards\": 3,
      \"schema\": {
        \"document_schemas\": {
          \"article\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"title\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"body\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                }
              },
              \"x-antfly-include-in-all\": [\"title\", \"body\"]
            }
          }
        },
        \"default_type\": \"article\"
      },
      \"indexes\": {
        \"search_idx\": {
          \"type\": \"full_text\"
        }
      }
    }
    ```

    **Table with vector similarity search:**
    ```json
    {
      \"num_shards\": 5,
      \"description\": \"Product catalog with semantic search\",
      \"schema\": {
        \"document_schemas\": {
          \"product\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"product_id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"name\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"description\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                },
                \"price\": {
                  \"type\": \"number\",
                  \"x-antfly-types\": [\"numeric\"]
                }
              },
              \"x-antfly-include-in-all\": [\"name\", \"description\"]
            }
          }
        },
        \"default_type\": \"product\"
      },
      \"indexes\": {
        \"semantic_idx\": {
          \"type\": \"embeddings\",
          \"field\": \"description\",
          \"embedder\": {
            \"provider\": \"ollama\",
            \"model\": \"all-minilm\",
            \"url\": \"http://localhost:11434\"
          }
        }
      }
    }
    ```

    ## Best Practices

    - Define schema for core fields to improve performance
    - Start with fewer shards for small datasets (1-3)
    - Use meaningful table names (e.g., \"products\", \"users\", \"articles\")
    - Consider adding both full-text and vector indexes for hybrid search

    Args:
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Table
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateTableRequest,
) -> Response[Error | Table]:
    r"""Create a new table

     Creates a new table with optional schema definition, indexes, and configuration.

    ## Use Cases

    **Simple table for unstructured data:**
    ```json
    {
      \"num_shards\": 1
    }
    ```

    **Table with full-text search:**
    ```json
    {
      \"num_shards\": 3,
      \"schema\": {
        \"document_schemas\": {
          \"article\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"title\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"body\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                }
              },
              \"x-antfly-include-in-all\": [\"title\", \"body\"]
            }
          }
        },
        \"default_type\": \"article\"
      },
      \"indexes\": {
        \"search_idx\": {
          \"type\": \"full_text\"
        }
      }
    }
    ```

    **Table with vector similarity search:**
    ```json
    {
      \"num_shards\": 5,
      \"description\": \"Product catalog with semantic search\",
      \"schema\": {
        \"document_schemas\": {
          \"product\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"product_id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"name\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"description\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                },
                \"price\": {
                  \"type\": \"number\",
                  \"x-antfly-types\": [\"numeric\"]
                }
              },
              \"x-antfly-include-in-all\": [\"name\", \"description\"]
            }
          }
        },
        \"default_type\": \"product\"
      },
      \"indexes\": {
        \"semantic_idx\": {
          \"type\": \"embeddings\",
          \"field\": \"description\",
          \"embedder\": {
            \"provider\": \"ollama\",
            \"model\": \"all-minilm\",
            \"url\": \"http://localhost:11434\"
          }
        }
      }
    }
    ```

    ## Best Practices

    - Define schema for core fields to improve performance
    - Start with fewer shards for small datasets (1-3)
    - Use meaningful table names (e.g., \"products\", \"users\", \"articles\")
    - Consider adding both full-text and vector indexes for hybrid search

    Args:
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Table]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: CreateTableRequest,
) -> Error | Table | None:
    r"""Create a new table

     Creates a new table with optional schema definition, indexes, and configuration.

    ## Use Cases

    **Simple table for unstructured data:**
    ```json
    {
      \"num_shards\": 1
    }
    ```

    **Table with full-text search:**
    ```json
    {
      \"num_shards\": 3,
      \"schema\": {
        \"document_schemas\": {
          \"article\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"title\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"body\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                }
              },
              \"x-antfly-include-in-all\": [\"title\", \"body\"]
            }
          }
        },
        \"default_type\": \"article\"
      },
      \"indexes\": {
        \"search_idx\": {
          \"type\": \"full_text\"
        }
      }
    }
    ```

    **Table with vector similarity search:**
    ```json
    {
      \"num_shards\": 5,
      \"description\": \"Product catalog with semantic search\",
      \"schema\": {
        \"document_schemas\": {
          \"product\": {
            \"schema\": {
              \"type\": \"object\",
              \"properties\": {
                \"product_id\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"keyword\"]
                },
                \"name\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\", \"keyword\"]
                },
                \"description\": {
                  \"type\": \"string\",
                  \"x-antfly-types\": [\"text\"]
                },
                \"price\": {
                  \"type\": \"number\",
                  \"x-antfly-types\": [\"numeric\"]
                }
              },
              \"x-antfly-include-in-all\": [\"name\", \"description\"]
            }
          }
        },
        \"default_type\": \"product\"
      },
      \"indexes\": {
        \"semantic_idx\": {
          \"type\": \"embeddings\",
          \"field\": \"description\",
          \"embedder\": {
            \"provider\": \"ollama\",
            \"model\": \"all-minilm\",
            \"url\": \"http://localhost:11434\"
          }
        }
      }
    }
    ```

    ## Best Practices

    - Define schema for core fields to improve performance
    - Start with fewer shards for small datasets (1-3)
    - Use meaningful table names (e.g., \"products\", \"users\", \"articles\")
    - Consider adding both full-text and vector indexes for hybrid search

    Args:
        table_name (str):
        body (CreateTableRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Table
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
