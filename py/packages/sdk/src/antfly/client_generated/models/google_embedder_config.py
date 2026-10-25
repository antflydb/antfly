from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.google_embedder_config_provider import GoogleEmbedderConfigProvider
from ..models.google_embedder_config_request_format import GoogleEmbedderConfigRequestFormat
from ..models.google_embedder_config_task_encoding import GoogleEmbedderConfigTaskEncoding
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.embedding_retrieval_config import EmbeddingRetrievalConfig


T = TypeVar("T", bound="GoogleEmbedderConfig")


@_attrs_define
class GoogleEmbedderConfig:
    """Configuration for the Google AI (Gemini) embedding provider.

    API key via `api_key` field or `GEMINI_API_KEY` environment variable.

    **Example Models:** gemini-embedding-2 (default, 3072 dims)

    **Docs:** https://ai.google.dev/gemini-api/docs/embeddings

        Example:
            {'provider': 'gemini', 'model': 'gemini-embedding-2', 'dimension': 3072, 'api_key': 'your-api-key'}

        Attributes:
            provider (GoogleEmbedderConfigProvider):
            request_format (GoogleEmbedderConfigRequestFormat | Unset): Wire protocol. New configurations default to
                embed_content; existing catalogs retain their pinned protocol. No automatic fallback. Default:
                GoogleEmbedderConfigRequestFormat.EMBED_CONTENT.
            task_encoding (GoogleEmbedderConfigTaskEncoding | Unset): Retrieval task representation, independent of model
                name. Defaults to prompt with embed_content and task_type with legacy protocols. Changing it requires re-
                embedding.
            batch_size (int | Unset): Maximum inputs per upstream request. embed_content requires 1 so independent records
                never become one aggregated embedding.
            project_id (str | Unset): The Google Cloud project ID (optional for Gemini API, required for Vertex AI).
            location (str | Unset): The Google Cloud location (e.g., 'us-central1'). Required for Vertex AI, optional for
                Gemini API.
            model (str | Unset): The name of the embedding model to use. Default: 'gemini-embedding-2'. Example: gemini-
                embedding-2.
            dimension (int | Unset): The dimension of the embedding vector (768, 1536, or 3072 recommended). Default: 3072.
            api_key (str | Unset): The Google API key. Can also be set via GEMINI_API_KEY environment variable.
            url (str | Unset): The URL of the Google API endpoint (optional, uses default if not specified).
            retrieval (EmbeddingRetrievalConfig | Unset): Advanced retrieval-role overrides. Antfly assigns canonical task
                intent
                automatically: semantic-search inputs are `RETRIEVAL_QUERY`, while index
                and artifact writes are `RETRIEVAL_DOCUMENT`. These fields only override
                how a provider or instruction-aware model represents that intent.
    """

    provider: GoogleEmbedderConfigProvider
    request_format: GoogleEmbedderConfigRequestFormat | Unset = GoogleEmbedderConfigRequestFormat.EMBED_CONTENT
    task_encoding: GoogleEmbedderConfigTaskEncoding | Unset = UNSET
    batch_size: int | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    model: str | Unset = "gemini-embedding-2"
    dimension: int | Unset = 3072
    api_key: str | Unset = UNSET
    url: str | Unset = UNSET
    retrieval: EmbeddingRetrievalConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        request_format: str | Unset = UNSET
        if not isinstance(self.request_format, Unset):
            request_format = self.request_format.value

        task_encoding: str | Unset = UNSET
        if not isinstance(self.task_encoding, Unset):
            task_encoding = self.task_encoding.value

        batch_size = self.batch_size

        project_id = self.project_id

        location = self.location

        model = self.model

        dimension = self.dimension

        api_key = self.api_key

        url = self.url

        retrieval: dict[str, Any] | Unset = UNSET
        if not isinstance(self.retrieval, Unset):
            retrieval = self.retrieval.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if request_format is not UNSET:
            field_dict["request_format"] = request_format
        if task_encoding is not UNSET:
            field_dict["task_encoding"] = task_encoding
        if batch_size is not UNSET:
            field_dict["batch_size"] = batch_size
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if model is not UNSET:
            field_dict["model"] = model
        if dimension is not UNSET:
            field_dict["dimension"] = dimension
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if url is not UNSET:
            field_dict["url"] = url
        if retrieval is not UNSET:
            field_dict["retrieval"] = retrieval

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.embedding_retrieval_config import EmbeddingRetrievalConfig

        d = dict(src_dict)
        provider = GoogleEmbedderConfigProvider(d.pop("provider"))

        _request_format = d.pop("request_format", UNSET)
        request_format: GoogleEmbedderConfigRequestFormat | Unset
        if isinstance(_request_format, Unset):
            request_format = UNSET
        else:
            request_format = GoogleEmbedderConfigRequestFormat(_request_format)

        _task_encoding = d.pop("task_encoding", UNSET)
        task_encoding: GoogleEmbedderConfigTaskEncoding | Unset
        if isinstance(_task_encoding, Unset):
            task_encoding = UNSET
        else:
            task_encoding = GoogleEmbedderConfigTaskEncoding(_task_encoding)

        batch_size = d.pop("batch_size", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        model = d.pop("model", UNSET)

        dimension = d.pop("dimension", UNSET)

        api_key = d.pop("api_key", UNSET)

        url = d.pop("url", UNSET)

        _retrieval = d.pop("retrieval", UNSET)
        retrieval: EmbeddingRetrievalConfig | Unset
        if isinstance(_retrieval, Unset):
            retrieval = UNSET
        else:
            retrieval = EmbeddingRetrievalConfig.from_dict(_retrieval)

        google_embedder_config = cls(
            provider=provider,
            request_format=request_format,
            task_encoding=task_encoding,
            batch_size=batch_size,
            project_id=project_id,
            location=location,
            model=model,
            dimension=dimension,
            api_key=api_key,
            url=url,
            retrieval=retrieval,
        )

        google_embedder_config.additional_properties = d
        return google_embedder_config

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
