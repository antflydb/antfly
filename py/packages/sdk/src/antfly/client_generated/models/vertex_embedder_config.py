from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.vertex_embedder_config_provider import VertexEmbedderConfigProvider
from ..models.vertex_embedder_config_request_format import VertexEmbedderConfigRequestFormat
from ..models.vertex_embedder_config_task_encoding import VertexEmbedderConfigTaskEncoding
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.embedding_retrieval_config import EmbeddingRetrievalConfig


T = TypeVar("T", bound="VertexEmbedderConfig")


@_attrs_define
class VertexEmbedderConfig:
    """Configuration for Google Cloud Vertex AI embedding models (enterprise-grade).

    Uses Application Default Credentials (ADC) for authentication. Requires IAM role `roles/aiplatform.user`.

    **Example Model:** gemini-embedding-2 (default, 3072 dims)

    Antfly's Vertex embedder currently supports text inputs. Binary media is rejected
    instead of being flattened or silently discarded.

    **Docs:** https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings

        Example:
            {'provider': 'vertex', 'model': 'gemini-embedding-2', 'project_id': 'my-gcp-project', 'location': 'global',
                'dimension': 3072}

        Attributes:
            provider (VertexEmbedderConfigProvider):
            request_format (VertexEmbedderConfigRequestFormat | Unset): Wire protocol. New configurations default to
                embed_content; existing catalogs retain their pinned protocol. No automatic fallback. Default:
                VertexEmbedderConfigRequestFormat.EMBED_CONTENT.
            task_encoding (VertexEmbedderConfigTaskEncoding | Unset): Retrieval task representation, independent of model
                name. Defaults to prompt with embed_content and task_type with legacy protocols. Changing it requires re-
                embedding.
            batch_size (int | Unset): Maximum inputs per upstream request. embed_content requires 1 so independent records
                never become one aggregated embedding.
            model (str | Unset): The name of the Vertex AI embedding model to use. Default: 'gemini-embedding-2'. Example:
                gemini-embedding-2.
            project_id (str | Unset): Google Cloud project ID. Can also be set via GOOGLE_CLOUD_PROJECT environment
                variable.
            location (str | Unset): Google Cloud region for Vertex AI API (e.g., 'us-central1', 'europe-west1'). Can also be
                set via GOOGLE_CLOUD_LOCATION. Defaults to 'global' for new configurations. Default: 'global'.
            credentials_path (str | Unset): Path to an ADC credential JSON file (service-account, authorized-user, or
                external-account). Alternative to the default ADC chain.
            dimension (int | Unset): The dimension of the embedding vector (768, 1536, or 3072 for gemini-embedding-2).
                Default: 3072.
            retrieval (EmbeddingRetrievalConfig | Unset): Advanced retrieval-role overrides. Antfly assigns canonical task
                intent
                automatically: semantic-search inputs are `RETRIEVAL_QUERY`, while index
                and artifact writes are `RETRIEVAL_DOCUMENT`. These fields only override
                how a provider or instruction-aware model represents that intent.
    """

    provider: VertexEmbedderConfigProvider
    request_format: VertexEmbedderConfigRequestFormat | Unset = VertexEmbedderConfigRequestFormat.EMBED_CONTENT
    task_encoding: VertexEmbedderConfigTaskEncoding | Unset = UNSET
    batch_size: int | Unset = UNSET
    model: str | Unset = "gemini-embedding-2"
    project_id: str | Unset = UNSET
    location: str | Unset = "global"
    credentials_path: str | Unset = UNSET
    dimension: int | Unset = 3072
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

        model = self.model

        project_id = self.project_id

        location = self.location

        credentials_path = self.credentials_path

        dimension = self.dimension

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
        if model is not UNSET:
            field_dict["model"] = model
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
        if dimension is not UNSET:
            field_dict["dimension"] = dimension
        if retrieval is not UNSET:
            field_dict["retrieval"] = retrieval

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.embedding_retrieval_config import EmbeddingRetrievalConfig

        d = dict(src_dict)
        provider = VertexEmbedderConfigProvider(d.pop("provider"))

        _request_format = d.pop("request_format", UNSET)
        request_format: VertexEmbedderConfigRequestFormat | Unset
        if isinstance(_request_format, Unset):
            request_format = UNSET
        else:
            request_format = VertexEmbedderConfigRequestFormat(_request_format)

        _task_encoding = d.pop("task_encoding", UNSET)
        task_encoding: VertexEmbedderConfigTaskEncoding | Unset
        if isinstance(_task_encoding, Unset):
            task_encoding = UNSET
        else:
            task_encoding = VertexEmbedderConfigTaskEncoding(_task_encoding)

        batch_size = d.pop("batch_size", UNSET)

        model = d.pop("model", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        dimension = d.pop("dimension", UNSET)

        _retrieval = d.pop("retrieval", UNSET)
        retrieval: EmbeddingRetrievalConfig | Unset
        if isinstance(_retrieval, Unset):
            retrieval = UNSET
        else:
            retrieval = EmbeddingRetrievalConfig.from_dict(_retrieval)

        vertex_embedder_config = cls(
            provider=provider,
            request_format=request_format,
            task_encoding=task_encoding,
            batch_size=batch_size,
            model=model,
            project_id=project_id,
            location=location,
            credentials_path=credentials_path,
            dimension=dimension,
            retrieval=retrieval,
        )

        vertex_embedder_config.additional_properties = d
        return vertex_embedder_config

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
