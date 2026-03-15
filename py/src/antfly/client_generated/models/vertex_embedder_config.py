from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="VertexEmbedderConfig")


@_attrs_define
class VertexEmbedderConfig:
    """Configuration for Google Cloud Vertex AI embedding models (enterprise-grade).

    Uses Application Default Credentials (ADC) for authentication. Requires IAM role `roles/aiplatform.user`.

    **Example Models:** gemini-embedding-001 (default, 3072 dims), multimodalembedding (images/audio/video)

    **Docs:** https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings

        Example:
            {'provider': 'vertex', 'model': 'gemini-embedding-001', 'project_id': 'my-gcp-project', 'location': 'us-
                central1', 'dimension': 3072}

        Attributes:
            model (str): The name of the Vertex AI embedding model to use. Default: 'gemini-embedding-001'. Example: gemini-
                embedding-001.
            project_id (Union[Unset, str]): Google Cloud project ID. Can also be set via GOOGLE_CLOUD_PROJECT environment
                variable.
            location (Union[Unset, str]): Google Cloud region for Vertex AI API (e.g., 'us-central1', 'europe-west1'). Can
                also be set via GOOGLE_CLOUD_LOCATION. Defaults to 'us-central1'. Default: 'us-central1'.
            credentials_path (Union[Unset, str]): Path to service account JSON key file. Alternative to ADC for non-GCP
                environments.
            dimension (Union[Unset, int]): The dimension of the embedding vector (768, 1536, or 3072 for gemini-
                embedding-001; 128-1408 for multimodalembedding). Default: 3072.
    """

    model: str = "gemini-embedding-001"
    project_id: Union[Unset, str] = UNSET
    location: Union[Unset, str] = "us-central1"
    credentials_path: Union[Unset, str] = UNSET
    dimension: Union[Unset, int] = 3072
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        project_id = self.project_id

        location = self.location

        credentials_path = self.credentials_path

        dimension = self.dimension

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
        if dimension is not UNSET:
            field_dict["dimension"] = dimension

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        dimension = d.pop("dimension", UNSET)

        vertex_embedder_config = cls(
            model=model,
            project_id=project_id,
            location=location,
            credentials_path=credentials_path,
            dimension=dimension,
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
