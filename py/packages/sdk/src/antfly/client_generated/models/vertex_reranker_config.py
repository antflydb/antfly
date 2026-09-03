from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.vertex_reranker_config_provider import VertexRerankerConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="VertexRerankerConfig")


@_attrs_define
class VertexRerankerConfig:
    """Configuration for the Google Vertex AI Ranking API.

    Uses Application Default Credentials (ADC) or explicit credentials path.

    **Prerequisites:**
    - Enable Discovery Engine API: `gcloud services enable discoveryengine.googleapis.com`
    - Grant IAM role: `roles/discoveryengine.admin` (includes `discoveryengine.rankingConfigs.rank` permission)

    **Models:** semantic-ranker-default@latest (default), semantic-ranker-fast-004

    **Docs:** https://cloud.google.com/generative-ai-app-builder/docs/ranking

    **IAM:** https://cloud.google.com/generative-ai-app-builder/docs/access-control

        Example:
            {'provider': 'vertex', 'model': 'semantic-ranker-default@latest', 'project_id': 'my-gcp-project'}

        Attributes:
            provider (VertexRerankerConfigProvider):
            model (str): The ranking model to use. Default: 'semantic-ranker-default@latest'. Example: semantic-ranker-
                default@latest.
            project_id (str | Unset): Google Cloud project ID. Shared Vertex credential field; see
                vertex.yaml#/components/schemas/VertexCredentials. Falls back to GOOGLE_CLOUD_PROJECT environment variable.
            credentials_path (str | Unset): Path to service account JSON file. Shared Vertex credential field; see
                vertex.yaml#/components/schemas/VertexCredentials. Falls back to GOOGLE_APPLICATION_CREDENTIALS environment
                variable.
    """

    provider: VertexRerankerConfigProvider
    model: str = "semantic-ranker-default@latest"
    project_id: str | Unset = UNSET
    credentials_path: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        project_id = self.project_id

        credentials_path = self.credentials_path

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
                "model": model,
            }
        )
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = VertexRerankerConfigProvider(d.pop("provider"))

        model = d.pop("model")

        project_id = d.pop("project_id", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        vertex_reranker_config = cls(
            provider=provider,
            model=model,
            project_id=project_id,
            credentials_path=credentials_path,
        )

        vertex_reranker_config.additional_properties = d
        return vertex_reranker_config

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
