from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

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
            model (str): The ranking model to use. Default: 'semantic-ranker-default@latest'. Example: semantic-ranker-
                default@latest.
            project_id (Union[Unset, str]): Google Cloud project ID. Falls back to GOOGLE_CLOUD_PROJECT environment
                variable.
            credentials_path (Union[Unset, str]): Path to service account JSON file. Falls back to
                GOOGLE_APPLICATION_CREDENTIALS environment variable.
            top_n (Union[Unset, int]): Maximum number of records to return. If not specified, returns all documents with
                scores.
    """

    model: str = "semantic-ranker-default@latest"
    project_id: Union[Unset, str] = UNSET
    credentials_path: Union[Unset, str] = UNSET
    top_n: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        project_id = self.project_id

        credentials_path = self.credentials_path

        top_n = self.top_n

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
        if top_n is not UNSET:
            field_dict["top_n"] = top_n

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        project_id = d.pop("project_id", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        top_n = d.pop("top_n", UNSET)

        vertex_reranker_config = cls(
            model=model,
            project_id=project_id,
            credentials_path=credentials_path,
            top_n=top_n,
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
