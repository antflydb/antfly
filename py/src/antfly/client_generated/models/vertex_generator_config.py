from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="VertexGeneratorConfig")


@_attrs_define
class VertexGeneratorConfig:
    """Configuration for Google Cloud Vertex AI generative models (enterprise-grade).

    Uses Application Default Credentials (ADC) for authentication. In GCP environments
    (Cloud Run, GKE, Compute Engine) this is automatic. For local dev, run
    `gcloud auth application-default login`. Requires IAM role `roles/aiplatform.user`.

    **Example Models:** gemini-2.5-flash (default), gemini-2.5-pro, gemini-3.0-pro

    **Docs:** https://cloud.google.com/vertex-ai/generative-ai/docs/learn/models

        Example:
            {'provider': 'vertex', 'model': 'gemini-2.5-flash', 'project_id': 'my-gcp-project', 'location': 'us-central1',
                'temperature': 0.7, 'max_tokens': 4096}

        Attributes:
            model (str): The name of the Vertex AI model to use. Default: 'gemini-2.5-flash'. Example: gemini-2.5-flash.
            project_id (Union[Unset, str]): Google Cloud project ID. Can also be set via GOOGLE_CLOUD_PROJECT environment
                variable.
            location (Union[Unset, str]): Google Cloud region for Vertex AI API (e.g., 'us-central1', 'europe-west1'). Can
                also be set via GOOGLE_CLOUD_LOCATION. Defaults to 'us-central1'. Default: 'us-central1'.
            credentials_path (Union[Unset, str]): Path to service account JSON key file. Sets GOOGLE_APPLICATION_CREDENTIALS
                environment variable. Alternative to ADC for non-GCP environments.
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-2.0). Higher values make output more
                random.
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate in the response.
            top_p (Union[Unset, float]): Nucleus sampling parameter (0.0-1.0). Alternative to temperature.
            top_k (Union[Unset, int]): Top-k sampling parameter. Only sample from the top K options for each subsequent
                token.
    """

    model: str = "gemini-2.5-flash"
    project_id: Union[Unset, str] = UNSET
    location: Union[Unset, str] = "us-central1"
    credentials_path: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    top_k: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        project_id = self.project_id

        location = self.location

        credentials_path = self.credentials_path

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        top_k = self.top_k

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
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if top_k is not UNSET:
            field_dict["top_k"] = top_k

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        vertex_generator_config = cls(
            model=model,
            project_id=project_id,
            location=location,
            credentials_path=credentials_path,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
        )

        vertex_generator_config.additional_properties = d
        return vertex_generator_config

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
