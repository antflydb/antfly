from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_provider import ExtractionProvider
from ..models.extraction_schema_version import ExtractionSchemaVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_options import ExtractionOptions
    from ..models.extraction_schema import ExtractionSchema


T = TypeVar("T", bound="ExtractionConfig")


@_attrs_define
class ExtractionConfig:
    """
    Attributes:
        provider (ExtractionProvider):
        model (str | Unset):
        schema_version (ExtractionSchemaVersion | Unset): Omission preserves the legacy extraction contract. Version 2
            opts into strict mixed-task schemas, per-input replacements and explicit offsets; the selected model/runtime
            must support every requested feature.
        url (str | Unset):
        api_url (str | Unset):
        project_id (str | Unset): Google Cloud project for the vertex provider.
        location (str | Unset): Vertex AI location.
        region (str | Unset): AWS region for the bedrock provider.
        credentials_path (str | Unset): Path to Google service account credentials for the vertex provider.
        account_id (str | Unset): Cloudflare Workers AI account identifier.
        gateway_id (str | Unset): Optional Cloudflare AI Gateway identifier.
        api_key (str | Unset):
        bearer_token (str | Unset):
        schema (ExtractionSchema | Unset): Version 1 selects one extraction family; entities may accompany relations.
            With schema_version 2, entities, attributes, classifications, structures,
            and ordinary relations may share one encoded input. joint_ie is a separate,
            mutually exclusive typed graph schema. The version 2 compiler rejects
            unknown fields and validates all references before model execution.
        options (ExtractionOptions | Unset):
    """

    provider: ExtractionProvider
    model: str | Unset = UNSET
    schema_version: ExtractionSchemaVersion | Unset = UNSET
    url: str | Unset = UNSET
    api_url: str | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    region: str | Unset = UNSET
    credentials_path: str | Unset = UNSET
    account_id: str | Unset = UNSET
    gateway_id: str | Unset = UNSET
    api_key: str | Unset = UNSET
    bearer_token: str | Unset = UNSET
    schema: ExtractionSchema | Unset = UNSET
    options: ExtractionOptions | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        schema_version: int | Unset = UNSET
        if not isinstance(self.schema_version, Unset):
            schema_version = self.schema_version.value

        url = self.url

        api_url = self.api_url

        project_id = self.project_id

        location = self.location

        region = self.region

        credentials_path = self.credentials_path

        account_id = self.account_id

        gateway_id = self.gateway_id

        api_key = self.api_key

        bearer_token = self.bearer_token

        schema: dict[str, Any] | Unset = UNSET
        if not isinstance(self.schema, Unset):
            schema = self.schema.to_dict()

        options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.options, Unset):
            options = self.options.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if model is not UNSET:
            field_dict["model"] = model
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
        if url is not UNSET:
            field_dict["url"] = url
        if api_url is not UNSET:
            field_dict["api_url"] = api_url
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if region is not UNSET:
            field_dict["region"] = region
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
        if account_id is not UNSET:
            field_dict["account_id"] = account_id
        if gateway_id is not UNSET:
            field_dict["gateway_id"] = gateway_id
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if bearer_token is not UNSET:
            field_dict["bearer_token"] = bearer_token
        if schema is not UNSET:
            field_dict["schema"] = schema
        if options is not UNSET:
            field_dict["options"] = options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_options import ExtractionOptions
        from ..models.extraction_schema import ExtractionSchema

        d = dict(src_dict)
        provider = ExtractionProvider(d.pop("provider"))

        model = d.pop("model", UNSET)

        _schema_version = d.pop("schema_version", UNSET)
        schema_version: ExtractionSchemaVersion | Unset
        if isinstance(_schema_version, Unset):
            schema_version = UNSET
        else:
            schema_version = ExtractionSchemaVersion(_schema_version)

        url = d.pop("url", UNSET)

        api_url = d.pop("api_url", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        region = d.pop("region", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        account_id = d.pop("account_id", UNSET)

        gateway_id = d.pop("gateway_id", UNSET)

        api_key = d.pop("api_key", UNSET)

        bearer_token = d.pop("bearer_token", UNSET)

        _schema = d.pop("schema", UNSET)
        schema: ExtractionSchema | Unset
        if isinstance(_schema, Unset):
            schema = UNSET
        else:
            schema = ExtractionSchema.from_dict(_schema)

        _options = d.pop("options", UNSET)
        options: ExtractionOptions | Unset
        if isinstance(_options, Unset):
            options = UNSET
        else:
            options = ExtractionOptions.from_dict(_options)

        extraction_config = cls(
            provider=provider,
            model=model,
            schema_version=schema_version,
            url=url,
            api_url=api_url,
            project_id=project_id,
            location=location,
            region=region,
            credentials_path=credentials_path,
            account_id=account_id,
            gateway_id=gateway_id,
            api_key=api_key,
            bearer_token=bearer_token,
            schema=schema,
            options=options,
        )

        extraction_config.additional_properties = d
        return extraction_config

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
