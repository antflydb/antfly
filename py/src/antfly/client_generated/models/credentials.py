from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="Credentials")


@_attrs_define
class Credentials:
    """
    Attributes:
        endpoint (Union[Unset, str]): S3-compatible endpoint (e.g., 's3.amazonaws.com' or 'localhost:9000' for MinIO)
            Example: s3.amazonaws.com.
        use_ssl (Union[Unset, bool]): Enable SSL/TLS for S3 connections (default: true for AWS, false for local MinIO)
            Default: True.
        access_key_id (Union[Unset, str]): AWS access key ID. Supports keystore syntax for secret lookup. Falls back to
            AWS_ACCESS_KEY_ID environment variable if not set. Example: your-access-key-id.
        secret_access_key (Union[Unset, str]): AWS secret access key. Supports keystore syntax for secret lookup. Falls
            back to AWS_SECRET_ACCESS_KEY environment variable if not set. Example: your-secret-access-key.
        session_token (Union[Unset, str]): Optional AWS session token for temporary credentials. Supports keystore
            syntax for secret lookup. Example: your-session-token.
    """

    endpoint: Union[Unset, str] = UNSET
    use_ssl: Union[Unset, bool] = True
    access_key_id: Union[Unset, str] = UNSET
    secret_access_key: Union[Unset, str] = UNSET
    session_token: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        endpoint = self.endpoint

        use_ssl = self.use_ssl

        access_key_id = self.access_key_id

        secret_access_key = self.secret_access_key

        session_token = self.session_token

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if endpoint is not UNSET:
            field_dict["endpoint"] = endpoint
        if use_ssl is not UNSET:
            field_dict["use_ssl"] = use_ssl
        if access_key_id is not UNSET:
            field_dict["access_key_id"] = access_key_id
        if secret_access_key is not UNSET:
            field_dict["secret_access_key"] = secret_access_key
        if session_token is not UNSET:
            field_dict["session_token"] = session_token

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        endpoint = d.pop("endpoint", UNSET)

        use_ssl = d.pop("use_ssl", UNSET)

        access_key_id = d.pop("access_key_id", UNSET)

        secret_access_key = d.pop("secret_access_key", UNSET)

        session_token = d.pop("session_token", UNSET)

        credentials = cls(
            endpoint=endpoint,
            use_ssl=use_ssl,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

        credentials.additional_properties = d
        return credentials

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
