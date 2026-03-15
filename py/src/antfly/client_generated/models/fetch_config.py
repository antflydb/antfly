from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.credentials import Credentials


T = TypeVar("T", bound="FetchConfig")


@_attrs_define
class FetchConfig:
    """Configuration for URL content fetching.

    Uses lib/scraping for downloading and processing. Supports:
    - HTTP/HTTPS URLs with security validation
    - HTML pages (extracts readable text via go-readability)
    - PDF files (extracts text)
    - Images (returns as data URIs)
    - Plain text files
    - S3 URLs (requires s3_credentials)

    Security features (from lib/scraping.ContentSecurityConfig):
    - Allowed host whitelist
    - Private IP blocking (SSRF prevention)
    - Download size limits
    - Timeout controls

        Attributes:
            s3_credentials (Union[Unset, Credentials]):
            max_content_length (Union[Unset, int]): Maximum content length in characters (truncated if exceeded) Default:
                50000.
            allowed_hosts (Union[Unset, list[str]]): Whitelist of allowed hostnames for fetching.
                If empty, all hosts are allowed (except private IPs).
                Example: ["docs.example.com", "api.example.com"]
            block_private_ips (Union[Unset, bool]): Block requests to private IP ranges (SSRF prevention).
                Blocked: 127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                 Default: True.
            max_download_size_bytes (Union[Unset, int]): Maximum download size in bytes (default: 100MB) Default: 104857600.
            timeout_seconds (Union[Unset, int]): Download timeout in seconds Default: 30.
    """

    s3_credentials: Union[Unset, "Credentials"] = UNSET
    max_content_length: Union[Unset, int] = 50000
    allowed_hosts: Union[Unset, list[str]] = UNSET
    block_private_ips: Union[Unset, bool] = True
    max_download_size_bytes: Union[Unset, int] = 104857600
    timeout_seconds: Union[Unset, int] = 30
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        s3_credentials: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.s3_credentials, Unset):
            s3_credentials = self.s3_credentials.to_dict()

        max_content_length = self.max_content_length

        allowed_hosts: Union[Unset, list[str]] = UNSET
        if not isinstance(self.allowed_hosts, Unset):
            allowed_hosts = self.allowed_hosts

        block_private_ips = self.block_private_ips

        max_download_size_bytes = self.max_download_size_bytes

        timeout_seconds = self.timeout_seconds

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if s3_credentials is not UNSET:
            field_dict["s3_credentials"] = s3_credentials
        if max_content_length is not UNSET:
            field_dict["max_content_length"] = max_content_length
        if allowed_hosts is not UNSET:
            field_dict["allowed_hosts"] = allowed_hosts
        if block_private_ips is not UNSET:
            field_dict["block_private_ips"] = block_private_ips
        if max_download_size_bytes is not UNSET:
            field_dict["max_download_size_bytes"] = max_download_size_bytes
        if timeout_seconds is not UNSET:
            field_dict["timeout_seconds"] = timeout_seconds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.credentials import Credentials

        d = dict(src_dict)
        _s3_credentials = d.pop("s3_credentials", UNSET)
        s3_credentials: Union[Unset, Credentials]
        if isinstance(_s3_credentials, Unset):
            s3_credentials = UNSET
        else:
            s3_credentials = Credentials.from_dict(_s3_credentials)

        max_content_length = d.pop("max_content_length", UNSET)

        allowed_hosts = cast(list[str], d.pop("allowed_hosts", UNSET))

        block_private_ips = d.pop("block_private_ips", UNSET)

        max_download_size_bytes = d.pop("max_download_size_bytes", UNSET)

        timeout_seconds = d.pop("timeout_seconds", UNSET)

        fetch_config = cls(
            s3_credentials=s3_credentials,
            max_content_length=max_content_length,
            allowed_hosts=allowed_hosts,
            block_private_ips=block_private_ips,
            max_download_size_bytes=max_download_size_bytes,
            timeout_seconds=timeout_seconds,
        )

        fetch_config.additional_properties = d
        return fetch_config

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
