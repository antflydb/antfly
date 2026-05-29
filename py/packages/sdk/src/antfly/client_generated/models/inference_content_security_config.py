from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceContentSecurityConfig")


@_attrs_define
class InferenceContentSecurityConfig:
    """
    Attributes:
        allowed_hosts (list[str] | Unset): Whitelist of allowed hostnames/IPs for link downloads. If empty, all hosts
            are allowed (except private IPs if block_private_ips is true). Example: ['example.com', 'cdn.example.com',
            '192.0.2.1'].
        block_private_ips (bool | Unset): Block requests to private IP ranges (127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12,
            192.168.0.0/16, 169.254.0.0/16) Default: True.
        max_download_size_bytes (int | Unset): Maximum size of downloaded content in bytes Default: 104857600. Example:
            104857600.
        download_timeout_seconds (int | Unset): Timeout for individual download operations in seconds Default: 30.
            Example: 30.
        max_image_dimension (int | Unset): Maximum image width/height in pixels (images will be resized) Default: 2048.
            Example: 2048.
        allowed_paths (list[str] | Unset): Whitelist of allowed path prefixes for file:// and s3:// URLs. If empty, all
            paths are allowed. For file:// use absolute paths (e.g., /Users/data/). For s3:// use bucket/prefix (e.g., my-
            bucket/uploads/). Example: ['/Users/data/', 'my-bucket/uploads/'].
        user_agent (str | Unset): User-Agent header for HTTP downloads. Defaults to 'AntflyDB/1.0' if not set. Some
            servers (e.g., Wikipedia) reject requests without a User-Agent. Example: AntflyDB/1.0.
    """

    allowed_hosts: list[str] | Unset = UNSET
    block_private_ips: bool | Unset = True
    max_download_size_bytes: int | Unset = 104857600
    download_timeout_seconds: int | Unset = 30
    max_image_dimension: int | Unset = 2048
    allowed_paths: list[str] | Unset = UNSET
    user_agent: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        allowed_hosts: list[str] | Unset = UNSET
        if not isinstance(self.allowed_hosts, Unset):
            allowed_hosts = self.allowed_hosts

        block_private_ips = self.block_private_ips

        max_download_size_bytes = self.max_download_size_bytes

        download_timeout_seconds = self.download_timeout_seconds

        max_image_dimension = self.max_image_dimension

        allowed_paths: list[str] | Unset = UNSET
        if not isinstance(self.allowed_paths, Unset):
            allowed_paths = self.allowed_paths

        user_agent = self.user_agent

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if allowed_hosts is not UNSET:
            field_dict["allowed_hosts"] = allowed_hosts
        if block_private_ips is not UNSET:
            field_dict["block_private_ips"] = block_private_ips
        if max_download_size_bytes is not UNSET:
            field_dict["max_download_size_bytes"] = max_download_size_bytes
        if download_timeout_seconds is not UNSET:
            field_dict["download_timeout_seconds"] = download_timeout_seconds
        if max_image_dimension is not UNSET:
            field_dict["max_image_dimension"] = max_image_dimension
        if allowed_paths is not UNSET:
            field_dict["allowed_paths"] = allowed_paths
        if user_agent is not UNSET:
            field_dict["user_agent"] = user_agent

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        allowed_hosts = cast(list[str], d.pop("allowed_hosts", UNSET))

        block_private_ips = d.pop("block_private_ips", UNSET)

        max_download_size_bytes = d.pop("max_download_size_bytes", UNSET)

        download_timeout_seconds = d.pop("download_timeout_seconds", UNSET)

        max_image_dimension = d.pop("max_image_dimension", UNSET)

        allowed_paths = cast(list[str], d.pop("allowed_paths", UNSET))

        user_agent = d.pop("user_agent", UNSET)

        inference_content_security_config = cls(
            allowed_hosts=allowed_hosts,
            block_private_ips=block_private_ips,
            max_download_size_bytes=max_download_size_bytes,
            download_timeout_seconds=download_timeout_seconds,
            max_image_dimension=max_image_dimension,
            allowed_paths=allowed_paths,
            user_agent=user_agent,
        )

        inference_content_security_config.additional_properties = d
        return inference_content_security_config

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
