from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.query_request_grep_scan_version import QueryRequestGrepScanVersion

T = TypeVar("T", bound="QueryRequestGrepScan")


@_attrs_define
class QueryRequestGrepScan:
    """Opt-in v1 exact grep miss accounting for ascending _id snapshot scans.
    Necessary UTF-8 byte literal alternatives permit the server to omit only
    validated nonmatching content. Every document retains its ordered hit,
    metadata and permissions. Server-owned _computed.antfly_grep_miss_v1
    binds request identity, snapshot, ID, revision and original byte count.
    Unsupported or malformed documents retain their complete bodies.
    This reduces wire bytes, not storage scanning. Clients must verify the
    accounting proof before skipping lexical evaluation. Requires stored
    sources; incompatible with graph, hierarchy or evaluation queries.

        Attributes:
            version (QueryRequestGrepScanVersion):
            document_field (str):
            alternatives (list[str]): Necessary byte literals; total UTF-8 bytes may not exceed 16384.
            max_content_bytes (int):
    """

    version: QueryRequestGrepScanVersion
    document_field: str
    alternatives: list[str]
    max_content_bytes: int

    def to_dict(self) -> dict[str, Any]:
        version = self.version.value

        document_field = self.document_field

        alternatives = self.alternatives

        max_content_bytes = self.max_content_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "version": version,
                "document_field": document_field,
                "alternatives": alternatives,
                "max_content_bytes": max_content_bytes,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        version = QueryRequestGrepScanVersion(d.pop("version"))

        document_field = d.pop("document_field")

        alternatives = cast(list[str], d.pop("alternatives"))

        max_content_bytes = d.pop("max_content_bytes")

        query_request_grep_scan = cls(
            version=version,
            document_field=document_field,
            alternatives=alternatives,
            max_content_bytes=max_content_bytes,
        )

        return query_request_grep_scan
