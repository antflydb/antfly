from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.read_requirements_visibility import ReadRequirementsVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.read_receipt import ReadReceipt


T = TypeVar("T", bound="ReadRequirements")


@_attrs_define
class ReadRequirements:
    """Freshness requirements for writable Iceberg reads. Search supports accepted and published (default accepted); SQL
    supports accepted and committed (default committed). Unsupported visibility modes fail explicitly. Cannot be
    combined with legacy lake_read or lake_visibility. A pinned transaction snapshot never advances to satisfy a newer
    receipt.

        Attributes:
            visibility (ReadRequirementsVisibility | Unset): Accepted includes durable WAL writes; committed reads an
                Iceberg commit; published reads the search index generation. These are distinct visibility stages.
            after_write (ReadReceipt | Unset): Opaque write receipt returned by ingestion. Pass it back unchanged to require
                a read including that write; it is a minimum freshness frontier, not an exact snapshot.
            wait_ms (int | Unset): Readiness wait budget within the query or statement timeout. Defaults to 5000 ms when
                requirements are supplied; zero fails immediately if coverage is unavailable.
    """

    visibility: ReadRequirementsVisibility | Unset = UNSET
    after_write: ReadReceipt | Unset = UNSET
    wait_ms: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        after_write: dict[str, Any] | Unset = UNSET
        if not isinstance(self.after_write, Unset):
            after_write = self.after_write.to_dict()

        wait_ms = self.wait_ms

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if after_write is not UNSET:
            field_dict["after_write"] = after_write
        if wait_ms is not UNSET:
            field_dict["wait_ms"] = wait_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.read_receipt import ReadReceipt

        d = dict(src_dict)
        _visibility = d.pop("visibility", UNSET)
        visibility: ReadRequirementsVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = ReadRequirementsVisibility(_visibility)

        _after_write = d.pop("after_write", UNSET)
        after_write: ReadReceipt | Unset
        if isinstance(_after_write, Unset):
            after_write = UNSET
        else:
            after_write = ReadReceipt.from_dict(_after_write)

        wait_ms = d.pop("wait_ms", UNSET)

        read_requirements = cls(
            visibility=visibility,
            after_write=after_write,
            wait_ms=wait_ms,
        )

        return read_requirements
