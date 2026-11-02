from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.decision_table_source_mime_type import DecisionTableSourceMimeType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_table_source_filter_query import DecisionTableSourceFilterQuery


T = TypeVar("T", bound="DecisionTableSource")


@_attrs_define
class DecisionTableSource:
    """Antfly gateway-only, request-local table image reference. The gateway uses the ingress identity through the ordinary
    query path, checks the current revision, optional source identity, digest, MIME, byte length and pixel bound, then
    materializes provider-compatible image evidence. It never forwards this locator or a credential upstream. The digest
    is not an access grant. Direct inference servers without table access reject this form.

        Attributes:
            table (str):
            document_id (str):
            field (str):
            revision_field (str):
            revision (str):
            sha256 (str):
            mime_type (DecisionTableSourceMimeType):
            byte_length (int):
            identity (str | Unset):
            max_pixels (int | Unset):  Default: 67108864.
            filter_query (DecisionTableSourceFilterQuery | Unset): Additional caller selection, conjoined with the gateway's
                live row policy; never an authorization override.
            permissions_field (str | Unset):
            principals (list[str] | Unset): Optional narrowing check against permissions_field.public/readers; it cannot
                grant table or row access.
    """

    table: str
    document_id: str
    field: str
    revision_field: str
    revision: str
    sha256: str
    mime_type: DecisionTableSourceMimeType
    byte_length: int
    identity: str | Unset = UNSET
    max_pixels: int | Unset = 67108864
    filter_query: DecisionTableSourceFilterQuery | Unset = UNSET
    permissions_field: str | Unset = UNSET
    principals: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        document_id = self.document_id

        field = self.field

        revision_field = self.revision_field

        revision = self.revision

        sha256 = self.sha256

        mime_type = self.mime_type.value

        byte_length = self.byte_length

        identity = self.identity

        max_pixels = self.max_pixels

        filter_query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_query, Unset):
            filter_query = self.filter_query.to_dict()

        permissions_field = self.permissions_field

        principals: list[str] | Unset = UNSET
        if not isinstance(self.principals, Unset):
            principals = self.principals

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table": table,
                "document_id": document_id,
                "field": field,
                "revision_field": revision_field,
                "revision": revision,
                "sha256": sha256,
                "mime_type": mime_type,
                "byte_length": byte_length,
            }
        )
        if identity is not UNSET:
            field_dict["identity"] = identity
        if max_pixels is not UNSET:
            field_dict["max_pixels"] = max_pixels
        if filter_query is not UNSET:
            field_dict["filter_query"] = filter_query
        if permissions_field is not UNSET:
            field_dict["permissions_field"] = permissions_field
        if principals is not UNSET:
            field_dict["principals"] = principals

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_table_source_filter_query import DecisionTableSourceFilterQuery

        d = dict(src_dict)
        table = d.pop("table")

        document_id = d.pop("document_id")

        field = d.pop("field")

        revision_field = d.pop("revision_field")

        revision = d.pop("revision")

        sha256 = d.pop("sha256")

        mime_type = DecisionTableSourceMimeType(d.pop("mime_type"))

        byte_length = d.pop("byte_length")

        identity = d.pop("identity", UNSET)

        max_pixels = d.pop("max_pixels", UNSET)

        _filter_query = d.pop("filter_query", UNSET)
        filter_query: DecisionTableSourceFilterQuery | Unset
        if isinstance(_filter_query, Unset):
            filter_query = UNSET
        else:
            filter_query = DecisionTableSourceFilterQuery.from_dict(_filter_query)

        permissions_field = d.pop("permissions_field", UNSET)

        principals = cast(list[str], d.pop("principals", UNSET))

        decision_table_source = cls(
            table=table,
            document_id=document_id,
            field=field,
            revision_field=revision_field,
            revision=revision,
            sha256=sha256,
            mime_type=mime_type,
            byte_length=byte_length,
            identity=identity,
            max_pixels=max_pixels,
            filter_query=filter_query,
            permissions_field=permissions_field,
            principals=principals,
        )

        return decision_table_source
