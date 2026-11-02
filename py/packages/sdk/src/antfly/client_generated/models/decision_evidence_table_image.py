from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.decision_evidence_table_image_detail import DecisionEvidenceTableImageDetail
from ..models.decision_evidence_table_image_type import DecisionEvidenceTableImageType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_table_source import DecisionTableSource


T = TypeVar("T", bound="DecisionEvidenceTableImage")


@_attrs_define
class DecisionEvidenceTableImage:
    """
    Attributes:
        type_ (DecisionEvidenceTableImageType):
        source (DecisionTableSource): Antfly gateway-only, request-local table image reference. The gateway uses the
            ingress identity through the ordinary query path, checks the current revision, optional source identity, digest,
            MIME, byte length and pixel bound, then materializes provider-compatible image evidence. It never forwards this
            locator or a credential upstream. The digest is not an access grant. Direct inference servers without table
            access reject this form.
        detail (DecisionEvidenceTableImageDetail | Unset):  Default: DecisionEvidenceTableImageDetail.AUTO.
    """

    type_: DecisionEvidenceTableImageType
    source: DecisionTableSource
    detail: DecisionEvidenceTableImageDetail | Unset = DecisionEvidenceTableImageDetail.AUTO

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        source = self.source.to_dict()

        detail: str | Unset = UNSET
        if not isinstance(self.detail, Unset):
            detail = self.detail.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "source": source,
            }
        )
        if detail is not UNSET:
            field_dict["detail"] = detail

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_table_source import DecisionTableSource

        d = dict(src_dict)
        type_ = DecisionEvidenceTableImageType(d.pop("type"))

        source = DecisionTableSource.from_dict(d.pop("source"))

        _detail = d.pop("detail", UNSET)
        detail: DecisionEvidenceTableImageDetail | Unset
        if isinstance(_detail, Unset):
            detail = UNSET
        else:
            detail = DecisionEvidenceTableImageDetail(_detail)

        decision_evidence_table_image = cls(
            type_=type_,
            source=source,
            detail=detail,
        )

        return decision_evidence_table_image
