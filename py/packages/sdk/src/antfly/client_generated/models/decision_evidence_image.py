from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.decision_evidence_image_detail import DecisionEvidenceImageDetail
from ..models.decision_evidence_image_type import DecisionEvidenceImageType
from ..types import UNSET, Unset

T = TypeVar("T", bound="DecisionEvidenceImage")


@_attrs_define
class DecisionEvidenceImage:
    """
    Attributes:
        type_ (DecisionEvidenceImageType):
        image_url (str): Base64 data URL (image/png, image/jpeg, image/webp, image/gif), or a public HTTP(S) URL for
            OpenAI. Antfly never fetches image URLs for decision adapters.
        detail (DecisionEvidenceImageDetail | Unset):  Default: DecisionEvidenceImageDetail.AUTO.
    """

    type_: DecisionEvidenceImageType
    image_url: str
    detail: DecisionEvidenceImageDetail | Unset = DecisionEvidenceImageDetail.AUTO

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        image_url = self.image_url

        detail: str | Unset = UNSET
        if not isinstance(self.detail, Unset):
            detail = self.detail.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "image_url": image_url,
            }
        )
        if detail is not UNSET:
            field_dict["detail"] = detail

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = DecisionEvidenceImageType(d.pop("type"))

        image_url = d.pop("image_url")

        _detail = d.pop("detail", UNSET)
        detail: DecisionEvidenceImageDetail | Unset
        if isinstance(_detail, Unset):
            detail = UNSET
        else:
            detail = DecisionEvidenceImageDetail(_detail)

        decision_evidence_image = cls(
            type_=type_,
            image_url=image_url,
            detail=detail,
        )

        return decision_evidence_image
