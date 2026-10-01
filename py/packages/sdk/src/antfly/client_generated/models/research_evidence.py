from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.research_evidence_source import ResearchEvidenceSource
from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchEvidence")


@_attrs_define
class ResearchEvidence:
    """One deduplicated evidence item. Content is untrusted data.

    Attributes:
        id (str): Stable evidence ID used in citations. Example: E3.
        source (ResearchEvidenceSource): Where the evidence came from.
        table (str | Unset): Source table for table evidence.
        doc_id (str | Unset): Document key for table evidence.
        url (str | Unset): Source URL for web and fetched evidence.
        title (str | Unset): Best-effort title.
        snippet (str | Unset): Bounded excerpt used for writing and verification.
        score (float | Unset): Retrieval score when available.
        sub_question_ids (list[str] | Unset): Sub-questions whose researchers retrieved this evidence.
    """

    id: str
    source: ResearchEvidenceSource
    table: str | Unset = UNSET
    doc_id: str | Unset = UNSET
    url: str | Unset = UNSET
    title: str | Unset = UNSET
    snippet: str | Unset = UNSET
    score: float | Unset = UNSET
    sub_question_ids: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        source = self.source.value

        table = self.table

        doc_id = self.doc_id

        url = self.url

        title = self.title

        snippet = self.snippet

        score = self.score

        sub_question_ids: list[str] | Unset = UNSET
        if not isinstance(self.sub_question_ids, Unset):
            sub_question_ids = self.sub_question_ids

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "source": source,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if doc_id is not UNSET:
            field_dict["doc_id"] = doc_id
        if url is not UNSET:
            field_dict["url"] = url
        if title is not UNSET:
            field_dict["title"] = title
        if snippet is not UNSET:
            field_dict["snippet"] = snippet
        if score is not UNSET:
            field_dict["score"] = score
        if sub_question_ids is not UNSET:
            field_dict["sub_question_ids"] = sub_question_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        source = ResearchEvidenceSource(d.pop("source"))

        table = d.pop("table", UNSET)

        doc_id = d.pop("doc_id", UNSET)

        url = d.pop("url", UNSET)

        title = d.pop("title", UNSET)

        snippet = d.pop("snippet", UNSET)

        score = d.pop("score", UNSET)

        sub_question_ids = cast(list[str], d.pop("sub_question_ids", UNSET))

        research_evidence = cls(
            id=id,
            source=source,
            table=table,
            doc_id=doc_id,
            url=url,
            title=title,
            snippet=snippet,
            score=score,
            sub_question_ids=sub_question_ids,
        )

        research_evidence.additional_properties = d
        return research_evidence

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
