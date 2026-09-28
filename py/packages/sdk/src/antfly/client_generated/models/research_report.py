from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_report_section import ResearchReportSection


T = TypeVar("T", bound="ResearchReport")


@_attrs_define
class ResearchReport:
    """
    Attributes:
        markdown (str): The full report rendered as markdown, with a sources list.
        title (str | Unset): Report title.
        summary (str | Unset): Executive summary.
        sections (list[ResearchReportSection] | Unset): Report sections.
    """

    markdown: str
    title: str | Unset = UNSET
    summary: str | Unset = UNSET
    sections: list[ResearchReportSection] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        markdown = self.markdown

        title = self.title

        summary = self.summary

        sections: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sections, Unset):
            sections = []
            for sections_item_data in self.sections:
                sections_item = sections_item_data.to_dict()
                sections.append(sections_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "markdown": markdown,
            }
        )
        if title is not UNSET:
            field_dict["title"] = title
        if summary is not UNSET:
            field_dict["summary"] = summary
        if sections is not UNSET:
            field_dict["sections"] = sections

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_report_section import ResearchReportSection

        d = dict(src_dict)
        markdown = d.pop("markdown")

        title = d.pop("title", UNSET)

        summary = d.pop("summary", UNSET)

        _sections = d.pop("sections", UNSET)
        sections: list[ResearchReportSection] | Unset = UNSET
        if _sections is not UNSET:
            sections = []
            for sections_item_data in _sections:
                sections_item = ResearchReportSection.from_dict(sections_item_data)

                sections.append(sections_item)

        research_report = cls(
            markdown=markdown,
            title=title,
            summary=summary,
            sections=sections,
        )

        research_report.additional_properties = d
        return research_report

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
