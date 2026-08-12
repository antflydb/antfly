from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_artifact_child_range import DocumentArtifactChildRange


T = TypeVar("T", bound="DocumentArtifactManifest")


@_attrs_define
class DocumentArtifactManifest:
    """Inspection view for a derived document artifact produced from a source
    table row. The typed fields form the stable summary contract. The
    embedded manifest/state JSON fields are optional raw detail intended
    for admin/debug inspection so producers can evolve their internal unit
    schema without changing this route contract.

        Attributes:
            document_id (str): Stable identity of the source document.
            artifact_name (str): Name of the derived artifact. Example: document_units_v1.
            artifact_id (str): Stable identity of this artifact under the document.
            manifest_version (int): Version of the opaque manifest payload schema.
            generation (int): Monotonic generation for the current artifact state.
            source_url (str): Source URL or source identifier used to derive this artifact.
            source_fingerprint (str): Fingerprint of the source bytes and extractor configuration.
            content_type (str): Effective source content type selected during extraction.
            route_type (str): Producer route selected for the source content. Example: pdf.
            unit_count (int): Number of extracted document units.
            chunk_count (int): Number of indexable chunks derived from the units.
            child_ranges (list[DocumentArtifactChildRange]): Parsed child range descriptors for this artifact generation.
            child_range_count (int): Number of storage child ranges used by this artifact.
            merge_status (str): Current materialization or merge status.
            merge_from_generation (int): Previous artifact generation used by the current merge plan.
            merge_to_generation (int): Target artifact generation produced by the current merge plan.
            merge_operation_granularity (str): Granularity used when computing merge-plan operations.
            merge_operation_count (int): Number of merge operations recorded for this artifact.
            unsupported_reason (None | str | Unset): Reason extraction was skipped, when the source type is unsupported.
            ocr_attempted_count (int | Unset): Number of pages for which OCR was attempted.
            ocr_selected_count (int | Unset): Number of pages where OCR text was selected over embedded text.
            ocr_retained_embedded_count (int | Unset): Number of attempted pages where embedded text remained preferable.
            ocr_failed_count (int | Unset): Number of pages whose OCR rendering or recognition failed.
            ocr_failed_page_numbers (list[int] | Unset): Up to 32 one-based page numbers where OCR failed.
            ocr_failed_pages_truncated (bool | Unset): Whether additional failed page numbers were omitted from the summary.
            last_error_code (None | str | Unset): Last extraction or materialization error code, when the current artifact
                generation failed.
            last_error_message (None | str | Unset): Human-readable last extraction or materialization error summary, when
                available.
            manifest_json (str | Unset): Opaque JSON manifest for the artifact units and provenance. Present only for raw
                detail responses.
            state_json (None | str | Unset): Optional opaque JSON state for incremental processing. Present only for raw
                detail responses.
    """

    document_id: str
    artifact_name: str
    artifact_id: str
    manifest_version: int
    generation: int
    source_url: str
    source_fingerprint: str
    content_type: str
    route_type: str
    unit_count: int
    chunk_count: int
    child_ranges: list[DocumentArtifactChildRange]
    child_range_count: int
    merge_status: str
    merge_from_generation: int
    merge_to_generation: int
    merge_operation_granularity: str
    merge_operation_count: int
    unsupported_reason: None | str | Unset = UNSET
    ocr_attempted_count: int | Unset = UNSET
    ocr_selected_count: int | Unset = UNSET
    ocr_retained_embedded_count: int | Unset = UNSET
    ocr_failed_count: int | Unset = UNSET
    ocr_failed_page_numbers: list[int] | Unset = UNSET
    ocr_failed_pages_truncated: bool | Unset = UNSET
    last_error_code: None | str | Unset = UNSET
    last_error_message: None | str | Unset = UNSET
    manifest_json: str | Unset = UNSET
    state_json: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        document_id = self.document_id

        artifact_name = self.artifact_name

        artifact_id = self.artifact_id

        manifest_version = self.manifest_version

        generation = self.generation

        source_url = self.source_url

        source_fingerprint = self.source_fingerprint

        content_type = self.content_type

        route_type = self.route_type

        unit_count = self.unit_count

        chunk_count = self.chunk_count

        child_ranges = []
        for child_ranges_item_data in self.child_ranges:
            child_ranges_item = child_ranges_item_data.to_dict()
            child_ranges.append(child_ranges_item)

        child_range_count = self.child_range_count

        merge_status = self.merge_status

        merge_from_generation = self.merge_from_generation

        merge_to_generation = self.merge_to_generation

        merge_operation_granularity = self.merge_operation_granularity

        merge_operation_count = self.merge_operation_count

        unsupported_reason: None | str | Unset
        if isinstance(self.unsupported_reason, Unset):
            unsupported_reason = UNSET
        else:
            unsupported_reason = self.unsupported_reason

        ocr_attempted_count = self.ocr_attempted_count

        ocr_selected_count = self.ocr_selected_count

        ocr_retained_embedded_count = self.ocr_retained_embedded_count

        ocr_failed_count = self.ocr_failed_count

        ocr_failed_page_numbers: list[int] | Unset = UNSET
        if not isinstance(self.ocr_failed_page_numbers, Unset):
            ocr_failed_page_numbers = self.ocr_failed_page_numbers

        ocr_failed_pages_truncated = self.ocr_failed_pages_truncated

        last_error_code: None | str | Unset
        if isinstance(self.last_error_code, Unset):
            last_error_code = UNSET
        else:
            last_error_code = self.last_error_code

        last_error_message: None | str | Unset
        if isinstance(self.last_error_message, Unset):
            last_error_message = UNSET
        else:
            last_error_message = self.last_error_message

        manifest_json = self.manifest_json

        state_json: None | str | Unset
        if isinstance(self.state_json, Unset):
            state_json = UNSET
        else:
            state_json = self.state_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "document_id": document_id,
                "artifact_name": artifact_name,
                "artifact_id": artifact_id,
                "manifest_version": manifest_version,
                "generation": generation,
                "source_url": source_url,
                "source_fingerprint": source_fingerprint,
                "content_type": content_type,
                "route_type": route_type,
                "unit_count": unit_count,
                "chunk_count": chunk_count,
                "child_ranges": child_ranges,
                "child_range_count": child_range_count,
                "merge_status": merge_status,
                "merge_from_generation": merge_from_generation,
                "merge_to_generation": merge_to_generation,
                "merge_operation_granularity": merge_operation_granularity,
                "merge_operation_count": merge_operation_count,
            }
        )
        if unsupported_reason is not UNSET:
            field_dict["unsupported_reason"] = unsupported_reason
        if ocr_attempted_count is not UNSET:
            field_dict["ocr_attempted_count"] = ocr_attempted_count
        if ocr_selected_count is not UNSET:
            field_dict["ocr_selected_count"] = ocr_selected_count
        if ocr_retained_embedded_count is not UNSET:
            field_dict["ocr_retained_embedded_count"] = ocr_retained_embedded_count
        if ocr_failed_count is not UNSET:
            field_dict["ocr_failed_count"] = ocr_failed_count
        if ocr_failed_page_numbers is not UNSET:
            field_dict["ocr_failed_page_numbers"] = ocr_failed_page_numbers
        if ocr_failed_pages_truncated is not UNSET:
            field_dict["ocr_failed_pages_truncated"] = ocr_failed_pages_truncated
        if last_error_code is not UNSET:
            field_dict["last_error_code"] = last_error_code
        if last_error_message is not UNSET:
            field_dict["last_error_message"] = last_error_message
        if manifest_json is not UNSET:
            field_dict["manifest_json"] = manifest_json
        if state_json is not UNSET:
            field_dict["state_json"] = state_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_child_range import DocumentArtifactChildRange

        d = dict(src_dict)
        document_id = d.pop("document_id")

        artifact_name = d.pop("artifact_name")

        artifact_id = d.pop("artifact_id")

        manifest_version = d.pop("manifest_version")

        generation = d.pop("generation")

        source_url = d.pop("source_url")

        source_fingerprint = d.pop("source_fingerprint")

        content_type = d.pop("content_type")

        route_type = d.pop("route_type")

        unit_count = d.pop("unit_count")

        chunk_count = d.pop("chunk_count")

        child_ranges = []
        _child_ranges = d.pop("child_ranges")
        for child_ranges_item_data in _child_ranges:
            child_ranges_item = DocumentArtifactChildRange.from_dict(child_ranges_item_data)

            child_ranges.append(child_ranges_item)

        child_range_count = d.pop("child_range_count")

        merge_status = d.pop("merge_status")

        merge_from_generation = d.pop("merge_from_generation")

        merge_to_generation = d.pop("merge_to_generation")

        merge_operation_granularity = d.pop("merge_operation_granularity")

        merge_operation_count = d.pop("merge_operation_count")

        def _parse_unsupported_reason(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        unsupported_reason = _parse_unsupported_reason(d.pop("unsupported_reason", UNSET))

        ocr_attempted_count = d.pop("ocr_attempted_count", UNSET)

        ocr_selected_count = d.pop("ocr_selected_count", UNSET)

        ocr_retained_embedded_count = d.pop("ocr_retained_embedded_count", UNSET)

        ocr_failed_count = d.pop("ocr_failed_count", UNSET)

        ocr_failed_page_numbers = cast(list[int], d.pop("ocr_failed_page_numbers", UNSET))

        ocr_failed_pages_truncated = d.pop("ocr_failed_pages_truncated", UNSET)

        def _parse_last_error_code(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error_code = _parse_last_error_code(d.pop("last_error_code", UNSET))

        def _parse_last_error_message(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error_message = _parse_last_error_message(d.pop("last_error_message", UNSET))

        manifest_json = d.pop("manifest_json", UNSET)

        def _parse_state_json(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        state_json = _parse_state_json(d.pop("state_json", UNSET))

        document_artifact_manifest = cls(
            document_id=document_id,
            artifact_name=artifact_name,
            artifact_id=artifact_id,
            manifest_version=manifest_version,
            generation=generation,
            source_url=source_url,
            source_fingerprint=source_fingerprint,
            content_type=content_type,
            route_type=route_type,
            unit_count=unit_count,
            chunk_count=chunk_count,
            child_ranges=child_ranges,
            child_range_count=child_range_count,
            merge_status=merge_status,
            merge_from_generation=merge_from_generation,
            merge_to_generation=merge_to_generation,
            merge_operation_granularity=merge_operation_granularity,
            merge_operation_count=merge_operation_count,
            unsupported_reason=unsupported_reason,
            ocr_attempted_count=ocr_attempted_count,
            ocr_selected_count=ocr_selected_count,
            ocr_retained_embedded_count=ocr_retained_embedded_count,
            ocr_failed_count=ocr_failed_count,
            ocr_failed_page_numbers=ocr_failed_page_numbers,
            ocr_failed_pages_truncated=ocr_failed_pages_truncated,
            last_error_code=last_error_code,
            last_error_message=last_error_message,
            manifest_json=manifest_json,
            state_json=state_json,
        )

        document_artifact_manifest.additional_properties = d
        return document_artifact_manifest

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
