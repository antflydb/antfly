from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.retained_dataset_owner_progress_mode import RetainedDatasetOwnerProgressMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.retained_dataset_archive_progress import RetainedDatasetArchiveProgress
    from ..models.retained_dataset_native_progress import RetainedDatasetNativeProgress


T = TypeVar("T", bound="RetainedDatasetOwnerProgress")


@_attrs_define
class RetainedDatasetOwnerProgress:
    """
    Attributes:
        group (str):
        mode (RetainedDatasetOwnerProgressMode):
        archive_authoritative (bool):
        native (None | RetainedDatasetNativeProgress | Unset):
        archive (None | RetainedDatasetArchiveProgress | Unset):
        status_error (None | str | Unset): Failure to obtain this owner's current status; distinct from a persisted
            maintenance failure.
    """

    group: str
    mode: RetainedDatasetOwnerProgressMode
    archive_authoritative: bool
    native: None | RetainedDatasetNativeProgress | Unset = UNSET
    archive: None | RetainedDatasetArchiveProgress | Unset = UNSET
    status_error: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.retained_dataset_archive_progress import RetainedDatasetArchiveProgress
        from ..models.retained_dataset_native_progress import RetainedDatasetNativeProgress

        group = self.group

        mode = self.mode.value

        archive_authoritative = self.archive_authoritative

        native: dict[str, Any] | None | Unset
        if isinstance(self.native, Unset):
            native = UNSET
        elif isinstance(self.native, RetainedDatasetNativeProgress):
            native = self.native.to_dict()
        else:
            native = self.native

        archive: dict[str, Any] | None | Unset
        if isinstance(self.archive, Unset):
            archive = UNSET
        elif isinstance(self.archive, RetainedDatasetArchiveProgress):
            archive = self.archive.to_dict()
        else:
            archive = self.archive

        status_error: None | str | Unset
        if isinstance(self.status_error, Unset):
            status_error = UNSET
        else:
            status_error = self.status_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "group": group,
                "mode": mode,
                "archive_authoritative": archive_authoritative,
            }
        )
        if native is not UNSET:
            field_dict["native"] = native
        if archive is not UNSET:
            field_dict["archive"] = archive
        if status_error is not UNSET:
            field_dict["status_error"] = status_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retained_dataset_archive_progress import RetainedDatasetArchiveProgress
        from ..models.retained_dataset_native_progress import RetainedDatasetNativeProgress

        d = dict(src_dict)
        group = d.pop("group")

        mode = RetainedDatasetOwnerProgressMode(d.pop("mode"))

        archive_authoritative = d.pop("archive_authoritative")

        def _parse_native(data: object) -> None | RetainedDatasetNativeProgress | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                native_type_1 = RetainedDatasetNativeProgress.from_dict(data)

                return native_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RetainedDatasetNativeProgress | Unset, data)

        native = _parse_native(d.pop("native", UNSET))

        def _parse_archive(data: object) -> None | RetainedDatasetArchiveProgress | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                archive_type_1 = RetainedDatasetArchiveProgress.from_dict(data)

                return archive_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RetainedDatasetArchiveProgress | Unset, data)

        archive = _parse_archive(d.pop("archive", UNSET))

        def _parse_status_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        status_error = _parse_status_error(d.pop("status_error", UNSET))

        retained_dataset_owner_progress = cls(
            group=group,
            mode=mode,
            archive_authoritative=archive_authoritative,
            native=native,
            archive=archive,
            status_error=status_error,
        )

        retained_dataset_owner_progress.additional_properties = d
        return retained_dataset_owner_progress

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
