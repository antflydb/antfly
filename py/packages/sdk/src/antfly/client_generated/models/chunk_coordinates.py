from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_coordinates_kind import ChunkCoordinatesKind
from ..models.chunk_coordinates_picture_basis import ChunkCoordinatesPictureBasis
from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkCoordinates")


@_attrs_define
class ChunkCoordinates:
    """Original-source coordinates; time intervals are half-open integer nanoseconds and text spans are UTF-8 byte offsets.

    Attributes:
        kind (ChunkCoordinatesKind):
        start_byte (int | Unset):
        end_byte (int | Unset):
        page (int | Unset):
        region (list[int] | Unset): Full-raster coordinates [left, top, right, bottom], end exclusive;
            raster_width/height declare the basis, including adaptively rendered PDF pages.
        raster_width (int | Unset): Width of the complete source/prepared-page raster before tiling, not the returned
            crop width.
        raster_height (int | Unset): Height of the complete source/prepared-page raster before tiling; map PDF regions
            through page_box and page_rotation.
        start_ns (int | Unset):
        end_ns (int | Unset):
        first_picture (int | Unset):
        selected_pictures (list[int] | Unset): Ordered selectors in the immutable source index, including repeated
            cadence selections.
        picture_basis (ChunkCoordinatesPictureBasis | Unset): WebM logical presentation pictures or the first MP4 packet
            of a completed picture assembly.
        picture_count (int | Unset):
        start_sample (int | Unset):
        end_sample (int | Unset):
        sample_rate (int | Unset):
        start_tick (int | Unset):
        end_tick (int | Unset):
        timescale (int | Unset):
        page_rotation (int | Unset):
        page_box (list[float] | Unset): Original PDF page box in PDF user-space coordinates.
    """

    kind: ChunkCoordinatesKind
    start_byte: int | Unset = UNSET
    end_byte: int | Unset = UNSET
    page: int | Unset = UNSET
    region: list[int] | Unset = UNSET
    raster_width: int | Unset = UNSET
    raster_height: int | Unset = UNSET
    start_ns: int | Unset = UNSET
    end_ns: int | Unset = UNSET
    first_picture: int | Unset = UNSET
    selected_pictures: list[int] | Unset = UNSET
    picture_basis: ChunkCoordinatesPictureBasis | Unset = UNSET
    picture_count: int | Unset = UNSET
    start_sample: int | Unset = UNSET
    end_sample: int | Unset = UNSET
    sample_rate: int | Unset = UNSET
    start_tick: int | Unset = UNSET
    end_tick: int | Unset = UNSET
    timescale: int | Unset = UNSET
    page_rotation: int | Unset = UNSET
    page_box: list[float] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        start_byte = self.start_byte

        end_byte = self.end_byte

        page = self.page

        region: list[int] | Unset = UNSET
        if not isinstance(self.region, Unset):
            region = self.region

        raster_width = self.raster_width

        raster_height = self.raster_height

        start_ns = self.start_ns

        end_ns = self.end_ns

        first_picture = self.first_picture

        selected_pictures: list[int] | Unset = UNSET
        if not isinstance(self.selected_pictures, Unset):
            selected_pictures = self.selected_pictures

        picture_basis: str | Unset = UNSET
        if not isinstance(self.picture_basis, Unset):
            picture_basis = self.picture_basis.value

        picture_count = self.picture_count

        start_sample = self.start_sample

        end_sample = self.end_sample

        sample_rate = self.sample_rate

        start_tick = self.start_tick

        end_tick = self.end_tick

        timescale = self.timescale

        page_rotation = self.page_rotation

        page_box: list[float] | Unset = UNSET
        if not isinstance(self.page_box, Unset):
            page_box = self.page_box

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
            }
        )
        if start_byte is not UNSET:
            field_dict["start_byte"] = start_byte
        if end_byte is not UNSET:
            field_dict["end_byte"] = end_byte
        if page is not UNSET:
            field_dict["page"] = page
        if region is not UNSET:
            field_dict["region"] = region
        if raster_width is not UNSET:
            field_dict["raster_width"] = raster_width
        if raster_height is not UNSET:
            field_dict["raster_height"] = raster_height
        if start_ns is not UNSET:
            field_dict["start_ns"] = start_ns
        if end_ns is not UNSET:
            field_dict["end_ns"] = end_ns
        if first_picture is not UNSET:
            field_dict["first_picture"] = first_picture
        if selected_pictures is not UNSET:
            field_dict["selected_pictures"] = selected_pictures
        if picture_basis is not UNSET:
            field_dict["picture_basis"] = picture_basis
        if picture_count is not UNSET:
            field_dict["picture_count"] = picture_count
        if start_sample is not UNSET:
            field_dict["start_sample"] = start_sample
        if end_sample is not UNSET:
            field_dict["end_sample"] = end_sample
        if sample_rate is not UNSET:
            field_dict["sample_rate"] = sample_rate
        if start_tick is not UNSET:
            field_dict["start_tick"] = start_tick
        if end_tick is not UNSET:
            field_dict["end_tick"] = end_tick
        if timescale is not UNSET:
            field_dict["timescale"] = timescale
        if page_rotation is not UNSET:
            field_dict["page_rotation"] = page_rotation
        if page_box is not UNSET:
            field_dict["page_box"] = page_box

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = ChunkCoordinatesKind(d.pop("kind"))

        start_byte = d.pop("start_byte", UNSET)

        end_byte = d.pop("end_byte", UNSET)

        page = d.pop("page", UNSET)

        region = cast(list[int], d.pop("region", UNSET))

        raster_width = d.pop("raster_width", UNSET)

        raster_height = d.pop("raster_height", UNSET)

        start_ns = d.pop("start_ns", UNSET)

        end_ns = d.pop("end_ns", UNSET)

        first_picture = d.pop("first_picture", UNSET)

        selected_pictures = cast(list[int], d.pop("selected_pictures", UNSET))

        _picture_basis = d.pop("picture_basis", UNSET)
        picture_basis: ChunkCoordinatesPictureBasis | Unset
        if isinstance(_picture_basis, Unset):
            picture_basis = UNSET
        else:
            picture_basis = ChunkCoordinatesPictureBasis(_picture_basis)

        picture_count = d.pop("picture_count", UNSET)

        start_sample = d.pop("start_sample", UNSET)

        end_sample = d.pop("end_sample", UNSET)

        sample_rate = d.pop("sample_rate", UNSET)

        start_tick = d.pop("start_tick", UNSET)

        end_tick = d.pop("end_tick", UNSET)

        timescale = d.pop("timescale", UNSET)

        page_rotation = d.pop("page_rotation", UNSET)

        page_box = cast(list[float], d.pop("page_box", UNSET))

        chunk_coordinates = cls(
            kind=kind,
            start_byte=start_byte,
            end_byte=end_byte,
            page=page,
            region=region,
            raster_width=raster_width,
            raster_height=raster_height,
            start_ns=start_ns,
            end_ns=end_ns,
            first_picture=first_picture,
            selected_pictures=selected_pictures,
            picture_basis=picture_basis,
            picture_count=picture_count,
            start_sample=start_sample,
            end_sample=end_sample,
            sample_rate=sample_rate,
            start_tick=start_tick,
            end_tick=end_tick,
            timescale=timescale,
            page_rotation=page_rotation,
            page_box=page_box,
        )

        chunk_coordinates.additional_properties = d
        return chunk_coordinates

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
