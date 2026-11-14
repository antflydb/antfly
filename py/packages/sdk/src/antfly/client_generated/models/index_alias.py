from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.index_alias_require import IndexAliasRequire
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.index_readiness_status import IndexReadinessStatus


T = TypeVar("T", bound="IndexAlias")


@_attrs_define
class IndexAlias:
    """
    Attributes:
        index (str):
        swapped_at_ms (int):
        require (IndexAliasRequire):
        idempotency_key (str):
        readiness (IndexReadinessStatus):
        previous_index (None | str | Unset):
    """

    index: str
    swapped_at_ms: int
    require: IndexAliasRequire
    idempotency_key: str
    readiness: IndexReadinessStatus
    previous_index: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        swapped_at_ms = self.swapped_at_ms

        require = self.require.value

        idempotency_key = self.idempotency_key

        readiness = self.readiness.to_dict()

        previous_index: None | str | Unset
        if isinstance(self.previous_index, Unset):
            previous_index = UNSET
        else:
            previous_index = self.previous_index

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
                "swapped_at_ms": swapped_at_ms,
                "require": require,
                "idempotency_key": idempotency_key,
                "readiness": readiness,
            }
        )
        if previous_index is not UNSET:
            field_dict["previous_index"] = previous_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.index_readiness_status import IndexReadinessStatus

        d = dict(src_dict)
        index = d.pop("index")

        swapped_at_ms = d.pop("swapped_at_ms")

        require = IndexAliasRequire(d.pop("require"))

        idempotency_key = d.pop("idempotency_key")

        readiness = IndexReadinessStatus.from_dict(d.pop("readiness"))

        def _parse_previous_index(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        previous_index = _parse_previous_index(d.pop("previous_index", UNSET))

        index_alias = cls(
            index=index,
            swapped_at_ms=swapped_at_ms,
            require=require,
            idempotency_key=idempotency_key,
            readiness=readiness,
            previous_index=previous_index,
        )

        index_alias.additional_properties = d
        return index_alias

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
