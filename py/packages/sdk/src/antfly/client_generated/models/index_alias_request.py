from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.index_alias_request_require import IndexAliasRequestRequire
from ..types import UNSET, Unset

T = TypeVar("T", bound="IndexAliasRequest")


@_attrs_define
class IndexAliasRequest:
    """
    Attributes:
        index (str):
        idempotency_key (str):
        expected_index (str | Unset):
        require (IndexAliasRequestRequire | Unset):  Default: IndexAliasRequestRequire.COMPLETE.
    """

    index: str
    idempotency_key: str
    expected_index: str | Unset = UNSET
    require: IndexAliasRequestRequire | Unset = IndexAliasRequestRequire.COMPLETE

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        idempotency_key = self.idempotency_key

        expected_index = self.expected_index

        require: str | Unset = UNSET
        if not isinstance(self.require, Unset):
            require = self.require.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "index": index,
                "idempotency_key": idempotency_key,
            }
        )
        if expected_index is not UNSET:
            field_dict["expected_index"] = expected_index
        if require is not UNSET:
            field_dict["require"] = require

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index = d.pop("index")

        idempotency_key = d.pop("idempotency_key")

        expected_index = d.pop("expected_index", UNSET)

        _require = d.pop("require", UNSET)
        require: IndexAliasRequestRequire | Unset
        if isinstance(_require, Unset):
            require = UNSET
        else:
            require = IndexAliasRequestRequire(_require)

        index_alias_request = cls(
            index=index,
            idempotency_key=idempotency_key,
            expected_index=expected_index,
            require=require,
        )

        return index_alias_request
