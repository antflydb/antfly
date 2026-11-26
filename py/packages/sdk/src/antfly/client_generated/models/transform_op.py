from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.transform_op_type import TransformOpType
from ..types import UNSET, Unset

T = TypeVar("T", bound="TransformOp")


@_attrs_define
class TransformOp:
    """
    Attributes:
        op (TransformOpType): MongoDB-style update operator
        path (str): JSONPath to field (e.g., "$.user.name", "$.tags", or "user.name").

            Reserved DB-owned paths take exactly two segments and accept only
            `$set` and `$unset` (`$unset` deletes, like a `null` value in an
            insert); every other operator is rejected:
            - `_embeddings.<index>`: explicit vectors for an external index.
            - `_artifacts.<name>`: an artifact seed for an asset or embedding
              enrichment (see `ArtifactSeed`).
            - `_chunks.<name>`: an artifact seed for a chunk enrichment.

            `_edges.<index>.<type>` accepts `$push`, `$pull` and `$addToSet`.
             Example: $.views.
        value (Any | Unset): Value for operation (not required for $unset). Type depends on the supported operator.
            `$pull` removes every array element exactly equal to this JSON value; projected graph-edge values identify the
            relationship by `target`.
    """

    op: TransformOpType
    path: str
    value: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        path = self.path

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
                "path": path,
            }
        )
        if value is not UNSET:
            field_dict["value"] = value

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        op = TransformOpType(d.pop("op"))

        path = d.pop("path")

        value = d.pop("value", UNSET)

        transform_op = cls(
            op=op,
            path=path,
            value=value,
        )

        transform_op.additional_properties = d
        return transform_op

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
