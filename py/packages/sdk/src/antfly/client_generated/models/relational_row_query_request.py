from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_row_condition import RelationalRowCondition
    from ..models.relational_row_index_bound import RelationalRowIndexBound


T = TypeVar("T", bound="RelationalRowQueryRequest")


@_attrs_define
class RelationalRowQueryRequest:
    """Bounded relational scan in primary-key order, or composite index order
    when index is supplied. Index queries require schema_version and every
    owning shard must have the selected generation ready. Partial indexes
    require their WHERE predicates to be implied by the query conditions.
    The bounded proof combines per-column equality, tighter ranges,
    exclusions, and NULL-aware predicates using exact typed values and
    matching collations. Unsupported implications fail closed. Explicit
    scan bounds alone are not an implication proof. Equal tuples are
    ordered by primary key. Each shard read pins its own immutable schema
    and row snapshot; this is not a table-wide consistent snapshot.
    Resume with the last returned _id as from for primary scans, or its
    cursor as after for index scans. A resumed request opens a fresh snapshot,
    not a retained cursor; concurrent mutations may move rows across the
    continuation boundary. Keep index, bounds and conditions unchanged when paging.
    An empty projection returns row identities and versions only.

        Attributes:
            fields (list[str]):
            index (str | Unset): Ready composite secondary index. Requires schema_version; cannot be combined with from/to.
            after (str | Unset): Opaque exclusive index-order cursor from the last returned row. Binds the immutable schema
                version, logical index name, and comparison semantics, independent of owner-local physical generations. Each
                owner must still prove its current local index is ready.
            lower (RelationalRowIndexBound | Unset): Typed left-prefix bound in declared index order, including descending
                components. Inclusive bounds include the entire matching prefix. Integer components accept exact decimal
                strings; null is an indexed null.
            upper (RelationalRowIndexBound | Unset): Typed left-prefix bound in declared index order, including descending
                components. Inclusive bounds include the entire matching prefix. Integer components accept exact decimal
                strings; null is an indexed null.
            conditions (list[RelationalRowCondition] | Unset):
            from_ (str | Unset): Exclusive lower primary-key bound, including pagination continuation.
            to (str | Unset): Exclusive upper primary-key bound.
            limit (int | Unset):  Default: 128.
            schema_version (int | Unset): Reject the read if an owning shard has a different active schema epoch. Zero is a
                valid epoch and is distinct from omission.
    """

    fields: list[str]
    index: str | Unset = UNSET
    after: str | Unset = UNSET
    lower: RelationalRowIndexBound | Unset = UNSET
    upper: RelationalRowIndexBound | Unset = UNSET
    conditions: list[RelationalRowCondition] | Unset = UNSET
    from_: str | Unset = UNSET
    to: str | Unset = UNSET
    limit: int | Unset = 128
    schema_version: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        index = self.index

        after = self.after

        lower: dict[str, Any] | Unset = UNSET
        if not isinstance(self.lower, Unset):
            lower = self.lower.to_dict()

        upper: dict[str, Any] | Unset = UNSET
        if not isinstance(self.upper, Unset):
            upper = self.upper.to_dict()

        conditions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.conditions, Unset):
            conditions = []
            for conditions_item_data in self.conditions:
                conditions_item = conditions_item_data.to_dict()
                conditions.append(conditions_item)

        from_ = self.from_

        to = self.to

        limit = self.limit

        schema_version = self.schema_version

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fields": fields,
            }
        )
        if index is not UNSET:
            field_dict["index"] = index
        if after is not UNSET:
            field_dict["after"] = after
        if lower is not UNSET:
            field_dict["lower"] = lower
        if upper is not UNSET:
            field_dict["upper"] = upper
        if conditions is not UNSET:
            field_dict["conditions"] = conditions
        if from_ is not UNSET:
            field_dict["from"] = from_
        if to is not UNSET:
            field_dict["to"] = to
        if limit is not UNSET:
            field_dict["limit"] = limit
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_row_condition import RelationalRowCondition
        from ..models.relational_row_index_bound import RelationalRowIndexBound

        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        index = d.pop("index", UNSET)

        after = d.pop("after", UNSET)

        _lower = d.pop("lower", UNSET)
        lower: RelationalRowIndexBound | Unset
        if isinstance(_lower, Unset):
            lower = UNSET
        else:
            lower = RelationalRowIndexBound.from_dict(_lower)

        _upper = d.pop("upper", UNSET)
        upper: RelationalRowIndexBound | Unset
        if isinstance(_upper, Unset):
            upper = UNSET
        else:
            upper = RelationalRowIndexBound.from_dict(_upper)

        _conditions = d.pop("conditions", UNSET)
        conditions: list[RelationalRowCondition] | Unset = UNSET
        if _conditions is not UNSET:
            conditions = []
            for conditions_item_data in _conditions:
                conditions_item = RelationalRowCondition.from_dict(conditions_item_data)

                conditions.append(conditions_item)

        from_ = d.pop("from", UNSET)

        to = d.pop("to", UNSET)

        limit = d.pop("limit", UNSET)

        schema_version = d.pop("schema_version", UNSET)

        relational_row_query_request = cls(
            fields=fields,
            index=index,
            after=after,
            lower=lower,
            upper=upper,
            conditions=conditions,
            from_=from_,
            to=to,
            limit=limit,
            schema_version=schema_version,
        )

        return relational_row_query_request
