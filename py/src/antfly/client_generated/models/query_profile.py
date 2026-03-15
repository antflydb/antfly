from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.join_profile import JoinProfile
    from ..models.merge_profile import MergeProfile
    from ..models.reranker_profile import RerankerProfile
    from ..models.shards_profile import ShardsProfile


T = TypeVar("T", bound="QueryProfile")


@_attrs_define
class QueryProfile:
    """Detailed execution profiling for a query. Present in the response
    when the request sets `profile: true`.

        Attributes:
            shards (Union[Unset, ShardsProfile]): Shard-level execution statistics.
            join (Union[Unset, JoinProfile]): Join execution statistics.
            reranker (Union[Unset, RerankerProfile]): Reranking execution statistics.
            merge (Union[Unset, MergeProfile]): Result merge statistics for hybrid search.
    """

    shards: Union[Unset, "ShardsProfile"] = UNSET
    join: Union[Unset, "JoinProfile"] = UNSET
    reranker: Union[Unset, "RerankerProfile"] = UNSET
    merge: Union[Unset, "MergeProfile"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        shards: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.shards, Unset):
            shards = self.shards.to_dict()

        join: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.join, Unset):
            join = self.join.to_dict()

        reranker: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.reranker, Unset):
            reranker = self.reranker.to_dict()

        merge: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.merge, Unset):
            merge = self.merge.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if shards is not UNSET:
            field_dict["shards"] = shards
        if join is not UNSET:
            field_dict["join"] = join
        if reranker is not UNSET:
            field_dict["reranker"] = reranker
        if merge is not UNSET:
            field_dict["merge"] = merge

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.join_profile import JoinProfile
        from ..models.merge_profile import MergeProfile
        from ..models.reranker_profile import RerankerProfile
        from ..models.shards_profile import ShardsProfile

        d = dict(src_dict)
        _shards = d.pop("shards", UNSET)
        shards: Union[Unset, ShardsProfile]
        if isinstance(_shards, Unset):
            shards = UNSET
        else:
            shards = ShardsProfile.from_dict(_shards)

        _join = d.pop("join", UNSET)
        join: Union[Unset, JoinProfile]
        if isinstance(_join, Unset):
            join = UNSET
        else:
            join = JoinProfile.from_dict(_join)

        _reranker = d.pop("reranker", UNSET)
        reranker: Union[Unset, RerankerProfile]
        if isinstance(_reranker, Unset):
            reranker = UNSET
        else:
            reranker = RerankerProfile.from_dict(_reranker)

        _merge = d.pop("merge", UNSET)
        merge: Union[Unset, MergeProfile]
        if isinstance(_merge, Unset):
            merge = UNSET
        else:
            merge = MergeProfile.from_dict(_merge)

        query_profile = cls(
            shards=shards,
            join=join,
            reranker=reranker,
            merge=merge,
        )

        query_profile.additional_properties = d
        return query_profile

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
