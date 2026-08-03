from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.replication_source_type import ReplicationSourceType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.boolean_query import BooleanQuery
    from ..models.conjunction_query import ConjunctionQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.disjunction_query import DisjunctionQuery
    from ..models.doc_id_query import DocIdQuery
    from ..models.fuzzy_query import FuzzyQuery
    from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
    from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
    from ..models.geo_distance_query import GeoDistanceQuery
    from ..models.geo_shape_query import GeoShapeQuery
    from ..models.ip_range_query import IPRangeQuery
    from ..models.match_all_query import MatchAllQuery
    from ..models.match_none_query import MatchNoneQuery
    from ..models.match_phrase_query import MatchPhraseQuery
    from ..models.match_query import MatchQuery
    from ..models.multi_match_query import MultiMatchQuery
    from ..models.multi_phrase_query import MultiPhraseQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.phrase_query import PhraseQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.query_string_query import QueryStringQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.replication_route import ReplicationRoute
    from ..models.replication_source_action_hint import ReplicationSourceActionHint
    from ..models.replication_source_status import ReplicationSourceStatus
    from ..models.replication_transform_op import ReplicationTransformOp
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="ReplicationSource")


@_attrs_define
class ReplicationSource:
    """
    Attributes:
        type_ (ReplicationSourceType): Type of the replication source. Currently only "postgres" is supported.
             Example: postgres.
        dsn (str): Data source name (connection string) for the PostgreSQL database.
            Supports `${secret:key_name}` references that resolve from the Antfly secret store
            or environment variables. Requires `wal_level=logical` on the source.
             Example: ${secret:pg_dsn}.
        postgres_table (str): Name of the table in the PostgreSQL database to replicate from.
             Example: users.
        key_template (str | Unset): Template for constructing the Antfly document key from PG columns.
            A plain string (e.g., "id") uses that column's value directly.
            Use `{{column}}` syntax for composite keys: `{{tenant_id}}:{{user_id}}`.
             Default: 'id'. Example: id.
        slot_name (str | Unset): PostgreSQL replication slot name. If omitted, auto-derived from
            the Antfly table and PG table names. Specify this when using
            pre-created slots (e.g., on Supabase or Neon).
        publication_name (str | Unset): PostgreSQL publication name. If omitted, auto-derived and created
            automatically. Specify this when using pre-created publications.
        on_update (list[ReplicationTransformOp] | Unset): Transform operations applied on INSERT/UPDATE events. Values
            can
            reference PG columns via `{{column}}` syntax. If omitted, auto-generates
            `$set` for every column (passthrough mode).
             Example: [{'op': '$set', 'path': 'email', 'value': '{{user_email}}'}, {'op': '$set', 'path': 'score', 'value':
            '{{score}}'}, {'op': '$merge', 'value': '{{metadata}}'}, {'op': '$set', 'path': 'active', 'value': True}].
        on_delete (list[ReplicationTransformOp] | Unset): Transform operations applied on DELETE events. If omitted,
            auto-derives
            `$unset` ops from `on_update`'s `$set` paths (safe for multi-source).
            Use `$delete_document` op to delete the entire Antfly document.
             Example: [{'op': '$set', 'path': 'active', 'value': False}].
        publication_filter (BooleanQuery | BoolFieldQuery | ConjunctionQuery | DateRangeStringQuery | DisjunctionQuery |
            DocIdQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery |
            IPRangeQuery | MatchAllQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery |
            MultiPhraseQuery | NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery |
            TermRangeQuery | Unset | WildcardQuery):
        routes (list[ReplicationRoute] | Unset): Conditional routes for fan-out replication. Each route evaluates its
            `where` filter against every CDC row and, on match, writes to the
            specified `target_table`. Multiple routes can match the same row.

            When routes are present, the top-level `on_update`/`on_delete` are
            ignored — each route defines its own transforms.
             Example: [{'target_table': 'premium_users', 'where': {'term': 'premium', 'field': 'tier'}}, {'target_table':
            'free_users', 'where': {'term': 'free', 'field': 'tier'}}].
        require_exact_cutover (bool | Unset): When true, indicates this source was reseeded with exact cutover mode
            and must replay from a consistent snapshot before streaming.
        status (ReplicationSourceStatus | Unset): Runtime status of this replication source. Present only in GET table
            detail responses, not in create/update requests.
        action_hint (ReplicationSourceActionHint | Unset): Action hint for this replication source when remediation is
            recommended.
            Present only in GET table detail responses.
    """

    type_: ReplicationSourceType
    dsn: str
    postgres_table: str
    key_template: str | Unset = "id"
    slot_name: str | Unset = UNSET
    publication_name: str | Unset = UNSET
    on_update: list[ReplicationTransformOp] | Unset = UNSET
    on_delete: list[ReplicationTransformOp] | Unset = UNSET
    publication_filter: (
        BooleanQuery
        | BoolFieldQuery
        | ConjunctionQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | MatchAllQuery
        | MatchNoneQuery
        | MatchPhraseQuery
        | MatchQuery
        | MultiMatchQuery
        | MultiPhraseQuery
        | NumericRangeQuery
        | PhraseQuery
        | PrefixQuery
        | QueryStringQuery
        | RegexpQuery
        | TermQuery
        | TermRangeQuery
        | Unset
        | WildcardQuery
    ) = UNSET
    routes: list[ReplicationRoute] | Unset = UNSET
    require_exact_cutover: bool | Unset = UNSET
    status: ReplicationSourceStatus | Unset = UNSET
    action_hint: ReplicationSourceActionHint | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
        from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
        from ..models.geo_distance_query import GeoDistanceQuery
        from ..models.ip_range_query import IPRangeQuery
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.match_phrase_query import MatchPhraseQuery
        from ..models.match_query import MatchQuery
        from ..models.multi_match_query import MultiMatchQuery
        from ..models.multi_phrase_query import MultiPhraseQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.phrase_query import PhraseQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.query_string_query import QueryStringQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        type_ = self.type_.value

        dsn = self.dsn

        postgres_table = self.postgres_table

        key_template = self.key_template

        slot_name = self.slot_name

        publication_name = self.publication_name

        on_update: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.on_update, Unset):
            on_update = []
            for on_update_item_data in self.on_update:
                on_update_item = on_update_item_data.to_dict()
                on_update.append(on_update_item)

        on_delete: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.on_delete, Unset):
            on_delete = []
            for on_delete_item_data in self.on_delete:
                on_delete_item = on_delete_item_data.to_dict()
                on_delete.append(on_delete_item)

        publication_filter: dict[str, Any] | Unset
        if isinstance(self.publication_filter, Unset):
            publication_filter = UNSET
        elif isinstance(self.publication_filter, TermQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, MatchQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, MultiMatchQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, MatchPhraseQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, PhraseQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, MultiPhraseQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, FuzzyQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, PrefixQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, RegexpQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, WildcardQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, QueryStringQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, NumericRangeQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, TermRangeQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, DateRangeStringQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, BooleanQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, ConjunctionQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, DisjunctionQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, MatchAllQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, MatchNoneQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, DocIdQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, BoolFieldQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, IPRangeQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, GeoBoundingBoxQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, GeoDistanceQuery):
            publication_filter = self.publication_filter.to_dict()
        elif isinstance(self.publication_filter, GeoBoundingPolygonQuery):
            publication_filter = self.publication_filter.to_dict()
        else:
            publication_filter = self.publication_filter.to_dict()

        routes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.routes, Unset):
            routes = []
            for routes_item_data in self.routes:
                routes_item = routes_item_data.to_dict()
                routes.append(routes_item)

        require_exact_cutover = self.require_exact_cutover

        status: dict[str, Any] | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.to_dict()

        action_hint: dict[str, Any] | Unset = UNSET
        if not isinstance(self.action_hint, Unset):
            action_hint = self.action_hint.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "dsn": dsn,
                "postgres_table": postgres_table,
            }
        )
        if key_template is not UNSET:
            field_dict["key_template"] = key_template
        if slot_name is not UNSET:
            field_dict["slot_name"] = slot_name
        if publication_name is not UNSET:
            field_dict["publication_name"] = publication_name
        if on_update is not UNSET:
            field_dict["on_update"] = on_update
        if on_delete is not UNSET:
            field_dict["on_delete"] = on_delete
        if publication_filter is not UNSET:
            field_dict["publication_filter"] = publication_filter
        if routes is not UNSET:
            field_dict["routes"] = routes
        if require_exact_cutover is not UNSET:
            field_dict["require_exact_cutover"] = require_exact_cutover
        if status is not UNSET:
            field_dict["status"] = status
        if action_hint is not UNSET:
            field_dict["action_hint"] = action_hint

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
        from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
        from ..models.geo_distance_query import GeoDistanceQuery
        from ..models.geo_shape_query import GeoShapeQuery
        from ..models.ip_range_query import IPRangeQuery
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.match_phrase_query import MatchPhraseQuery
        from ..models.match_query import MatchQuery
        from ..models.multi_match_query import MultiMatchQuery
        from ..models.multi_phrase_query import MultiPhraseQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.phrase_query import PhraseQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.query_string_query import QueryStringQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.replication_route import ReplicationRoute
        from ..models.replication_source_action_hint import ReplicationSourceActionHint
        from ..models.replication_source_status import ReplicationSourceStatus
        from ..models.replication_transform_op import ReplicationTransformOp
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        d = dict(src_dict)
        type_ = ReplicationSourceType(d.pop("type"))

        dsn = d.pop("dsn")

        postgres_table = d.pop("postgres_table")

        key_template = d.pop("key_template", UNSET)

        slot_name = d.pop("slot_name", UNSET)

        publication_name = d.pop("publication_name", UNSET)

        _on_update = d.pop("on_update", UNSET)
        on_update: list[ReplicationTransformOp] | Unset = UNSET
        if _on_update is not UNSET:
            on_update = []
            for on_update_item_data in _on_update:
                on_update_item = ReplicationTransformOp.from_dict(on_update_item_data)

                on_update.append(on_update_item)

        _on_delete = d.pop("on_delete", UNSET)
        on_delete: list[ReplicationTransformOp] | Unset = UNSET
        if _on_delete is not UNSET:
            on_delete = []
            for on_delete_item_data in _on_delete:
                on_delete_item = ReplicationTransformOp.from_dict(on_delete_item_data)

                on_delete.append(on_delete_item)

        def _parse_publication_filter(
            data: object,
        ) -> (
            BooleanQuery
            | BoolFieldQuery
            | ConjunctionQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchNoneQuery
            | MatchPhraseQuery
            | MatchQuery
            | MultiMatchQuery
            | MultiPhraseQuery
            | NumericRangeQuery
            | PhraseQuery
            | PrefixQuery
            | QueryStringQuery
            | RegexpQuery
            | TermQuery
            | TermRangeQuery
            | Unset
            | WildcardQuery
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_0 = TermQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_type_25 = GeoShapeQuery.from_dict(data)

            return componentsschemas_query_type_25

        publication_filter = _parse_publication_filter(d.pop("publication_filter", UNSET))

        _routes = d.pop("routes", UNSET)
        routes: list[ReplicationRoute] | Unset = UNSET
        if _routes is not UNSET:
            routes = []
            for routes_item_data in _routes:
                routes_item = ReplicationRoute.from_dict(routes_item_data)

                routes.append(routes_item)

        require_exact_cutover = d.pop("require_exact_cutover", UNSET)

        _status = d.pop("status", UNSET)
        status: ReplicationSourceStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = ReplicationSourceStatus.from_dict(_status)

        _action_hint = d.pop("action_hint", UNSET)
        action_hint: ReplicationSourceActionHint | Unset
        if isinstance(_action_hint, Unset):
            action_hint = UNSET
        else:
            action_hint = ReplicationSourceActionHint.from_dict(_action_hint)

        replication_source = cls(
            type_=type_,
            dsn=dsn,
            postgres_table=postgres_table,
            key_template=key_template,
            slot_name=slot_name,
            publication_name=publication_name,
            on_update=on_update,
            on_delete=on_delete,
            publication_filter=publication_filter,
            routes=routes,
            require_exact_cutover=require_exact_cutover,
            status=status,
            action_hint=action_hint,
        )

        replication_source.additional_properties = d
        return replication_source

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
