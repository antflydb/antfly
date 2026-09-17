from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_constraint_exactly_one_of_type import ExtractionConstraintExactlyOneOfType

if TYPE_CHECKING:
    from ..models.extraction_constraint_and import ExtractionConstraintAnd
    from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
    from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
    from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
    from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
    from ..models.extraction_constraint_excludes import ExtractionConstraintExcludes
    from ..models.extraction_constraint_iff import ExtractionConstraintIff
    from ..models.extraction_constraint_implies import ExtractionConstraintImplies
    from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
    from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
    from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
    from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
    from ..models.extraction_constraint_not import ExtractionConstraintNot
    from ..models.extraction_constraint_or import ExtractionConstraintOr


T = TypeVar("T", bound="ExtractionConstraintExactlyOneOf")


@_attrs_define
class ExtractionConstraintExactlyOneOf:
    """
    Attributes:
        type_ (ExtractionConstraintExactlyOneOfType):
        children (list[ExtractionConstraintAnd | ExtractionConstraintAnyOtherSelected | ExtractionConstraintAnySelected
            | ExtractionConstraintAtLevel | ExtractionConstraintCardinality | ExtractionConstraintExactlyOneOf |
            ExtractionConstraintExcludes | ExtractionConstraintIff | ExtractionConstraintImplies |
            ExtractionConstraintIsDefault | ExtractionConstraintLabelRef | ExtractionConstraintMaxLevel |
            ExtractionConstraintMinLevel | ExtractionConstraintNot | ExtractionConstraintOr]):
    """

    type_: ExtractionConstraintExactlyOneOfType
    children: list[
        ExtractionConstraintAnd
        | ExtractionConstraintAnyOtherSelected
        | ExtractionConstraintAnySelected
        | ExtractionConstraintAtLevel
        | ExtractionConstraintCardinality
        | ExtractionConstraintExactlyOneOf
        | ExtractionConstraintExcludes
        | ExtractionConstraintIff
        | ExtractionConstraintImplies
        | ExtractionConstraintIsDefault
        | ExtractionConstraintLabelRef
        | ExtractionConstraintMaxLevel
        | ExtractionConstraintMinLevel
        | ExtractionConstraintNot
        | ExtractionConstraintOr
    ]

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_constraint_and import ExtractionConstraintAnd
        from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
        from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
        from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
        from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
        from ..models.extraction_constraint_iff import ExtractionConstraintIff
        from ..models.extraction_constraint_implies import ExtractionConstraintImplies
        from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
        from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
        from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
        from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
        from ..models.extraction_constraint_not import ExtractionConstraintNot
        from ..models.extraction_constraint_or import ExtractionConstraintOr

        type_ = self.type_.value

        children = []
        for children_item_data in self.children:
            children_item: dict[str, Any]
            if isinstance(children_item_data, ExtractionConstraintLabelRef):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintAnySelected):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintAnyOtherSelected):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintIsDefault):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintCardinality):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintMinLevel):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintMaxLevel):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintAtLevel):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintNot):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintAnd):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintOr):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintExactlyOneOf):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintImplies):
                children_item = children_item_data.to_dict()
            elif isinstance(children_item_data, ExtractionConstraintIff):
                children_item = children_item_data.to_dict()
            else:
                children_item = children_item_data.to_dict()

            children.append(children_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "children": children,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_constraint_and import ExtractionConstraintAnd
        from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
        from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
        from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
        from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
        from ..models.extraction_constraint_excludes import ExtractionConstraintExcludes
        from ..models.extraction_constraint_iff import ExtractionConstraintIff
        from ..models.extraction_constraint_implies import ExtractionConstraintImplies
        from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
        from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
        from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
        from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
        from ..models.extraction_constraint_not import ExtractionConstraintNot
        from ..models.extraction_constraint_or import ExtractionConstraintOr

        d = dict(src_dict)
        type_ = ExtractionConstraintExactlyOneOfType(d.pop("type"))

        children = []
        _children = d.pop("children")
        for children_item_data in _children:

            def _parse_children_item(
                data: object,
            ) -> (
                ExtractionConstraintAnd
                | ExtractionConstraintAnyOtherSelected
                | ExtractionConstraintAnySelected
                | ExtractionConstraintAtLevel
                | ExtractionConstraintCardinality
                | ExtractionConstraintExactlyOneOf
                | ExtractionConstraintExcludes
                | ExtractionConstraintIff
                | ExtractionConstraintImplies
                | ExtractionConstraintIsDefault
                | ExtractionConstraintLabelRef
                | ExtractionConstraintMaxLevel
                | ExtractionConstraintMinLevel
                | ExtractionConstraintNot
                | ExtractionConstraintOr
            ):
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_0 = (
                        ExtractionConstraintLabelRef.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_1 = (
                        ExtractionConstraintAnySelected.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_2 = (
                        ExtractionConstraintAnyOtherSelected.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_3 = (
                        ExtractionConstraintIsDefault.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_3
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_4 = (
                        ExtractionConstraintCardinality.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_4
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_5 = (
                        ExtractionConstraintMinLevel.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_5
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_6 = (
                        ExtractionConstraintMaxLevel.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_6
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_7 = (
                        ExtractionConstraintAtLevel.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_7
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_8 = ExtractionConstraintNot.from_dict(
                        data
                    )

                    return componentsschemas_extraction_classification_constraint_type_8
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_9 = ExtractionConstraintAnd.from_dict(
                        data
                    )

                    return componentsschemas_extraction_classification_constraint_type_9
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_10 = ExtractionConstraintOr.from_dict(
                        data
                    )

                    return componentsschemas_extraction_classification_constraint_type_10
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_11 = (
                        ExtractionConstraintExactlyOneOf.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_11
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_12 = (
                        ExtractionConstraintImplies.from_dict(data)
                    )

                    return componentsschemas_extraction_classification_constraint_type_12
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_extraction_classification_constraint_type_13 = ExtractionConstraintIff.from_dict(
                        data
                    )

                    return componentsschemas_extraction_classification_constraint_type_13
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_14 = ExtractionConstraintExcludes.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_14

            children_item = _parse_children_item(children_item_data)

            children.append(children_item)

        extraction_constraint_exactly_one_of = cls(
            type_=type_,
            children=children,
        )

        return extraction_constraint_exactly_one_of
