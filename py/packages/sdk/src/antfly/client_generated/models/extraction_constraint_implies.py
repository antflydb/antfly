from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_constraint_implies_type import ExtractionConstraintImpliesType

if TYPE_CHECKING:
    from ..models.extraction_constraint_and import ExtractionConstraintAnd
    from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
    from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
    from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
    from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
    from ..models.extraction_constraint_exactly_one_of import ExtractionConstraintExactlyOneOf
    from ..models.extraction_constraint_excludes import ExtractionConstraintExcludes
    from ..models.extraction_constraint_iff import ExtractionConstraintIff
    from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
    from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
    from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
    from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
    from ..models.extraction_constraint_not import ExtractionConstraintNot
    from ..models.extraction_constraint_or import ExtractionConstraintOr


T = TypeVar("T", bound="ExtractionConstraintImplies")


@_attrs_define
class ExtractionConstraintImplies:
    """
    Attributes:
        type_ (ExtractionConstraintImpliesType):
        cond (ExtractionConstraintAnd | ExtractionConstraintAnyOtherSelected | ExtractionConstraintAnySelected |
            ExtractionConstraintAtLevel | ExtractionConstraintCardinality | ExtractionConstraintExactlyOneOf |
            ExtractionConstraintExcludes | ExtractionConstraintIff | ExtractionConstraintImplies |
            ExtractionConstraintIsDefault | ExtractionConstraintLabelRef | ExtractionConstraintMaxLevel |
            ExtractionConstraintMinLevel | ExtractionConstraintNot | ExtractionConstraintOr): Declarative, bounded
            constraint AST. Task and label references are validated before inference. Nesting is bounded by the server
            schema limit.
        then (ExtractionConstraintAnd | ExtractionConstraintAnyOtherSelected | ExtractionConstraintAnySelected |
            ExtractionConstraintAtLevel | ExtractionConstraintCardinality | ExtractionConstraintExactlyOneOf |
            ExtractionConstraintExcludes | ExtractionConstraintIff | ExtractionConstraintImplies |
            ExtractionConstraintIsDefault | ExtractionConstraintLabelRef | ExtractionConstraintMaxLevel |
            ExtractionConstraintMinLevel | ExtractionConstraintNot | ExtractionConstraintOr): Declarative, bounded
            constraint AST. Task and label references are validated before inference. Nesting is bounded by the server
            schema limit.
    """

    type_: ExtractionConstraintImpliesType
    cond: (
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
    )
    then: (
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
    )

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_constraint_and import ExtractionConstraintAnd
        from ..models.extraction_constraint_any_other_selected import ExtractionConstraintAnyOtherSelected
        from ..models.extraction_constraint_any_selected import ExtractionConstraintAnySelected
        from ..models.extraction_constraint_at_level import ExtractionConstraintAtLevel
        from ..models.extraction_constraint_cardinality import ExtractionConstraintCardinality
        from ..models.extraction_constraint_exactly_one_of import ExtractionConstraintExactlyOneOf
        from ..models.extraction_constraint_iff import ExtractionConstraintIff
        from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
        from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
        from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
        from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
        from ..models.extraction_constraint_not import ExtractionConstraintNot
        from ..models.extraction_constraint_or import ExtractionConstraintOr

        type_ = self.type_.value

        cond: dict[str, Any]
        if isinstance(self.cond, ExtractionConstraintLabelRef):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintAnySelected):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintAnyOtherSelected):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintIsDefault):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintCardinality):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintMinLevel):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintMaxLevel):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintAtLevel):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintNot):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintAnd):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintOr):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintExactlyOneOf):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintImplies):
            cond = self.cond.to_dict()
        elif isinstance(self.cond, ExtractionConstraintIff):
            cond = self.cond.to_dict()
        else:
            cond = self.cond.to_dict()

        then: dict[str, Any]
        if isinstance(self.then, ExtractionConstraintLabelRef):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintAnySelected):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintAnyOtherSelected):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintIsDefault):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintCardinality):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintMinLevel):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintMaxLevel):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintAtLevel):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintNot):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintAnd):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintOr):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintExactlyOneOf):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintImplies):
            then = self.then.to_dict()
        elif isinstance(self.then, ExtractionConstraintIff):
            then = self.then.to_dict()
        else:
            then = self.then.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "cond": cond,
                "then": then,
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
        from ..models.extraction_constraint_exactly_one_of import ExtractionConstraintExactlyOneOf
        from ..models.extraction_constraint_excludes import ExtractionConstraintExcludes
        from ..models.extraction_constraint_iff import ExtractionConstraintIff
        from ..models.extraction_constraint_is_default import ExtractionConstraintIsDefault
        from ..models.extraction_constraint_label_ref import ExtractionConstraintLabelRef
        from ..models.extraction_constraint_max_level import ExtractionConstraintMaxLevel
        from ..models.extraction_constraint_min_level import ExtractionConstraintMinLevel
        from ..models.extraction_constraint_not import ExtractionConstraintNot
        from ..models.extraction_constraint_or import ExtractionConstraintOr

        d = dict(src_dict)
        type_ = ExtractionConstraintImpliesType(d.pop("type"))

        def _parse_cond(
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
                componentsschemas_extraction_classification_constraint_type_0 = ExtractionConstraintLabelRef.from_dict(
                    data
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
                componentsschemas_extraction_classification_constraint_type_3 = ExtractionConstraintIsDefault.from_dict(
                    data
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
                componentsschemas_extraction_classification_constraint_type_5 = ExtractionConstraintMinLevel.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_6 = ExtractionConstraintMaxLevel.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_7 = ExtractionConstraintAtLevel.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_8 = ExtractionConstraintNot.from_dict(data)

                return componentsschemas_extraction_classification_constraint_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_9 = ExtractionConstraintAnd.from_dict(data)

                return componentsschemas_extraction_classification_constraint_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_10 = ExtractionConstraintOr.from_dict(data)

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
                componentsschemas_extraction_classification_constraint_type_12 = ExtractionConstraintImplies.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_13 = ExtractionConstraintIff.from_dict(data)

                return componentsschemas_extraction_classification_constraint_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_extraction_classification_constraint_type_14 = ExtractionConstraintExcludes.from_dict(
                data
            )

            return componentsschemas_extraction_classification_constraint_type_14

        cond = _parse_cond(d.pop("cond"))

        def _parse_then(
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
                componentsschemas_extraction_classification_constraint_type_0 = ExtractionConstraintLabelRef.from_dict(
                    data
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
                componentsschemas_extraction_classification_constraint_type_3 = ExtractionConstraintIsDefault.from_dict(
                    data
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
                componentsschemas_extraction_classification_constraint_type_5 = ExtractionConstraintMinLevel.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_6 = ExtractionConstraintMaxLevel.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_7 = ExtractionConstraintAtLevel.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_8 = ExtractionConstraintNot.from_dict(data)

                return componentsschemas_extraction_classification_constraint_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_9 = ExtractionConstraintAnd.from_dict(data)

                return componentsschemas_extraction_classification_constraint_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_10 = ExtractionConstraintOr.from_dict(data)

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
                componentsschemas_extraction_classification_constraint_type_12 = ExtractionConstraintImplies.from_dict(
                    data
                )

                return componentsschemas_extraction_classification_constraint_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_extraction_classification_constraint_type_13 = ExtractionConstraintIff.from_dict(data)

                return componentsschemas_extraction_classification_constraint_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_extraction_classification_constraint_type_14 = ExtractionConstraintExcludes.from_dict(
                data
            )

            return componentsschemas_extraction_classification_constraint_type_14

        then = _parse_then(d.pop("then"))

        extraction_constraint_implies = cls(
            type_=type_,
            cond=cond,
            then=then,
        )

        return extraction_constraint_implies
