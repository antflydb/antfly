from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_submit_model_type_0 import MemoryEvaluationSubmitModelType0
    from ..models.memory_evaluation_verdict import MemoryEvaluationVerdict


T = TypeVar("T", bound="MemoryEvaluationSubmit")


@_attrs_define
class MemoryEvaluationSubmit:
    """
    Attributes:
        expected_revision (int):
        verdicts (list[MemoryEvaluationVerdict]):
        actor (str | Unset):
        model (MemoryEvaluationSubmitModelType0 | None | Unset):
    """

    expected_revision: int
    verdicts: list[MemoryEvaluationVerdict]
    actor: str | Unset = UNSET
    model: MemoryEvaluationSubmitModelType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_evaluation_submit_model_type_0 import MemoryEvaluationSubmitModelType0

        expected_revision = self.expected_revision

        verdicts = []
        for verdicts_item_data in self.verdicts:
            verdicts_item = verdicts_item_data.to_dict()
            verdicts.append(verdicts_item)

        actor = self.actor

        model: dict[str, Any] | None | Unset
        if isinstance(self.model, Unset):
            model = UNSET
        elif isinstance(self.model, MemoryEvaluationSubmitModelType0):
            model = self.model.to_dict()
        else:
            model = self.model

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expected_revision": expected_revision,
                "verdicts": verdicts,
            }
        )
        if actor is not UNSET:
            field_dict["actor"] = actor
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_submit_model_type_0 import MemoryEvaluationSubmitModelType0
        from ..models.memory_evaluation_verdict import MemoryEvaluationVerdict

        d = dict(src_dict)
        expected_revision = d.pop("expected_revision")

        verdicts = []
        _verdicts = d.pop("verdicts")
        for verdicts_item_data in _verdicts:
            verdicts_item = MemoryEvaluationVerdict.from_dict(verdicts_item_data)

            verdicts.append(verdicts_item)

        actor = d.pop("actor", UNSET)

        def _parse_model(data: object) -> MemoryEvaluationSubmitModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                model_type_0 = MemoryEvaluationSubmitModelType0.from_dict(data)

                return model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationSubmitModelType0 | None | Unset, data)

        model = _parse_model(d.pop("model", UNSET))

        memory_evaluation_submit = cls(
            expected_revision=expected_revision,
            verdicts=verdicts,
            actor=actor,
            model=model,
        )

        return memory_evaluation_submit
