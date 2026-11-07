from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.decider_cascade_stage_on_failure import DeciderCascadeStageOnFailure
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decider_cascade_stage_when import DeciderCascadeStageWhen
    from ..models.decider_config import DeciderConfig


T = TypeVar("T", bound="DeciderCascadeStage")


@_attrs_define
class DeciderCascadeStage:
    """
    Attributes:
        name (str): Unique within the cascade; reported in traces.
        decider (DeciderConfig):
        when (DeciderCascadeStageWhen | Unset): Required on every stage except the first, which must omit it. Inclusive
            band on the previous stage's answer.
        require_agreement (bool | Unset): Require this stage to agree with the preceding stage on the band question, or
            every question when unscoped. True is allowed only after the first stage. Disagreement stops this input and
            abstains; a keep_previous failure leaves agreement unverified and also abstains.
        on_failure (DeciderCascadeStageOnFailure | Unset): Defaults to fail, including on the first stage. fail ends the
            input with this stage's error; keep_previous keeps the previous answer and records the error in the trace, and
            is allowed only after the first stage. Default: DeciderCascadeStageOnFailure.FAIL.
    """

    name: str
    decider: DeciderConfig
    when: DeciderCascadeStageWhen | Unset = UNSET
    require_agreement: bool | Unset = UNSET
    on_failure: DeciderCascadeStageOnFailure | Unset = DeciderCascadeStageOnFailure.FAIL

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        decider = self.decider.to_dict()

        when: dict[str, Any] | Unset = UNSET
        if not isinstance(self.when, Unset):
            when = self.when.to_dict()

        require_agreement = self.require_agreement

        on_failure: str | Unset = UNSET
        if not isinstance(self.on_failure, Unset):
            on_failure = self.on_failure.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "decider": decider,
            }
        )
        if when is not UNSET:
            field_dict["when"] = when
        if require_agreement is not UNSET:
            field_dict["require_agreement"] = require_agreement
        if on_failure is not UNSET:
            field_dict["on_failure"] = on_failure

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decider_cascade_stage_when import DeciderCascadeStageWhen
        from ..models.decider_config import DeciderConfig

        d = dict(src_dict)
        name = d.pop("name")

        decider = DeciderConfig.from_dict(d.pop("decider"))

        _when = d.pop("when", UNSET)
        when: DeciderCascadeStageWhen | Unset
        if isinstance(_when, Unset):
            when = UNSET
        else:
            when = DeciderCascadeStageWhen.from_dict(_when)

        require_agreement = d.pop("require_agreement", UNSET)

        _on_failure = d.pop("on_failure", UNSET)
        on_failure: DeciderCascadeStageOnFailure | Unset
        if isinstance(_on_failure, Unset):
            on_failure = UNSET
        else:
            on_failure = DeciderCascadeStageOnFailure(_on_failure)

        decider_cascade_stage = cls(
            name=name,
            decider=decider,
            when=when,
            require_agreement=require_agreement,
            on_failure=on_failure,
        )

        return decider_cascade_stage
