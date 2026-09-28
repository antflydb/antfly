from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_retrieval_step_config import ResearchRetrievalStepConfig
    from ..models.research_step_config import ResearchStepConfig
    from ..models.research_write_step_config import ResearchWriteStepConfig


T = TypeVar("T", bound="ResearchAgentSteps")


@_attrs_define
class ResearchAgentSteps:
    """Per-role configuration for the research agent.

    Attributes:
        plan (ResearchStepConfig | Unset): Configuration for one research role. Generator and chain default to the top-
            level request values.
        research (ResearchRetrievalStepConfig | Unset): Configuration for researchers. Every researcher is a bounded
            retrieval
            agent run over the request's authorized queries. `tools` narrows the
            top-level tools policy and cannot widen it.
        reflect (ResearchStepConfig | Unset): Configuration for one research role. Generator and chain default to the
            top-level request values.
        write (ResearchWriteStepConfig | Unset): Configuration for the report writer.
        verify (ResearchStepConfig | Unset): Configuration for one research role. Generator and chain default to the
            top-level request values.
    """

    plan: ResearchStepConfig | Unset = UNSET
    research: ResearchRetrievalStepConfig | Unset = UNSET
    reflect: ResearchStepConfig | Unset = UNSET
    write: ResearchWriteStepConfig | Unset = UNSET
    verify: ResearchStepConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        plan: dict[str, Any] | Unset = UNSET
        if not isinstance(self.plan, Unset):
            plan = self.plan.to_dict()

        research: dict[str, Any] | Unset = UNSET
        if not isinstance(self.research, Unset):
            research = self.research.to_dict()

        reflect: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reflect, Unset):
            reflect = self.reflect.to_dict()

        write: dict[str, Any] | Unset = UNSET
        if not isinstance(self.write, Unset):
            write = self.write.to_dict()

        verify: dict[str, Any] | Unset = UNSET
        if not isinstance(self.verify, Unset):
            verify = self.verify.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if plan is not UNSET:
            field_dict["plan"] = plan
        if research is not UNSET:
            field_dict["research"] = research
        if reflect is not UNSET:
            field_dict["reflect"] = reflect
        if write is not UNSET:
            field_dict["write"] = write
        if verify is not UNSET:
            field_dict["verify"] = verify

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_retrieval_step_config import ResearchRetrievalStepConfig
        from ..models.research_step_config import ResearchStepConfig
        from ..models.research_write_step_config import ResearchWriteStepConfig

        d = dict(src_dict)
        _plan = d.pop("plan", UNSET)
        plan: ResearchStepConfig | Unset
        if isinstance(_plan, Unset):
            plan = UNSET
        else:
            plan = ResearchStepConfig.from_dict(_plan)

        _research = d.pop("research", UNSET)
        research: ResearchRetrievalStepConfig | Unset
        if isinstance(_research, Unset):
            research = UNSET
        else:
            research = ResearchRetrievalStepConfig.from_dict(_research)

        _reflect = d.pop("reflect", UNSET)
        reflect: ResearchStepConfig | Unset
        if isinstance(_reflect, Unset):
            reflect = UNSET
        else:
            reflect = ResearchStepConfig.from_dict(_reflect)

        _write = d.pop("write", UNSET)
        write: ResearchWriteStepConfig | Unset
        if isinstance(_write, Unset):
            write = UNSET
        else:
            write = ResearchWriteStepConfig.from_dict(_write)

        _verify = d.pop("verify", UNSET)
        verify: ResearchStepConfig | Unset
        if isinstance(_verify, Unset):
            verify = UNSET
        else:
            verify = ResearchStepConfig.from_dict(_verify)

        research_agent_steps = cls(
            plan=plan,
            research=research,
            reflect=reflect,
            write=write,
            verify=verify,
        )

        research_agent_steps.additional_properties = d
        return research_agent_steps

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
