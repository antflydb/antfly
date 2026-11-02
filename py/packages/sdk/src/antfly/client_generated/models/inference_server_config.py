from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resources_config import ResourcesConfig
    from ..models.runtime_config import RuntimeConfig


T = TypeVar("T", bound="InferenceServerConfig")


@_attrs_define
class InferenceServerConfig:
    """
    Attributes:
        resources (ResourcesConfig | Unset): Shared node-local resource policy for DB and standalone inference. All
            memory quantities are bytes.
        inference (RuntimeConfig | Unset):
    """

    resources: ResourcesConfig | Unset = UNSET
    inference: RuntimeConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        resources: dict[str, Any] | Unset = UNSET
        if not isinstance(self.resources, Unset):
            resources = self.resources.to_dict()

        inference: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inference, Unset):
            inference = self.inference.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if resources is not UNSET:
            field_dict["resources"] = resources
        if inference is not UNSET:
            field_dict["inference"] = inference

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resources_config import ResourcesConfig
        from ..models.runtime_config import RuntimeConfig

        d = dict(src_dict)
        _resources = d.pop("resources", UNSET)
        resources: ResourcesConfig | Unset
        if isinstance(_resources, Unset):
            resources = UNSET
        else:
            resources = ResourcesConfig.from_dict(_resources)

        _inference = d.pop("inference", UNSET)
        inference: RuntimeConfig | Unset
        if isinstance(_inference, Unset):
            inference = UNSET
        else:
            inference = RuntimeConfig.from_dict(_inference)

        inference_server_config = cls(
            resources=resources,
            inference=inference,
        )

        return inference_server_config
