from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_info_compatibility import InferenceModelInfoCompatibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_model_info_chunking_limits import InferenceModelInfoChunkingLimits
    from ..models.inference_model_info_dense_output import InferenceModelInfoDenseOutput
    from ..models.inference_model_info_error import InferenceModelInfoError
    from ..models.inference_model_info_inference_capabilities import InferenceModelInfoInferenceCapabilities
    from ..models.inference_model_info_qualified_limits import InferenceModelInfoQualifiedLimits


T = TypeVar("T", bound="InferenceModelInfo")


@_attrs_define
class InferenceModelInfo:
    """Information about a model including its capabilities

    Attributes:
        chunking_limits (InferenceModelInfoChunkingLimits | Unset): Effective model-aware chunk operational ceilings for
            working/output/event
            bytes, chunk/dependency/index counts and remote fetched bytes, requests
            and cache bytes. Independent of model token limits and object length.
            Configure through NodeConfig.chunking_limits or ANTFLY_CHUNK_<NAME>.
            Scoped and unscoped embedding discovery expose the same values.
        inference_capabilities (InferenceModelInfoInferenceCapabilities | Unset): Resolved executor/task facts;
            generation bits are false when the selected executor does not implement them.
        dense_output (InferenceModelInfoDenseOutput | Unset): Proven native dense output width and inclusive supported
            truncation range, read from resolved metadata without loading weights. Omitted when unknown or for non-embedding
            tasks.
        model_revision (str | Unset): Immutable resolved embedding or generator generation, returned for scoped
            model/task discovery without loading weights.
        qualified_limits (InferenceModelInfoQualifiedLimits | Unset): Pull-time qualified request profiles for this
            exact artifact,
            including backend, supported features and min/max geometry.
            Runtime admission revalidates artifact identity and resource limits.
            Omitted for artifacts without a qualification receipt.
        compatibility (InferenceModelInfoCompatibility | Unset): Whether this artifact can run in the current build.
            `incompatible`
            artifacts are rejected before load; a model whose directory cannot
            be read is listed as `incompatible` with `error` set.
        error (InferenceModelInfoError | Unset): Why the model is unusable, set when its directory could not be read.
            Other models still list.
        capabilities (list[str] | Unset): List of capabilities this model supports (omitted when empty). For rerankers,
            `late_interaction` or `colbert` selects native MaxSim token scoring.
        inputs (list[str] | Unset): List of input modalities this model accepts, such as `text`, `image`, or `audio`
    """

    chunking_limits: InferenceModelInfoChunkingLimits | Unset = UNSET
    inference_capabilities: InferenceModelInfoInferenceCapabilities | Unset = UNSET
    dense_output: InferenceModelInfoDenseOutput | Unset = UNSET
    model_revision: str | Unset = UNSET
    qualified_limits: InferenceModelInfoQualifiedLimits | Unset = UNSET
    compatibility: InferenceModelInfoCompatibility | Unset = UNSET
    error: InferenceModelInfoError | Unset = UNSET
    capabilities: list[str] | Unset = UNSET
    inputs: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        chunking_limits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.chunking_limits, Unset):
            chunking_limits = self.chunking_limits.to_dict()

        inference_capabilities: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inference_capabilities, Unset):
            inference_capabilities = self.inference_capabilities.to_dict()

        dense_output: dict[str, Any] | Unset = UNSET
        if not isinstance(self.dense_output, Unset):
            dense_output = self.dense_output.to_dict()

        model_revision = self.model_revision

        qualified_limits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.qualified_limits, Unset):
            qualified_limits = self.qualified_limits.to_dict()

        compatibility: str | Unset = UNSET
        if not isinstance(self.compatibility, Unset):
            compatibility = self.compatibility.value

        error: dict[str, Any] | Unset = UNSET
        if not isinstance(self.error, Unset):
            error = self.error.to_dict()

        capabilities: list[str] | Unset = UNSET
        if not isinstance(self.capabilities, Unset):
            capabilities = self.capabilities

        inputs: list[str] | Unset = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = self.inputs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if chunking_limits is not UNSET:
            field_dict["chunking_limits"] = chunking_limits
        if inference_capabilities is not UNSET:
            field_dict["inference_capabilities"] = inference_capabilities
        if dense_output is not UNSET:
            field_dict["dense_output"] = dense_output
        if model_revision is not UNSET:
            field_dict["model_revision"] = model_revision
        if qualified_limits is not UNSET:
            field_dict["qualified_limits"] = qualified_limits
        if compatibility is not UNSET:
            field_dict["compatibility"] = compatibility
        if error is not UNSET:
            field_dict["error"] = error
        if capabilities is not UNSET:
            field_dict["capabilities"] = capabilities
        if inputs is not UNSET:
            field_dict["inputs"] = inputs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_model_info_chunking_limits import InferenceModelInfoChunkingLimits
        from ..models.inference_model_info_dense_output import InferenceModelInfoDenseOutput
        from ..models.inference_model_info_error import InferenceModelInfoError
        from ..models.inference_model_info_inference_capabilities import InferenceModelInfoInferenceCapabilities
        from ..models.inference_model_info_qualified_limits import InferenceModelInfoQualifiedLimits

        d = dict(src_dict)
        _chunking_limits = d.pop("chunking_limits", UNSET)
        chunking_limits: InferenceModelInfoChunkingLimits | Unset
        if isinstance(_chunking_limits, Unset):
            chunking_limits = UNSET
        else:
            chunking_limits = InferenceModelInfoChunkingLimits.from_dict(_chunking_limits)

        _inference_capabilities = d.pop("inference_capabilities", UNSET)
        inference_capabilities: InferenceModelInfoInferenceCapabilities | Unset
        if isinstance(_inference_capabilities, Unset):
            inference_capabilities = UNSET
        else:
            inference_capabilities = InferenceModelInfoInferenceCapabilities.from_dict(_inference_capabilities)

        _dense_output = d.pop("dense_output", UNSET)
        dense_output: InferenceModelInfoDenseOutput | Unset
        if isinstance(_dense_output, Unset):
            dense_output = UNSET
        else:
            dense_output = InferenceModelInfoDenseOutput.from_dict(_dense_output)

        model_revision = d.pop("model_revision", UNSET)

        _qualified_limits = d.pop("qualified_limits", UNSET)
        qualified_limits: InferenceModelInfoQualifiedLimits | Unset
        if isinstance(_qualified_limits, Unset):
            qualified_limits = UNSET
        else:
            qualified_limits = InferenceModelInfoQualifiedLimits.from_dict(_qualified_limits)

        _compatibility = d.pop("compatibility", UNSET)
        compatibility: InferenceModelInfoCompatibility | Unset
        if isinstance(_compatibility, Unset):
            compatibility = UNSET
        else:
            compatibility = InferenceModelInfoCompatibility(_compatibility)

        _error = d.pop("error", UNSET)
        error: InferenceModelInfoError | Unset
        if isinstance(_error, Unset):
            error = UNSET
        else:
            error = InferenceModelInfoError.from_dict(_error)

        capabilities = cast(list[str], d.pop("capabilities", UNSET))

        inputs = cast(list[str], d.pop("inputs", UNSET))

        inference_model_info = cls(
            chunking_limits=chunking_limits,
            inference_capabilities=inference_capabilities,
            dense_output=dense_output,
            model_revision=model_revision,
            qualified_limits=qualified_limits,
            compatibility=compatibility,
            error=error,
            capabilities=capabilities,
            inputs=inputs,
        )

        inference_model_info.additional_properties = d
        return inference_model_info

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
