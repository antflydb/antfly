from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_generate_request_backend import InferenceGenerateRequestBackend
from ..models.inference_generate_request_cache_dtype import InferenceGenerateRequestCacheDtype
from ..models.inference_generate_request_compiled_target import InferenceGenerateRequestCompiledTarget
from ..models.inference_generate_request_mode import InferenceGenerateRequestMode
from ..models.inference_tool_choice_type_0 import InferenceToolChoiceType0
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_chat_message import InferenceChatMessage
    from ..models.inference_generate_response_format import InferenceGenerateResponseFormat
    from ..models.inference_tool import InferenceTool
    from ..models.inference_tool_choice_type_1 import InferenceToolChoiceType1


T = TypeVar("T", bound="InferenceGenerateRequest")


@_attrs_define
class InferenceGenerateRequest:
    """
    Attributes:
        model (str): Name of the generator model from models_dir/generators/ Example: google/gemma-3-1b-it.
        messages (list[InferenceChatMessage]): Conversation messages (OpenAI-compatible format)
        max_tokens (int | Unset): Maximum tokens to generate Default: 256. Example: 256.
        temperature (float | Unset): Sampling temperature (0.0 = deterministic, higher = more random) Default: 1.0.
            Example: 0.7.
        top_p (float | Unset): Nucleus sampling probability Default: 1.0.
        top_k (int | Unset): Top-k sampling (inference extension, not in OpenAI API) Default: 50.
        stream (bool | Unset): If true, partial message deltas will be sent as SSE events Default: False.
        tools (list[InferenceTool] | Unset): List of tools (functions) the model can call.
            Only supported by models with tool_call_format configured.
        min_p (float | Unset): Min-p sampling threshold. Filters tokens where p < min_p * max_p. Simpler alternative to
            top_p. Default: 0.0.
        repetition_penalty (float | Unset): Repetition penalty factor applied to previously generated tokens (1.0 =
            disabled, >1.0 penalizes, <1.0 encourages) Default: 1.0.
        frequency_penalty (float | Unset): Additive penalty based on token frequency in context (logit -=
            frequency_penalty * count) Default: 0.0.
        presence_penalty (float | Unset): Additive penalty for tokens that appeared at all in context (logit -=
            presence_penalty if count > 0) Default: 0.0.
        response_format (InferenceGenerateResponseFormat | Unset):
        grammar (str | Unset): inference-native grammar override. When set, this takes precedence over
            `response_format`.
            Grammar-constrained decoding is currently native-backend only.
        draft_model (str | Unset): inference-native speculative decoding extension. Path or model identifier for a
            smaller draft model.
        speculative_k (int | Unset): inference-native speculative decoding extension. Number of draft tokens proposed
            per verification round.
             Default: 4.
        cache_dtype (InferenceGenerateRequestCacheDtype | Unset): inference-native KV cache quantization format. Lower
            precision reduces memory usage but may
            affect generation quality. Default auto-selects based on backend (f16 for GPU, f32 for CPU).
        cache_compaction_ratio (float | Unset): inference-native KV cache compaction ratio applied after prefill via
            Attention Matching.
            Selects a subset of keys and fits new values via OLS to preserve attention behavior.
            0.02 = 50x compression, 0.1 = 10x, 0.5 = 2x. Null/omitted = no compaction.
        backend (InferenceGenerateRequestBackend | Unset): Optional backend override for this request.
            `auto` keeps the node default behavior.
            `onnx` forces ONNX generation when the model/package supports it.
            `native`, `metal`, and `mlx` force the native host backend choice.
            `xla` runs native generation with explicit PJRT/XLA compiled graph partitions and
            requires a PJRT plugin path via `ANTFLY_INFERENCE_XLA_PLUGIN`,
            `ANTFLY_INFERENCE_PJRT_PLUGIN`, `PJRT_PLUGIN_PATH`, or `PJRT_PLUGIN`.
            `webgpu` selects the Wasm/WebGPU backend in Wasm builds; pair it with
            `mode: "compiled"` to request WebGPU graph partition execution.
        mode (InferenceGenerateRequestMode | Unset): inference-native graph execution mode. `eager` keeps the direct
            runtime path when possible.
            `compiled` runs inference graph planning, partitioning, and backend executor attachment.
        compiled_target (InferenceGenerateRequestCompiledTarget | Unset): inference-native compiled graph target.
            `partitioned` attaches compiled executors to eligible
            graph partitions. `whole-model` requests a compiled backend only when it can own the full
            traced graph shape.
        tool_choice (InferenceToolChoiceType0 | InferenceToolChoiceType1 | Unset): Controls how the model uses tools.
            Options:
            - "auto": Model decides whether to call a tool (default)
            - "none": Model will not call any tools
            - "required": Model must call at least one tool
            - object: Force a specific function to be called
    """

    model: str
    messages: list[InferenceChatMessage]
    max_tokens: int | Unset = 256
    temperature: float | Unset = 1.0
    top_p: float | Unset = 1.0
    top_k: int | Unset = 50
    stream: bool | Unset = False
    tools: list[InferenceTool] | Unset = UNSET
    min_p: float | Unset = 0.0
    repetition_penalty: float | Unset = 1.0
    frequency_penalty: float | Unset = 0.0
    presence_penalty: float | Unset = 0.0
    response_format: InferenceGenerateResponseFormat | Unset = UNSET
    grammar: str | Unset = UNSET
    draft_model: str | Unset = UNSET
    speculative_k: int | Unset = 4
    cache_dtype: InferenceGenerateRequestCacheDtype | Unset = UNSET
    cache_compaction_ratio: float | Unset = UNSET
    backend: InferenceGenerateRequestBackend | Unset = UNSET
    mode: InferenceGenerateRequestMode | Unset = UNSET
    compiled_target: InferenceGenerateRequestCompiledTarget | Unset = UNSET
    tool_choice: InferenceToolChoiceType0 | InferenceToolChoiceType1 | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        messages = []
        for messages_item_data in self.messages:
            messages_item = messages_item_data.to_dict()
            messages.append(messages_item)

        max_tokens = self.max_tokens

        temperature = self.temperature

        top_p = self.top_p

        top_k = self.top_k

        stream = self.stream

        tools: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tools, Unset):
            tools = []
            for tools_item_data in self.tools:
                tools_item = tools_item_data.to_dict()
                tools.append(tools_item)

        min_p = self.min_p

        repetition_penalty = self.repetition_penalty

        frequency_penalty = self.frequency_penalty

        presence_penalty = self.presence_penalty

        response_format: dict[str, Any] | Unset = UNSET
        if not isinstance(self.response_format, Unset):
            response_format = self.response_format.to_dict()

        grammar = self.grammar

        draft_model = self.draft_model

        speculative_k = self.speculative_k

        cache_dtype: str | Unset = UNSET
        if not isinstance(self.cache_dtype, Unset):
            cache_dtype = self.cache_dtype.value

        cache_compaction_ratio = self.cache_compaction_ratio

        backend: str | Unset = UNSET
        if not isinstance(self.backend, Unset):
            backend = self.backend.value

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        compiled_target: str | Unset = UNSET
        if not isinstance(self.compiled_target, Unset):
            compiled_target = self.compiled_target.value

        tool_choice: dict[str, Any] | str | Unset
        if isinstance(self.tool_choice, Unset):
            tool_choice = UNSET
        elif isinstance(self.tool_choice, InferenceToolChoiceType0):
            tool_choice = self.tool_choice.value
        else:
            tool_choice = self.tool_choice.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "messages": messages,
            }
        )
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if stream is not UNSET:
            field_dict["stream"] = stream
        if tools is not UNSET:
            field_dict["tools"] = tools
        if min_p is not UNSET:
            field_dict["min_p"] = min_p
        if repetition_penalty is not UNSET:
            field_dict["repetition_penalty"] = repetition_penalty
        if frequency_penalty is not UNSET:
            field_dict["frequency_penalty"] = frequency_penalty
        if presence_penalty is not UNSET:
            field_dict["presence_penalty"] = presence_penalty
        if response_format is not UNSET:
            field_dict["response_format"] = response_format
        if grammar is not UNSET:
            field_dict["grammar"] = grammar
        if draft_model is not UNSET:
            field_dict["draft_model"] = draft_model
        if speculative_k is not UNSET:
            field_dict["speculative_k"] = speculative_k
        if cache_dtype is not UNSET:
            field_dict["cache_dtype"] = cache_dtype
        if cache_compaction_ratio is not UNSET:
            field_dict["cache_compaction_ratio"] = cache_compaction_ratio
        if backend is not UNSET:
            field_dict["backend"] = backend
        if mode is not UNSET:
            field_dict["mode"] = mode
        if compiled_target is not UNSET:
            field_dict["compiled_target"] = compiled_target
        if tool_choice is not UNSET:
            field_dict["tool_choice"] = tool_choice

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_chat_message import InferenceChatMessage
        from ..models.inference_generate_response_format import InferenceGenerateResponseFormat
        from ..models.inference_tool import InferenceTool
        from ..models.inference_tool_choice_type_1 import InferenceToolChoiceType1

        d = dict(src_dict)
        model = d.pop("model")

        messages = []
        _messages = d.pop("messages")
        for messages_item_data in _messages:
            messages_item = InferenceChatMessage.from_dict(messages_item_data)

            messages.append(messages_item)

        max_tokens = d.pop("max_tokens", UNSET)

        temperature = d.pop("temperature", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        stream = d.pop("stream", UNSET)

        _tools = d.pop("tools", UNSET)
        tools: list[InferenceTool] | Unset = UNSET
        if _tools is not UNSET:
            tools = []
            for tools_item_data in _tools:
                tools_item = InferenceTool.from_dict(tools_item_data)

                tools.append(tools_item)

        min_p = d.pop("min_p", UNSET)

        repetition_penalty = d.pop("repetition_penalty", UNSET)

        frequency_penalty = d.pop("frequency_penalty", UNSET)

        presence_penalty = d.pop("presence_penalty", UNSET)

        _response_format = d.pop("response_format", UNSET)
        response_format: InferenceGenerateResponseFormat | Unset
        if isinstance(_response_format, Unset):
            response_format = UNSET
        else:
            response_format = InferenceGenerateResponseFormat.from_dict(_response_format)

        grammar = d.pop("grammar", UNSET)

        draft_model = d.pop("draft_model", UNSET)

        speculative_k = d.pop("speculative_k", UNSET)

        _cache_dtype = d.pop("cache_dtype", UNSET)
        cache_dtype: InferenceGenerateRequestCacheDtype | Unset
        if isinstance(_cache_dtype, Unset):
            cache_dtype = UNSET
        else:
            cache_dtype = InferenceGenerateRequestCacheDtype(_cache_dtype)

        cache_compaction_ratio = d.pop("cache_compaction_ratio", UNSET)

        _backend = d.pop("backend", UNSET)
        backend: InferenceGenerateRequestBackend | Unset
        if isinstance(_backend, Unset):
            backend = UNSET
        else:
            backend = InferenceGenerateRequestBackend(_backend)

        _mode = d.pop("mode", UNSET)
        mode: InferenceGenerateRequestMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = InferenceGenerateRequestMode(_mode)

        _compiled_target = d.pop("compiled_target", UNSET)
        compiled_target: InferenceGenerateRequestCompiledTarget | Unset
        if isinstance(_compiled_target, Unset):
            compiled_target = UNSET
        else:
            compiled_target = InferenceGenerateRequestCompiledTarget(_compiled_target)

        def _parse_tool_choice(data: object) -> InferenceToolChoiceType0 | InferenceToolChoiceType1 | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_inference_tool_choice_type_0 = InferenceToolChoiceType0(data)

                return componentsschemas_inference_tool_choice_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_inference_tool_choice_type_1 = InferenceToolChoiceType1.from_dict(data)

            return componentsschemas_inference_tool_choice_type_1

        tool_choice = _parse_tool_choice(d.pop("tool_choice", UNSET))

        inference_generate_request = cls(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            stream=stream,
            tools=tools,
            min_p=min_p,
            repetition_penalty=repetition_penalty,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            response_format=response_format,
            grammar=grammar,
            draft_model=draft_model,
            speculative_k=speculative_k,
            cache_dtype=cache_dtype,
            cache_compaction_ratio=cache_compaction_ratio,
            backend=backend,
            mode=mode,
            compiled_target=compiled_target,
            tool_choice=tool_choice,
        )

        inference_generate_request.additional_properties = d
        return inference_generate_request

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
