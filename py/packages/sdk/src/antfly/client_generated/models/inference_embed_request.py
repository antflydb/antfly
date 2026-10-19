from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_embed_request_encoding_format import InferenceEmbedRequestEncodingFormat
from ..models.inference_embed_request_error_policy import InferenceEmbedRequestErrorPolicy
from ..models.inference_embed_request_input_type import InferenceEmbedRequestInputType
from ..models.inference_embed_request_task_type import InferenceEmbedRequestTaskType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.image_url_content_part import ImageURLContentPart
    from ..models.inference_embedding_content_input import InferenceEmbeddingContentInput
    from ..models.inference_embedding_group import InferenceEmbeddingGroup
    from ..models.media_content_part import MediaContentPart
    from ..models.text_content_part import TextContentPart


T = TypeVar("T", bound="InferenceEmbedRequest")


@_attrs_define
class InferenceEmbedRequest:
    r"""OpenAI-compatible embedding request with inference multimodal content-part extension. EmbeddingGemma 2 text encoding
    uses official task prefixes, a shared 8192-token limit, mean pooling including prompt tokens, and normalized
    vectors.

        Attributes:
            model (str): Model name to use for embedding generation
            input_ (InferenceEmbeddingContentInput | list[ImageURLContentPart | MediaContentPart | TextContentPart] |
                list[InferenceEmbeddingContentInput] | list[InferenceEmbeddingGroup] | list[str] | str): Input content to embed.
                Supports:
                - a single string
                - an array of strings
                - an array of OpenAI-style content parts for multimodal embedding
                - an object with an ordered content array, producing one embedding
                - an array of ordered content objects, producing one embedding per object
                Legacy arrays of content parts continue to produce one embedding per part.
            model_identity (str | Unset): Optional exact asset identity pin. A mismatch returns 409 before inference.
            encoding_format (InferenceEmbedRequestEncodingFormat | Unset): Encoding format for the embeddings (only "float"
                supported) Default: InferenceEmbedRequestEncodingFormat.FLOAT.
            dimensions (int | Unset): Optional truncation size for dense embeddings. Must be a positive integer no larger
                than the model embedding size. For normalized models the truncated vector is L2-re-normalized (Matryoshka
                semantics, matching the OpenAI dimensions parameter). EmbeddingGemma 2 is trained for 768, 512, 256, and 128
                dimensions. Not supported for sparse models.
            task_type (InferenceEmbedRequestTaskType | Unset): Optional embedding task type using Google embedding task-type
                names. EmbeddingGemma 2 applies its official prompt for each of the eight task types to text-bearing inputs and
                rejects custom instructions. For Jina v5 text embeddings, query-side tasks use the query prefix and
                RETRIEVAL_DOCUMENT uses the document prefix. For Qwen3-Embedding models, RETRIEVAL_QUERY uses the model's built-
                in web-retrieval instruction, RETRIEVAL_DOCUMENT is embedded raw, and every other task type requires an explicit
                instruction.
            instruction (str | Unset): Task description for instruction-aware embedding models (Qwen3-Embedding), rendered
                inside the query instruction wrapper ("Instruct: {instruction}\nQuery:{input}"). Optional for RETRIEVAL_QUERY,
                which has a model-owned default; required for other non-document task types; rejected for document tasks and
                models without instruction support.
            input_type (InferenceEmbedRequestInputType | Unset): Deprecated compatibility alias for task_type.
                search_query/query map to RETRIEVAL_QUERY; search_document/document map to RETRIEVAL_DOCUMENT; classification
                and clustering map to their Google task_type equivalents.
            error_policy (InferenceEmbedRequestErrorPolicy | Unset): Controls how dense embedding requests report per-input
                failures.
                `fail_fast` preserves OpenAI-compatible all-or-error behavior.
                `per_item` returns successful embeddings in `data` and indexed
                permanent/transient failures in `errors` without failing the
                entire HTTP request.
                 Default: InferenceEmbedRequestErrorPolicy.FAIL_FAST.
    """

    model: str
    input_: (
        InferenceEmbeddingContentInput
        | list[ImageURLContentPart | MediaContentPart | TextContentPart]
        | list[InferenceEmbeddingContentInput]
        | list[InferenceEmbeddingGroup]
        | list[str]
        | str
    )
    model_identity: str | Unset = UNSET
    encoding_format: InferenceEmbedRequestEncodingFormat | Unset = InferenceEmbedRequestEncodingFormat.FLOAT
    dimensions: int | Unset = UNSET
    task_type: InferenceEmbedRequestTaskType | Unset = UNSET
    instruction: str | Unset = UNSET
    input_type: InferenceEmbedRequestInputType | Unset = UNSET
    error_policy: InferenceEmbedRequestErrorPolicy | Unset = InferenceEmbedRequestErrorPolicy.FAIL_FAST
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.inference_embedding_content_input import InferenceEmbeddingContentInput
        from ..models.inference_embedding_group import InferenceEmbeddingGroup
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        model = self.model

        input_: dict[str, Any] | list[dict[str, Any]] | list[str] | str
        if isinstance(self.input_, str):
            input_ = self.input_
        elif isinstance(self.input_, InferenceEmbeddingContentInput):
            input_ = self.input_.to_dict()
        elif isinstance(self.input_, list):
            if all(isinstance(item, str) for item in self.input_):
                input_ = list(self.input_)
            elif all(
                isinstance(item, (InferenceEmbeddingContentInput, InferenceEmbeddingGroup)) for item in self.input_
            ):
                input_ = [item.to_dict() for item in self.input_]
            elif all(
                isinstance(item, (TextContentPart, ImageURLContentPart, MediaContentPart)) for item in self.input_
            ):
                input_ = [item.to_dict() for item in self.input_]
            else:
                raise TypeError("input list must contain only strings, content parts, or ordered content inputs")
        else:
            raise TypeError("input must be a string, list, or ordered content input")

        model_identity = self.model_identity

        encoding_format: str | Unset = UNSET
        if not isinstance(self.encoding_format, Unset):
            encoding_format = self.encoding_format.value

        dimensions = self.dimensions

        task_type: str | Unset = UNSET
        if not isinstance(self.task_type, Unset):
            task_type = self.task_type.value

        instruction = self.instruction

        input_type: str | Unset = UNSET
        if not isinstance(self.input_type, Unset):
            input_type = self.input_type.value

        error_policy: str | Unset = UNSET
        if not isinstance(self.error_policy, Unset):
            error_policy = self.error_policy.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "input": input_,
            }
        )
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if encoding_format is not UNSET:
            field_dict["encoding_format"] = encoding_format
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if task_type is not UNSET:
            field_dict["task_type"] = task_type
        if instruction is not UNSET:
            field_dict["instruction"] = instruction
        if input_type is not UNSET:
            field_dict["input_type"] = input_type
        if error_policy is not UNSET:
            field_dict["error_policy"] = error_policy

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.inference_embedding_content_input import InferenceEmbeddingContentInput
        from ..models.inference_embedding_group import InferenceEmbeddingGroup
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        d = dict(src_dict)
        model = d.pop("model")

        def _parse_input_(
            data: object,
        ) -> (
            InferenceEmbeddingContentInput
            | list[ImageURLContentPart | MediaContentPart | TextContentPart]
            | list[InferenceEmbeddingContentInput | InferenceEmbeddingGroup]
            | list[str]
            | str
        ):
            if isinstance(data, str):
                return data
            if isinstance(data, dict):
                if "content" not in data:
                    raise TypeError("ordered embedding input must contain content")
                return InferenceEmbeddingContentInput.from_dict(data)
            if not isinstance(data, list):
                raise TypeError("input must be a string, list, or ordered content input")
            if all(isinstance(item, str) for item in data):
                return cast(list[str], data)
            if not all(isinstance(item, dict) for item in data):
                raise TypeError("input list must contain only strings or objects")
            object_items = cast(list[dict[str, Any]], data)
            ordered = ["content" in item for item in object_items]
            if any(ordered):
                if not all(ordered):
                    raise TypeError("ordered content inputs cannot be mixed with legacy content parts")
                return [
                    InferenceEmbeddingGroup.from_dict(item)
                    if "title" in item
                    else InferenceEmbeddingContentInput.from_dict(item)
                    for item in object_items
                ]

            content_parts: list[ImageURLContentPart | MediaContentPart | TextContentPart] = []
            for item in object_items:
                kind = item.get("type")
                if kind == "text":
                    content_parts.append(TextContentPart.from_dict(item))
                elif kind == "image_url":
                    content_parts.append(ImageURLContentPart.from_dict(item))
                elif kind == "media":
                    content_parts.append(MediaContentPart.from_dict(item))
                else:
                    raise TypeError(f"unsupported content part type: {kind!r}")
            return content_parts

        input_ = _parse_input_(d.pop("input"))

        model_identity = d.pop("model_identity", UNSET)

        _encoding_format = d.pop("encoding_format", UNSET)
        encoding_format: InferenceEmbedRequestEncodingFormat | Unset
        if isinstance(_encoding_format, Unset):
            encoding_format = UNSET
        else:
            encoding_format = InferenceEmbedRequestEncodingFormat(_encoding_format)

        dimensions = d.pop("dimensions", UNSET)

        _task_type = d.pop("task_type", UNSET)
        task_type: InferenceEmbedRequestTaskType | Unset
        if isinstance(_task_type, Unset):
            task_type = UNSET
        else:
            task_type = InferenceEmbedRequestTaskType(_task_type)

        instruction = d.pop("instruction", UNSET)

        _input_type = d.pop("input_type", UNSET)
        input_type: InferenceEmbedRequestInputType | Unset
        if isinstance(_input_type, Unset):
            input_type = UNSET
        else:
            input_type = InferenceEmbedRequestInputType(_input_type)

        _error_policy = d.pop("error_policy", UNSET)
        error_policy: InferenceEmbedRequestErrorPolicy | Unset
        if isinstance(_error_policy, Unset):
            error_policy = UNSET
        else:
            error_policy = InferenceEmbedRequestErrorPolicy(_error_policy)

        inference_embed_request = cls(
            model=model,
            input_=input_,
            model_identity=model_identity,
            encoding_format=encoding_format,
            dimensions=dimensions,
            task_type=task_type,
            instruction=instruction,
            input_type=input_type,
            error_policy=error_policy,
        )

        inference_embed_request.additional_properties = d
        return inference_embed_request

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
