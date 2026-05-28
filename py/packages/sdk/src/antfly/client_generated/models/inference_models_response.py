from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_models_response_object import InferenceModelsResponseObject

if TYPE_CHECKING:
    from ..models.inference_backend_runtimes import InferenceBackendRuntimes
    from ..models.inference_models_response_chunkers import InferenceModelsResponseChunkers
    from ..models.inference_models_response_classifiers import InferenceModelsResponseClassifiers
    from ..models.inference_models_response_data_item import InferenceModelsResponseDataItem
    from ..models.inference_models_response_embedders import InferenceModelsResponseEmbedders
    from ..models.inference_models_response_extractors import InferenceModelsResponseExtractors
    from ..models.inference_models_response_generators import InferenceModelsResponseGenerators
    from ..models.inference_models_response_readers import InferenceModelsResponseReaders
    from ..models.inference_models_response_recognizers import InferenceModelsResponseRecognizers
    from ..models.inference_models_response_rerankers import InferenceModelsResponseRerankers
    from ..models.inference_models_response_rewriters import InferenceModelsResponseRewriters
    from ..models.inference_models_response_transcribers import InferenceModelsResponseTranscribers


T = TypeVar("T", bound="InferenceModelsResponse")


@_attrs_define
class InferenceModelsResponse:
    """
    Attributes:
        object_ (InferenceModelsResponseObject): OpenAI-compatible response object type.
        data (list[InferenceModelsResponseDataItem]): OpenAI-compatible flat model list for generation/embedding models.
        allow_downloads (bool): Whether clients should show model download commands. Default: True.
        backends (InferenceBackendRuntimes): Runtime backends compiled into this inference server.
        chunkers (InferenceModelsResponseChunkers): Available chunking models (always includes "fixed")
        rerankers (InferenceModelsResponseRerankers): Available reranking models
        classifiers (InferenceModelsResponseClassifiers): Available zero-shot classification models
        embedders (InferenceModelsResponseEmbedders): Available embedding models from models_dir/embedders/
        extractors (InferenceModelsResponseExtractors): Available extractor models (models with 'extraction' capability)
        generators (InferenceModelsResponseGenerators): Available generator/LLM models from models_dir/generators/
        recognizers (InferenceModelsResponseRecognizers): Available recognizer models from models_dir/recognizers/
        rewriters (InferenceModelsResponseRewriters): Available Seq2Seq rewriter models from models_dir/rewriters/
        readers (InferenceModelsResponseReaders): Available reader/OCR models from models_dir/readers/
        transcribers (InferenceModelsResponseTranscribers): Available transcriber/speech-to-text models from
            models_dir/transcribers/
    """

    object_: InferenceModelsResponseObject
    data: list[InferenceModelsResponseDataItem]
    backends: InferenceBackendRuntimes
    chunkers: InferenceModelsResponseChunkers
    rerankers: InferenceModelsResponseRerankers
    classifiers: InferenceModelsResponseClassifiers
    embedders: InferenceModelsResponseEmbedders
    extractors: InferenceModelsResponseExtractors
    generators: InferenceModelsResponseGenerators
    recognizers: InferenceModelsResponseRecognizers
    rewriters: InferenceModelsResponseRewriters
    readers: InferenceModelsResponseReaders
    transcribers: InferenceModelsResponseTranscribers
    allow_downloads: bool = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        allow_downloads = self.allow_downloads

        backends = self.backends.to_dict()

        chunkers = self.chunkers.to_dict()

        rerankers = self.rerankers.to_dict()

        classifiers = self.classifiers.to_dict()

        embedders = self.embedders.to_dict()

        extractors = self.extractors.to_dict()

        generators = self.generators.to_dict()

        recognizers = self.recognizers.to_dict()

        rewriters = self.rewriters.to_dict()

        readers = self.readers.to_dict()

        transcribers = self.transcribers.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "data": data,
                "allow_downloads": allow_downloads,
                "backends": backends,
                "chunkers": chunkers,
                "rerankers": rerankers,
                "classifiers": classifiers,
                "embedders": embedders,
                "extractors": extractors,
                "generators": generators,
                "recognizers": recognizers,
                "rewriters": rewriters,
                "readers": readers,
                "transcribers": transcribers,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_backend_runtimes import InferenceBackendRuntimes
        from ..models.inference_models_response_chunkers import InferenceModelsResponseChunkers
        from ..models.inference_models_response_classifiers import InferenceModelsResponseClassifiers
        from ..models.inference_models_response_data_item import InferenceModelsResponseDataItem
        from ..models.inference_models_response_embedders import InferenceModelsResponseEmbedders
        from ..models.inference_models_response_extractors import InferenceModelsResponseExtractors
        from ..models.inference_models_response_generators import InferenceModelsResponseGenerators
        from ..models.inference_models_response_readers import InferenceModelsResponseReaders
        from ..models.inference_models_response_recognizers import InferenceModelsResponseRecognizers
        from ..models.inference_models_response_rerankers import InferenceModelsResponseRerankers
        from ..models.inference_models_response_rewriters import InferenceModelsResponseRewriters
        from ..models.inference_models_response_transcribers import InferenceModelsResponseTranscribers

        d = dict(src_dict)
        object_ = InferenceModelsResponseObject(d.pop("object"))

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = InferenceModelsResponseDataItem.from_dict(data_item_data)

            data.append(data_item)

        allow_downloads = d.pop("allow_downloads")

        backends = InferenceBackendRuntimes.from_dict(d.pop("backends"))

        chunkers = InferenceModelsResponseChunkers.from_dict(d.pop("chunkers"))

        rerankers = InferenceModelsResponseRerankers.from_dict(d.pop("rerankers"))

        classifiers = InferenceModelsResponseClassifiers.from_dict(d.pop("classifiers"))

        embedders = InferenceModelsResponseEmbedders.from_dict(d.pop("embedders"))

        extractors = InferenceModelsResponseExtractors.from_dict(d.pop("extractors"))

        generators = InferenceModelsResponseGenerators.from_dict(d.pop("generators"))

        recognizers = InferenceModelsResponseRecognizers.from_dict(d.pop("recognizers"))

        rewriters = InferenceModelsResponseRewriters.from_dict(d.pop("rewriters"))

        readers = InferenceModelsResponseReaders.from_dict(d.pop("readers"))

        transcribers = InferenceModelsResponseTranscribers.from_dict(d.pop("transcribers"))

        inference_models_response = cls(
            object_=object_,
            data=data,
            allow_downloads=allow_downloads,
            backends=backends,
            chunkers=chunkers,
            rerankers=rerankers,
            classifiers=classifiers,
            embedders=embedders,
            extractors=extractors,
            generators=generators,
            recognizers=recognizers,
            rewriters=rewriters,
            readers=readers,
            transcribers=transcribers,
        )

        inference_models_response.additional_properties = d
        return inference_models_response

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
