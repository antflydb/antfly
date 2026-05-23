from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.termite_models_response_chunkers import TermiteModelsResponseChunkers
    from ..models.termite_models_response_classifiers import TermiteModelsResponseClassifiers
    from ..models.termite_models_response_embedders import TermiteModelsResponseEmbedders
    from ..models.termite_models_response_extractors import TermiteModelsResponseExtractors
    from ..models.termite_models_response_generators import TermiteModelsResponseGenerators
    from ..models.termite_models_response_readers import TermiteModelsResponseReaders
    from ..models.termite_models_response_recognizers import TermiteModelsResponseRecognizers
    from ..models.termite_models_response_rerankers import TermiteModelsResponseRerankers
    from ..models.termite_models_response_rewriters import TermiteModelsResponseRewriters
    from ..models.termite_models_response_transcribers import TermiteModelsResponseTranscribers


T = TypeVar("T", bound="TermiteModelsResponse")


@_attrs_define
class TermiteModelsResponse:
    """
    Attributes:
        chunkers (TermiteModelsResponseChunkers): Available chunking models (always includes "fixed")
        rerankers (TermiteModelsResponseRerankers): Available reranking models
        classifiers (TermiteModelsResponseClassifiers): Available zero-shot classification models
        embedders (TermiteModelsResponseEmbedders): Available embedding models from models_dir/embedders/
        extractors (TermiteModelsResponseExtractors): Available extractor models (models with 'extraction' capability)
        generators (TermiteModelsResponseGenerators): Available generator/LLM models from models_dir/generators/
        recognizers (TermiteModelsResponseRecognizers): Available recognizer models from models_dir/recognizers/
        rewriters (TermiteModelsResponseRewriters): Available Seq2Seq rewriter models from models_dir/rewriters/
        readers (TermiteModelsResponseReaders): Available reader/OCR models from models_dir/readers/
        transcribers (TermiteModelsResponseTranscribers): Available transcriber/speech-to-text models from
            models_dir/transcribers/
    """

    chunkers: TermiteModelsResponseChunkers
    rerankers: TermiteModelsResponseRerankers
    classifiers: TermiteModelsResponseClassifiers
    embedders: TermiteModelsResponseEmbedders
    extractors: TermiteModelsResponseExtractors
    generators: TermiteModelsResponseGenerators
    recognizers: TermiteModelsResponseRecognizers
    rewriters: TermiteModelsResponseRewriters
    readers: TermiteModelsResponseReaders
    transcribers: TermiteModelsResponseTranscribers
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
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
        from ..models.termite_models_response_chunkers import TermiteModelsResponseChunkers
        from ..models.termite_models_response_classifiers import TermiteModelsResponseClassifiers
        from ..models.termite_models_response_embedders import TermiteModelsResponseEmbedders
        from ..models.termite_models_response_extractors import TermiteModelsResponseExtractors
        from ..models.termite_models_response_generators import TermiteModelsResponseGenerators
        from ..models.termite_models_response_readers import TermiteModelsResponseReaders
        from ..models.termite_models_response_recognizers import TermiteModelsResponseRecognizers
        from ..models.termite_models_response_rerankers import TermiteModelsResponseRerankers
        from ..models.termite_models_response_rewriters import TermiteModelsResponseRewriters
        from ..models.termite_models_response_transcribers import TermiteModelsResponseTranscribers

        d = dict(src_dict)
        chunkers = TermiteModelsResponseChunkers.from_dict(d.pop("chunkers"))

        rerankers = TermiteModelsResponseRerankers.from_dict(d.pop("rerankers"))

        classifiers = TermiteModelsResponseClassifiers.from_dict(d.pop("classifiers"))

        embedders = TermiteModelsResponseEmbedders.from_dict(d.pop("embedders"))

        extractors = TermiteModelsResponseExtractors.from_dict(d.pop("extractors"))

        generators = TermiteModelsResponseGenerators.from_dict(d.pop("generators"))

        recognizers = TermiteModelsResponseRecognizers.from_dict(d.pop("recognizers"))

        rewriters = TermiteModelsResponseRewriters.from_dict(d.pop("rewriters"))

        readers = TermiteModelsResponseReaders.from_dict(d.pop("readers"))

        transcribers = TermiteModelsResponseTranscribers.from_dict(d.pop("transcribers"))

        termite_models_response = cls(
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

        termite_models_response.additional_properties = d
        return termite_models_response

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
