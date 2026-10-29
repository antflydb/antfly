from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.enrichment_producer_config_type import EnrichmentProducerConfigType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.constrained_generator_producer_config import ConstrainedGeneratorProducerConfig
    from ..models.document_extraction_config import DocumentExtractionConfig
    from ..models.enrichment_producer_config_config_type_2 import EnrichmentProducerConfigConfigType2
    from ..models.generated_extractor_config import GeneratedExtractorConfig
    from ..models.media_output_config import MediaOutputConfig
    from ..models.vector_labels_config import VectorLabelsConfig


T = TypeVar("T", bound="EnrichmentProducerConfig")


@_attrs_define
class EnrichmentProducerConfig:
    """
    Attributes:
        type_ (EnrichmentProducerConfigType):
        config (ConstrainedGeneratorProducerConfig | DocumentExtractionConfig | EnrichmentProducerConfigConfigType2 |
            GeneratedExtractorConfig | MediaOutputConfig | Unset | VectorLabelsConfig): Producer-specific configuration.
            Generator schemas and generated extractors share constrained output admission, batching, validation retry, and
            immutable artifact replay.
    """

    type_: EnrichmentProducerConfigType
    config: (
        ConstrainedGeneratorProducerConfig
        | DocumentExtractionConfig
        | EnrichmentProducerConfigConfigType2
        | GeneratedExtractorConfig
        | MediaOutputConfig
        | Unset
        | VectorLabelsConfig
    ) = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.constrained_generator_producer_config import ConstrainedGeneratorProducerConfig
        from ..models.enrichment_producer_config_config_type_2 import EnrichmentProducerConfigConfigType2
        from ..models.generated_extractor_config import GeneratedExtractorConfig
        from ..models.media_output_config import MediaOutputConfig
        from ..models.vector_labels_config import VectorLabelsConfig

        type_ = self.type_.value

        config: dict[str, Any] | Unset
        if isinstance(self.config, Unset):
            config = UNSET
        elif isinstance(self.config, ConstrainedGeneratorProducerConfig):
            config = self.config.to_dict()
        elif isinstance(self.config, GeneratedExtractorConfig):
            config = self.config.to_dict()
        elif isinstance(self.config, EnrichmentProducerConfigConfigType2):
            config = self.config.to_dict()
        elif isinstance(self.config, VectorLabelsConfig):
            config = self.config.to_dict()
        elif isinstance(self.config, MediaOutputConfig):
            config = self.config.to_dict()
        else:
            config = self.config.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.constrained_generator_producer_config import ConstrainedGeneratorProducerConfig
        from ..models.document_extraction_config import DocumentExtractionConfig
        from ..models.enrichment_producer_config_config_type_2 import EnrichmentProducerConfigConfigType2
        from ..models.generated_extractor_config import GeneratedExtractorConfig
        from ..models.media_output_config import MediaOutputConfig
        from ..models.vector_labels_config import VectorLabelsConfig

        d = dict(src_dict)
        type_ = EnrichmentProducerConfigType(d.pop("type"))

        def _parse_config(
            data: object,
        ) -> (
            ConstrainedGeneratorProducerConfig
            | DocumentExtractionConfig
            | EnrichmentProducerConfigConfigType2
            | GeneratedExtractorConfig
            | MediaOutputConfig
            | Unset
            | VectorLabelsConfig
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_0 = ConstrainedGeneratorProducerConfig.from_dict(data)

                return config_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_1 = GeneratedExtractorConfig.from_dict(data)

                return config_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_2 = EnrichmentProducerConfigConfigType2.from_dict(data)

                return config_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_3 = VectorLabelsConfig.from_dict(data)

                return config_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_4 = MediaOutputConfig.from_dict(data)

                return config_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            config_type_5 = DocumentExtractionConfig.from_dict(data)

            return config_type_5

        config = _parse_config(d.pop("config", UNSET))

        enrichment_producer_config = cls(
            type_=type_,
            config=config,
        )

        return enrichment_producer_config
