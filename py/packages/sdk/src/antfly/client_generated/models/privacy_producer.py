from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.privacy_producer_type import PrivacyProducerType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.constrained_generator_producer_config import ConstrainedGeneratorProducerConfig
    from ..models.extraction_config import ExtractionConfig
    from ..models.generated_extractor_config import GeneratedExtractorConfig


T = TypeVar("T", bound="PrivacyProducer")


@_attrs_define
class PrivacyProducer:
    """
    Attributes:
        type_ (PrivacyProducerType):
        config (ConstrainedGeneratorProducerConfig | ExtractionConfig | GeneratedExtractorConfig): Shared artifact
            producer configuration; privacy owns required categories and literal source spans. Credentials must use secret
            references or environment defaults.
        revision (str | Unset): Immutable provider/model deployment revision. The qualified default native model
            resolves its pinned revision automatically.
    """

    type_: PrivacyProducerType
    config: ConstrainedGeneratorProducerConfig | ExtractionConfig | GeneratedExtractorConfig
    revision: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.extraction_config import ExtractionConfig
        from ..models.generated_extractor_config import GeneratedExtractorConfig

        type_ = self.type_.value

        config: dict[str, Any]
        if isinstance(self.config, ExtractionConfig):
            config = self.config.to_dict()
        elif isinstance(self.config, GeneratedExtractorConfig):
            config = self.config.to_dict()
        else:
            config = self.config.to_dict()

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "config": config,
            }
        )
        if revision is not UNSET:
            field_dict["revision"] = revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.constrained_generator_producer_config import ConstrainedGeneratorProducerConfig
        from ..models.extraction_config import ExtractionConfig
        from ..models.generated_extractor_config import GeneratedExtractorConfig

        d = dict(src_dict)
        type_ = PrivacyProducerType(d.pop("type"))

        def _parse_config(
            data: object,
        ) -> ConstrainedGeneratorProducerConfig | ExtractionConfig | GeneratedExtractorConfig:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                config_type_0 = ExtractionConfig.from_dict(data)

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
            if not isinstance(data, dict):
                raise TypeError()
            config_type_2 = ConstrainedGeneratorProducerConfig.from_dict(data)

            return config_type_2

        config = _parse_config(d.pop("config"))

        revision = d.pop("revision", UNSET)

        privacy_producer = cls(
            type_=type_,
            config=config,
            revision=revision,
        )

        return privacy_producer
