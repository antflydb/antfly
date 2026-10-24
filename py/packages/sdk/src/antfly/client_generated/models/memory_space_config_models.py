from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_space_config_models_decider_type_0 import MemorySpaceConfigModelsDeciderType0
    from ..models.memory_space_config_models_embedder_type_0 import MemorySpaceConfigModelsEmbedderType0
    from ..models.memory_space_config_models_extractor_type_0 import MemorySpaceConfigModelsExtractorType0
    from ..models.memory_space_config_models_generator_type_0 import MemorySpaceConfigModelsGeneratorType0


T = TypeVar("T", bound="MemorySpaceConfigModels")


@_attrs_define
class MemorySpaceConfigModels:
    """
    Attributes:
        embedder (MemorySpaceConfigModelsEmbedderType0 | None | Unset):
        extractor (MemorySpaceConfigModelsExtractorType0 | None | Unset):
        generator (MemorySpaceConfigModelsGeneratorType0 | None | Unset):
        decider (MemorySpaceConfigModelsDeciderType0 | None | Unset):
    """

    embedder: MemorySpaceConfigModelsEmbedderType0 | None | Unset = UNSET
    extractor: MemorySpaceConfigModelsExtractorType0 | None | Unset = UNSET
    generator: MemorySpaceConfigModelsGeneratorType0 | None | Unset = UNSET
    decider: MemorySpaceConfigModelsDeciderType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_space_config_models_decider_type_0 import MemorySpaceConfigModelsDeciderType0
        from ..models.memory_space_config_models_embedder_type_0 import MemorySpaceConfigModelsEmbedderType0
        from ..models.memory_space_config_models_extractor_type_0 import MemorySpaceConfigModelsExtractorType0
        from ..models.memory_space_config_models_generator_type_0 import MemorySpaceConfigModelsGeneratorType0

        embedder: dict[str, Any] | None | Unset
        if isinstance(self.embedder, Unset):
            embedder = UNSET
        elif isinstance(self.embedder, MemorySpaceConfigModelsEmbedderType0):
            embedder = self.embedder.to_dict()
        else:
            embedder = self.embedder

        extractor: dict[str, Any] | None | Unset
        if isinstance(self.extractor, Unset):
            extractor = UNSET
        elif isinstance(self.extractor, MemorySpaceConfigModelsExtractorType0):
            extractor = self.extractor.to_dict()
        else:
            extractor = self.extractor

        generator: dict[str, Any] | None | Unset
        if isinstance(self.generator, Unset):
            generator = UNSET
        elif isinstance(self.generator, MemorySpaceConfigModelsGeneratorType0):
            generator = self.generator.to_dict()
        else:
            generator = self.generator

        decider: dict[str, Any] | None | Unset
        if isinstance(self.decider, Unset):
            decider = UNSET
        elif isinstance(self.decider, MemorySpaceConfigModelsDeciderType0):
            decider = self.decider.to_dict()
        else:
            decider = self.decider

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if embedder is not UNSET:
            field_dict["embedder"] = embedder
        if extractor is not UNSET:
            field_dict["extractor"] = extractor
        if generator is not UNSET:
            field_dict["generator"] = generator
        if decider is not UNSET:
            field_dict["decider"] = decider

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_space_config_models_decider_type_0 import MemorySpaceConfigModelsDeciderType0
        from ..models.memory_space_config_models_embedder_type_0 import MemorySpaceConfigModelsEmbedderType0
        from ..models.memory_space_config_models_extractor_type_0 import MemorySpaceConfigModelsExtractorType0
        from ..models.memory_space_config_models_generator_type_0 import MemorySpaceConfigModelsGeneratorType0

        d = dict(src_dict)

        def _parse_embedder(data: object) -> MemorySpaceConfigModelsEmbedderType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                embedder_type_0 = MemorySpaceConfigModelsEmbedderType0.from_dict(data)

                return embedder_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemorySpaceConfigModelsEmbedderType0 | None | Unset, data)

        embedder = _parse_embedder(d.pop("embedder", UNSET))

        def _parse_extractor(data: object) -> MemorySpaceConfigModelsExtractorType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                extractor_type_0 = MemorySpaceConfigModelsExtractorType0.from_dict(data)

                return extractor_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemorySpaceConfigModelsExtractorType0 | None | Unset, data)

        extractor = _parse_extractor(d.pop("extractor", UNSET))

        def _parse_generator(data: object) -> MemorySpaceConfigModelsGeneratorType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                generator_type_0 = MemorySpaceConfigModelsGeneratorType0.from_dict(data)

                return generator_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemorySpaceConfigModelsGeneratorType0 | None | Unset, data)

        generator = _parse_generator(d.pop("generator", UNSET))

        def _parse_decider(data: object) -> MemorySpaceConfigModelsDeciderType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                decider_type_0 = MemorySpaceConfigModelsDeciderType0.from_dict(data)

                return decider_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemorySpaceConfigModelsDeciderType0 | None | Unset, data)

        decider = _parse_decider(d.pop("decider", UNSET))

        memory_space_config_models = cls(
            embedder=embedder,
            extractor=extractor,
            generator=generator,
            decider=decider,
        )

        return memory_space_config_models
