from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_synthesis_request_format import InferenceSynthesisRequestFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceSynthesisRequest")


@_attrs_define
class InferenceSynthesisRequest:
    """
    Attributes:
        model (str): Canonical synthesizers/name target or qualified connection model.
        text (str):
        voice (str | Unset):
        format_ (InferenceSynthesisRequestFormat | Unset):
        speed (float | Unset):
    """

    model: str
    text: str
    voice: str | Unset = UNSET
    format_: InferenceSynthesisRequestFormat | Unset = UNSET
    speed: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        text = self.text

        voice = self.voice

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        speed = self.speed

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "text": text,
            }
        )
        if voice is not UNSET:
            field_dict["voice"] = voice
        if format_ is not UNSET:
            field_dict["format"] = format_
        if speed is not UNSET:
            field_dict["speed"] = speed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        text = d.pop("text")

        voice = d.pop("voice", UNSET)

        _format_ = d.pop("format", UNSET)
        format_: InferenceSynthesisRequestFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = InferenceSynthesisRequestFormat(_format_)

        speed = d.pop("speed", UNSET)

        inference_synthesis_request = cls(
            model=model,
            text=text,
            voice=voice,
            format_=format_,
            speed=speed,
        )

        return inference_synthesis_request
