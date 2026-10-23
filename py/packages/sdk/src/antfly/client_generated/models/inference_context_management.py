from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_context_management_mode import InferenceContextManagementMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_context_checkpoint import InferenceContextCheckpoint


T = TypeVar("T", bound="InferenceContextManagement")


@_attrs_define
class InferenceContextManagement:
    """
    Attributes:
        mode (InferenceContextManagementMode): `reject` fails when the rendered input exceeds the model budget. `window`
            permits deterministic removal of older turns. Omission preserves the
            server's existing reject-on-overflow behavior.
        checkpoint (InferenceContextCheckpoint | Unset):
        max_input_tokens (int | Unset): Optional client ceiling below the model's effective input limit.
        keep_recent_turns (int | Unset): Minimum number of most recent complete turns retained by windowing. Default: 2.
    """

    mode: InferenceContextManagementMode
    checkpoint: InferenceContextCheckpoint | Unset = UNSET
    max_input_tokens: int | Unset = UNSET
    keep_recent_turns: int | Unset = 2

    def to_dict(self) -> dict[str, Any]:
        mode = self.mode.value

        checkpoint: dict[str, Any] | Unset = UNSET
        if not isinstance(self.checkpoint, Unset):
            checkpoint = self.checkpoint.to_dict()

        max_input_tokens = self.max_input_tokens

        keep_recent_turns = self.keep_recent_turns

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "mode": mode,
            }
        )
        if checkpoint is not UNSET:
            field_dict["checkpoint"] = checkpoint
        if max_input_tokens is not UNSET:
            field_dict["max_input_tokens"] = max_input_tokens
        if keep_recent_turns is not UNSET:
            field_dict["keep_recent_turns"] = keep_recent_turns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_context_checkpoint import InferenceContextCheckpoint

        d = dict(src_dict)
        mode = InferenceContextManagementMode(d.pop("mode"))

        _checkpoint = d.pop("checkpoint", UNSET)
        checkpoint: InferenceContextCheckpoint | Unset
        if isinstance(_checkpoint, Unset):
            checkpoint = UNSET
        else:
            checkpoint = InferenceContextCheckpoint.from_dict(_checkpoint)

        max_input_tokens = d.pop("max_input_tokens", UNSET)

        keep_recent_turns = d.pop("keep_recent_turns", UNSET)

        inference_context_management = cls(
            mode=mode,
            checkpoint=checkpoint,
            max_input_tokens=max_input_tokens,
            keep_recent_turns=keep_recent_turns,
        )

        return inference_context_management
