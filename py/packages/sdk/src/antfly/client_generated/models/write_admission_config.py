from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="WriteAdmissionConfig")


@_attrs_define
class WriteAdmissionConfig:
    """
    Attributes:
        max_concurrent_requests (int | Unset): Maximum concurrent foreground data mutations in this process. The
            default is 16. Table batch writes, cross-table batches, linear merges,
            and transaction commits consume this budget. Schema, index, backup,
            restore, repair, and other administrative operations use dedicated
            control or background-maintenance paths. Set to 0 to disable write
            admission.

            Excess HTTP work in either foreground class is rejected immediately
            with HTTP 429 and Retry-After: 1; asynchronous protocols use their
            native failure response. Excess work is not queued. Both budgets are
            independent of transport safeguards and
            resources.inference.admission.max_concurrent_requests.
             Default: 16. Example: 16.
    """

    max_concurrent_requests: int | Unset = 16

    def to_dict(self) -> dict[str, Any]:
        max_concurrent_requests = self.max_concurrent_requests

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_concurrent_requests is not UNSET:
            field_dict["max_concurrent_requests"] = max_concurrent_requests

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_concurrent_requests = d.pop("max_concurrent_requests", UNSET)

        write_admission_config = cls(
            max_concurrent_requests=max_concurrent_requests,
        )

        return write_admission_config
