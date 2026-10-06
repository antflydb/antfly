"""Bounded validation for exact-proof shared index maintenance operations."""

import re
from typing import Any

from .client_generated.models import IndexMaintenanceOwnerProof, IndexMaintenanceRequest, IndexMaintenanceResponse
from .exceptions import AntflyException


def _decimal(value: Any, positive: bool) -> bool:
    return (
        isinstance(value, str)
        and re.fullmatch(r"0|[1-9][0-9]{0,19}", value) is not None
        and int(value) <= 2**64 - 1
        and (not positive or value != "0")
    )


def _digest(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def validate_request(request: IndexMaintenanceRequest) -> set[str]:
    if (
        not isinstance(request, IndexMaintenanceRequest)
        or not _decimal(request.table_id, True)
        or type(request.schema_version) is not int
        or not 0 <= request.schema_version <= 2**32 - 1
        or not isinstance(request.owners, list)
        or not 1 <= len(request.owners) <= 128
    ):
        raise ValueError("index maintenance requires a valid table/schema identity and 1..128 owner proofs")
    groups: set[str] = set()
    for owner in request.owners:
        if (
            not isinstance(owner, IndexMaintenanceOwnerProof)
            or not _decimal(owner.group_id, True)
            or not _decimal(owner.generation, True)
            or not _decimal(owner.maintenance_epoch, False)
            or type(owner.slot) is not int
            or not 0 <= owner.slot <= 2**32 - 1
            or not _digest(owner.owner)
            or not _digest(owner.comparison)
            or not _digest(owner.progress_digest)
            or owner.group_id in groups
        ):
            raise ValueError("index maintenance has an invalid or duplicate owner proof")
        groups.add(owner.group_id)
    return groups


def validate_response(value: Any, groups: set[str]) -> IndexMaintenanceResponse:
    if not isinstance(value, dict) or not isinstance(value.get("acknowledged_groups"), list):
        raise AntflyException("invalid index maintenance acknowledgement; resubmit identical proofs")
    acknowledged = value["acknowledged_groups"]
    if (
        len(acknowledged) != len(groups)
        or any(not isinstance(group, str) for group in acknowledged)
        or len(set(acknowledged)) != len(acknowledged)
        or set(acknowledged) != groups
    ):
        raise AntflyException("invalid or incomplete index maintenance acknowledgement set; resubmit identical proofs")
    return IndexMaintenanceResponse.from_dict(value)
