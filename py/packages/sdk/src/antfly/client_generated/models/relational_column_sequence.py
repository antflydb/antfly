from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_column_sequence_generation import RelationalColumnSequenceGeneration

T = TypeVar("T", bound="RelationalColumnSequence")


@_attrs_define
class RelationalColumnSequence:
    """Backend-bound owned-sequence default, assigned atomically with the table and stable attribute identity. Names are
    descriptive; allocation uses the exact catalog incarnation and native 64-bit identities. Requires reader capability
    37. Preparation allocates only for absent inputs; explicit NULL never requests a new value. Restore preserves stored
    values and never consumes the sequence. Clients cannot mint ownership by copying a descriptor onto a different table
    or column. Bulk mutation APIs preserve explicit identity values, like PostgreSQL COPY; SQL enforces ALWAYS and
    OVERRIDING rules. Implicit identity allocation uses table write authority; serial defaults and explicit sequence
    functions require sequence privileges.

        Attributes:
            generation (RelationalColumnSequenceGeneration): SQL owned-sequence default policy. Serial defaults preserve
                explicit values even under OVERRIDING USER VALUE; identity defaults obey PostgreSQL overriding rules.
            incarnation (str):
            sequence_id (str):
            namespace_id (str):
            table_id (str):
            column_id (str):
            database (str):
            namespace (str):
            name (str):
    """

    generation: RelationalColumnSequenceGeneration
    incarnation: str
    sequence_id: str
    namespace_id: str
    table_id: str
    column_id: str
    database: str
    namespace: str
    name: str

    def to_dict(self) -> dict[str, Any]:
        generation = self.generation.value

        incarnation = self.incarnation

        sequence_id = self.sequence_id

        namespace_id = self.namespace_id

        table_id = self.table_id

        column_id = self.column_id

        database = self.database

        namespace = self.namespace

        name = self.name

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "generation": generation,
                "incarnation": incarnation,
                "sequence_id": sequence_id,
                "namespace_id": namespace_id,
                "table_id": table_id,
                "column_id": column_id,
                "database": database,
                "namespace": namespace,
                "name": name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        generation = RelationalColumnSequenceGeneration(d.pop("generation"))

        incarnation = d.pop("incarnation")

        sequence_id = d.pop("sequence_id")

        namespace_id = d.pop("namespace_id")

        table_id = d.pop("table_id")

        column_id = d.pop("column_id")

        database = d.pop("database")

        namespace = d.pop("namespace")

        name = d.pop("name")

        relational_column_sequence = cls(
            generation=generation,
            incarnation=incarnation,
            sequence_id=sequence_id,
            namespace_id=namespace_id,
            table_id=table_id,
            column_id=column_id,
            database=database,
            namespace=namespace,
            name=name,
        )

        return relational_column_sequence
