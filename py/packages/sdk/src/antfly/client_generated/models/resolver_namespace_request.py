from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_redirect import ResolverRedirect
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="ResolverNamespaceRequest")


@_attrs_define
class ResolverNamespaceRequest:
    """Explicit namespace migration. Choose reset_merges=true or a complete key_map. Runtime suppressions and distinct pins
    must be remapped too, or explicitly reset with reset_curation=true.

        Attributes:
            table (str):
            key_template (str):
            reset_merges (bool | Unset):  Default: False.
            reset_curation (bool | Unset):  Default: False.
            key_map (list[ResolverRedirect] | Unset):
            aliases (list[ResolverRedirect] | Unset):
            expected_revision (int | Unset): Optional revision fence. Explicit zero fences the first edit; omission disables
                the fence.
            wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
                state with applied=false; it does not undo the mutation.
    """

    table: str
    key_template: str
    reset_merges: bool | Unset = False
    reset_curation: bool | Unset = False
    key_map: list[ResolverRedirect] | Unset = UNSET
    aliases: list[ResolverRedirect] | Unset = UNSET
    expected_revision: int | Unset = UNSET
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key_template = self.key_template

        reset_merges = self.reset_merges

        reset_curation = self.reset_curation

        key_map: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.key_map, Unset):
            key_map = []
            for key_map_item_data in self.key_map:
                key_map_item = key_map_item_data.to_dict()
                key_map.append(key_map_item)

        aliases: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.aliases, Unset):
            aliases = []
            for aliases_item_data in self.aliases:
                aliases_item = aliases_item_data.to_dict()
                aliases.append(aliases_item)

        expected_revision = self.expected_revision

        wait: dict[str, Any] | Unset = UNSET
        if not isinstance(self.wait, Unset):
            wait = self.wait.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table": table,
                "key_template": key_template,
            }
        )
        if reset_merges is not UNSET:
            field_dict["reset_merges"] = reset_merges
        if reset_curation is not UNSET:
            field_dict["reset_curation"] = reset_curation
        if key_map is not UNSET:
            field_dict["key_map"] = key_map
        if aliases is not UNSET:
            field_dict["aliases"] = aliases
        if expected_revision is not UNSET:
            field_dict["expected_revision"] = expected_revision
        if wait is not UNSET:
            field_dict["wait"] = wait

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_redirect import ResolverRedirect
        from ..models.resolver_wait import ResolverWait

        d = dict(src_dict)
        table = d.pop("table")

        key_template = d.pop("key_template")

        reset_merges = d.pop("reset_merges", UNSET)

        reset_curation = d.pop("reset_curation", UNSET)

        _key_map = d.pop("key_map", UNSET)
        key_map: list[ResolverRedirect] | Unset = UNSET
        if _key_map is not UNSET:
            key_map = []
            for key_map_item_data in _key_map:
                key_map_item = ResolverRedirect.from_dict(key_map_item_data)

                key_map.append(key_map_item)

        _aliases = d.pop("aliases", UNSET)
        aliases: list[ResolverRedirect] | Unset = UNSET
        if _aliases is not UNSET:
            aliases = []
            for aliases_item_data in _aliases:
                aliases_item = ResolverRedirect.from_dict(aliases_item_data)

                aliases.append(aliases_item)

        expected_revision = d.pop("expected_revision", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        resolver_namespace_request = cls(
            table=table,
            key_template=key_template,
            reset_merges=reset_merges,
            reset_curation=reset_curation,
            key_map=key_map,
            aliases=aliases,
            expected_revision=expected_revision,
            wait=wait,
        )

        return resolver_namespace_request
