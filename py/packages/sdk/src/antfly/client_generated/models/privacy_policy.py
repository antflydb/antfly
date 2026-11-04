from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.privacy_policy_category_groups_item import PrivacyPolicyCategoryGroupsItem
from ..models.privacy_policy_mode import PrivacyPolicyMode
from ..models.privacy_policy_replacement import PrivacyPolicyReplacement
from ..models.privacy_policy_sensitivity import PrivacyPolicySensitivity
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.privacy_category import PrivacyCategory
    from ..models.privacy_policy_execution import PrivacyPolicyExecution
    from ..models.privacy_producer import PrivacyProducer


T = TypeVar("T", bound="PrivacyPolicy")


@_attrs_define
class PrivacyPolicy:
    """
    Attributes:
        mode (PrivacyPolicyMode): Unsupported modes fail with a typed 422 response; pre-storage protection is never
            substituted with asynchronous inspection.
        fields (list[str]): Literal source document field names, matching artifact source field semantics.
        category_groups (list[PrivacyPolicyCategoryGroupsItem] | Unset):
        categories (list[PrivacyCategory] | Unset): Add categories or override group categories. The union of explicit
            and expanded group categories must contain at most 128 distinct names.
        sensitivity (PrivacyPolicySensitivity | Unset):  Default: PrivacyPolicySensitivity.BALANCED.
        replacement (PrivacyPolicyReplacement | Unset):  Default: PrivacyPolicyReplacement.CATEGORY.
        producer (PrivacyProducer | Unset):
        execution (PrivacyPolicyExecution | Unset):
    """

    mode: PrivacyPolicyMode
    fields: list[str]
    category_groups: list[PrivacyPolicyCategoryGroupsItem] | Unset = UNSET
    categories: list[PrivacyCategory] | Unset = UNSET
    sensitivity: PrivacyPolicySensitivity | Unset = PrivacyPolicySensitivity.BALANCED
    replacement: PrivacyPolicyReplacement | Unset = PrivacyPolicyReplacement.CATEGORY
    producer: PrivacyProducer | Unset = UNSET
    execution: PrivacyPolicyExecution | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        mode = self.mode.value

        fields = self.fields

        category_groups: list[str] | Unset = UNSET
        if not isinstance(self.category_groups, Unset):
            category_groups = []
            for category_groups_item_data in self.category_groups:
                category_groups_item = category_groups_item_data.value
                category_groups.append(category_groups_item)

        categories: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.categories, Unset):
            categories = []
            for categories_item_data in self.categories:
                categories_item = categories_item_data.to_dict()
                categories.append(categories_item)

        sensitivity: str | Unset = UNSET
        if not isinstance(self.sensitivity, Unset):
            sensitivity = self.sensitivity.value

        replacement: str | Unset = UNSET
        if not isinstance(self.replacement, Unset):
            replacement = self.replacement.value

        producer: dict[str, Any] | Unset = UNSET
        if not isinstance(self.producer, Unset):
            producer = self.producer.to_dict()

        execution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.execution, Unset):
            execution = self.execution.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "mode": mode,
                "fields": fields,
            }
        )
        if category_groups is not UNSET:
            field_dict["category_groups"] = category_groups
        if categories is not UNSET:
            field_dict["categories"] = categories
        if sensitivity is not UNSET:
            field_dict["sensitivity"] = sensitivity
        if replacement is not UNSET:
            field_dict["replacement"] = replacement
        if producer is not UNSET:
            field_dict["producer"] = producer
        if execution is not UNSET:
            field_dict["execution"] = execution

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_category import PrivacyCategory
        from ..models.privacy_policy_execution import PrivacyPolicyExecution
        from ..models.privacy_producer import PrivacyProducer

        d = dict(src_dict)
        mode = PrivacyPolicyMode(d.pop("mode"))

        fields = cast(list[str], d.pop("fields"))

        _category_groups = d.pop("category_groups", UNSET)
        category_groups: list[PrivacyPolicyCategoryGroupsItem] | Unset = UNSET
        if _category_groups is not UNSET:
            category_groups = []
            for category_groups_item_data in _category_groups:
                category_groups_item = PrivacyPolicyCategoryGroupsItem(category_groups_item_data)

                category_groups.append(category_groups_item)

        _categories = d.pop("categories", UNSET)
        categories: list[PrivacyCategory] | Unset = UNSET
        if _categories is not UNSET:
            categories = []
            for categories_item_data in _categories:
                categories_item = PrivacyCategory.from_dict(categories_item_data)

                categories.append(categories_item)

        _sensitivity = d.pop("sensitivity", UNSET)
        sensitivity: PrivacyPolicySensitivity | Unset
        if isinstance(_sensitivity, Unset):
            sensitivity = UNSET
        else:
            sensitivity = PrivacyPolicySensitivity(_sensitivity)

        _replacement = d.pop("replacement", UNSET)
        replacement: PrivacyPolicyReplacement | Unset
        if isinstance(_replacement, Unset):
            replacement = UNSET
        else:
            replacement = PrivacyPolicyReplacement(_replacement)

        _producer = d.pop("producer", UNSET)
        producer: PrivacyProducer | Unset
        if isinstance(_producer, Unset):
            producer = UNSET
        else:
            producer = PrivacyProducer.from_dict(_producer)

        _execution = d.pop("execution", UNSET)
        execution: PrivacyPolicyExecution | Unset
        if isinstance(_execution, Unset):
            execution = UNSET
        else:
            execution = PrivacyPolicyExecution.from_dict(_execution)

        privacy_policy = cls(
            mode=mode,
            fields=fields,
            category_groups=category_groups,
            categories=categories,
            sensitivity=sensitivity,
            replacement=replacement,
            producer=producer,
            execution=execution,
        )

        return privacy_policy
