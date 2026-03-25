from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_decision import AgentDecision
    from ..models.generator_config import GeneratorConfig
    from ..models.query_builder_request_example_documents_item import QueryBuilderRequestExampleDocumentsItem


T = TypeVar("T", bound="QueryBuilderRequest")


@_attrs_define
class QueryBuilderRequest:
    """
    Attributes:
        intent (str): Natural language description of the search intent Example: Find all published articles about
            machine learning from the last year.
        session_id (Union[Unset, str]): Correlation identifier for a bounded agent interaction. In Phase 1 this is
            echoed back to the client but does not imply server-side session persistence.
        decisions (Union[Unset, list['AgentDecision']]): Structured answers provided by the user as part of client-
            carried continuation.
        interactive (Union[Unset, bool]): If true, the agent may return clarification questions when needed. Default:
            True.
        max_internal_iterations (Union[Unset, int]): Additive bounded-agent field for the query builder. Phase 1 remains
            a single-pass generation flow, but this field is echoed in result accounting.
        max_user_clarifications (Union[Unset, int]): Maximum number of clarification turns the agent may request from
            the user.
        require_decision_after (Union[Unset, int]): Force a user-facing decision after this many unresolved internal
            passes.
        example_documents (Union[Unset, list['QueryBuilderRequestExampleDocumentsItem']]): Optional example documents to
            help the query builder infer field shapes and representative values. When omitted and the table has data but no
            schema, the server samples up to one document automatically.
        table (Union[Unset, str]): Name of the table to build query for. If provided, uses table schema for field
            context. Example: articles.
        schema_fields (Union[Unset, list[str]]): List of searchable field names to consider. Overrides table schema if
            provided. Example: ['title', 'content', 'status', 'published_at'].
        generator (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
    """

    intent: str
    session_id: Union[Unset, str] = UNSET
    decisions: Union[Unset, list["AgentDecision"]] = UNSET
    interactive: Union[Unset, bool] = True
    max_internal_iterations: Union[Unset, int] = UNSET
    max_user_clarifications: Union[Unset, int] = UNSET
    require_decision_after: Union[Unset, int] = UNSET
    example_documents: Union[Unset, list["QueryBuilderRequestExampleDocumentsItem"]] = UNSET
    table: Union[Unset, str] = UNSET
    schema_fields: Union[Unset, list[str]] = UNSET
    generator: Union[Unset, "GeneratorConfig"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        intent = self.intent

        session_id = self.session_id

        decisions: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.decisions, Unset):
            decisions = []
            for decisions_item_data in self.decisions:
                decisions_item = decisions_item_data.to_dict()
                decisions.append(decisions_item)

        interactive = self.interactive

        max_internal_iterations = self.max_internal_iterations

        max_user_clarifications = self.max_user_clarifications

        require_decision_after = self.require_decision_after

        example_documents: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.example_documents, Unset):
            example_documents = []
            for example_documents_item_data in self.example_documents:
                example_documents_item = example_documents_item_data.to_dict()
                example_documents.append(example_documents_item)

        table = self.table

        schema_fields: Union[Unset, list[str]] = UNSET
        if not isinstance(self.schema_fields, Unset):
            schema_fields = self.schema_fields

        generator: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "intent": intent,
            }
        )
        if session_id is not UNSET:
            field_dict["session_id"] = session_id
        if decisions is not UNSET:
            field_dict["decisions"] = decisions
        if interactive is not UNSET:
            field_dict["interactive"] = interactive
        if max_internal_iterations is not UNSET:
            field_dict["max_internal_iterations"] = max_internal_iterations
        if max_user_clarifications is not UNSET:
            field_dict["max_user_clarifications"] = max_user_clarifications
        if require_decision_after is not UNSET:
            field_dict["require_decision_after"] = require_decision_after
        if example_documents is not UNSET:
            field_dict["example_documents"] = example_documents
        if table is not UNSET:
            field_dict["table"] = table
        if schema_fields is not UNSET:
            field_dict["schema_fields"] = schema_fields
        if generator is not UNSET:
            field_dict["generator"] = generator

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_decision import AgentDecision
        from ..models.generator_config import GeneratorConfig
        from ..models.query_builder_request_example_documents_item import QueryBuilderRequestExampleDocumentsItem

        d = dict(src_dict)
        intent = d.pop("intent")

        session_id = d.pop("session_id", UNSET)

        decisions = []
        _decisions = d.pop("decisions", UNSET)
        for decisions_item_data in _decisions or []:
            decisions_item = AgentDecision.from_dict(decisions_item_data)

            decisions.append(decisions_item)

        interactive = d.pop("interactive", UNSET)

        max_internal_iterations = d.pop("max_internal_iterations", UNSET)

        max_user_clarifications = d.pop("max_user_clarifications", UNSET)

        require_decision_after = d.pop("require_decision_after", UNSET)

        example_documents = []
        _example_documents = d.pop("example_documents", UNSET)
        for example_documents_item_data in _example_documents or []:
            example_documents_item = QueryBuilderRequestExampleDocumentsItem.from_dict(example_documents_item_data)

            example_documents.append(example_documents_item)

        table = d.pop("table", UNSET)

        schema_fields = cast(list[str], d.pop("schema_fields", UNSET))

        _generator = d.pop("generator", UNSET)
        generator: Union[Unset, GeneratorConfig]
        if isinstance(_generator, Unset):
            generator = UNSET
        else:
            generator = GeneratorConfig.from_dict(_generator)

        query_builder_request = cls(
            intent=intent,
            session_id=session_id,
            decisions=decisions,
            interactive=interactive,
            max_internal_iterations=max_internal_iterations,
            max_user_clarifications=max_user_clarifications,
            require_decision_after=require_decision_after,
            example_documents=example_documents,
            table=table,
            schema_fields=schema_fields,
            generator=generator,
        )

        query_builder_request.additional_properties = d
        return query_builder_request

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
