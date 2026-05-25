from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_decision import AgentDecision
    from ..models.generator_config import GeneratorConfig
    from ..models.query_builder_request_constraints import QueryBuilderRequestConstraints
    from ..models.query_builder_request_example_documents_item import QueryBuilderRequestExampleDocumentsItem


T = TypeVar("T", bound="QueryBuilderRequest")


@_attrs_define
class QueryBuilderRequest:
    """
    Attributes:
        intent (str): Natural language description of the search intent Example: Find all published articles about
            machine learning from the last year.
        session_id (str | Unset): Correlation identifier for a bounded agent interaction. In Phase 1 this is echoed back
            to the client but does not imply server-side session persistence.
        decisions (list[AgentDecision] | Unset): Structured answers provided by the user as part of client-carried
            continuation.
        interactive (bool | Unset): If true, the agent may return clarification questions when needed. Default: True.
        max_internal_iterations (int | Unset): Additive bounded-agent field for the query builder. Phase 1 remains a
            single-pass generation flow, but this field is echoed in result accounting.
        max_user_clarifications (int | Unset): Maximum number of clarification turns the agent may request from the
            user.
        require_decision_after (int | Unset): Force a user-facing decision after this many unresolved internal passes.
        example_documents (list[QueryBuilderRequestExampleDocumentsItem] | Unset): Optional example documents to help
            the query builder infer field shapes and representative values. When omitted and the table has data but no
            schema, the server samples up to one document automatically.
        table (str | Unset): Name of the table to build query for. If provided, uses table schema for field context.
            Example: articles.
        schema_fields (list[str] | Unset): List of searchable field names to consider. Overrides table schema if
            provided. Example: ['title', 'content', 'status', 'published_at'].
        mode (str | Unset): Optional strategy hint for the coordinator. Suggested values are `auto`, `full_text`,
            `semantic`, `hybrid`, `filter`, `tree`, and `graph`. Unknown values are accepted for
            forward compatibility and may fall back to `auto`.
             Example: auto.
        output (str | Unset): Preferred output artifact. Suggested values are `query_request`, `bleve`, and
            `filter_query`. The compatibility `query` field is still returned for existing clients.
             Example: query_request.
        constraints (QueryBuilderRequestConstraints | Unset): Optional execution constraints for the coordinator, such
            as `limit`, `allowed_fields`,
            `prefer_indexes`, and `require_executable`.
             Example: {'limit': 10, 'require_executable': True, 'prefer_indexes': ['body_embedding']}.
        generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
    """

    intent: str
    session_id: str | Unset = UNSET
    decisions: list[AgentDecision] | Unset = UNSET
    interactive: bool | Unset = True
    max_internal_iterations: int | Unset = UNSET
    max_user_clarifications: int | Unset = UNSET
    require_decision_after: int | Unset = UNSET
    example_documents: list[QueryBuilderRequestExampleDocumentsItem] | Unset = UNSET
    table: str | Unset = UNSET
    schema_fields: list[str] | Unset = UNSET
    mode: str | Unset = UNSET
    output: str | Unset = UNSET
    constraints: QueryBuilderRequestConstraints | Unset = UNSET
    generator: GeneratorConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        intent = self.intent

        session_id = self.session_id

        decisions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.decisions, Unset):
            decisions = []
            for decisions_item_data in self.decisions:
                decisions_item = decisions_item_data.to_dict()
                decisions.append(decisions_item)

        interactive = self.interactive

        max_internal_iterations = self.max_internal_iterations

        max_user_clarifications = self.max_user_clarifications

        require_decision_after = self.require_decision_after

        example_documents: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.example_documents, Unset):
            example_documents = []
            for example_documents_item_data in self.example_documents:
                example_documents_item = example_documents_item_data.to_dict()
                example_documents.append(example_documents_item)

        table = self.table

        schema_fields: list[str] | Unset = UNSET
        if not isinstance(self.schema_fields, Unset):
            schema_fields = self.schema_fields

        mode = self.mode

        output = self.output

        constraints: dict[str, Any] | Unset = UNSET
        if not isinstance(self.constraints, Unset):
            constraints = self.constraints.to_dict()

        generator: dict[str, Any] | Unset = UNSET
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
        if mode is not UNSET:
            field_dict["mode"] = mode
        if output is not UNSET:
            field_dict["output"] = output
        if constraints is not UNSET:
            field_dict["constraints"] = constraints
        if generator is not UNSET:
            field_dict["generator"] = generator

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_decision import AgentDecision
        from ..models.generator_config import GeneratorConfig
        from ..models.query_builder_request_constraints import QueryBuilderRequestConstraints
        from ..models.query_builder_request_example_documents_item import QueryBuilderRequestExampleDocumentsItem

        d = dict(src_dict)
        intent = d.pop("intent")

        session_id = d.pop("session_id", UNSET)

        _decisions = d.pop("decisions", UNSET)
        decisions: list[AgentDecision] | Unset = UNSET
        if _decisions is not UNSET:
            decisions = []
            for decisions_item_data in _decisions:
                decisions_item = AgentDecision.from_dict(decisions_item_data)

                decisions.append(decisions_item)

        interactive = d.pop("interactive", UNSET)

        max_internal_iterations = d.pop("max_internal_iterations", UNSET)

        max_user_clarifications = d.pop("max_user_clarifications", UNSET)

        require_decision_after = d.pop("require_decision_after", UNSET)

        _example_documents = d.pop("example_documents", UNSET)
        example_documents: list[QueryBuilderRequestExampleDocumentsItem] | Unset = UNSET
        if _example_documents is not UNSET:
            example_documents = []
            for example_documents_item_data in _example_documents:
                example_documents_item = QueryBuilderRequestExampleDocumentsItem.from_dict(example_documents_item_data)

                example_documents.append(example_documents_item)

        table = d.pop("table", UNSET)

        schema_fields = cast(list[str], d.pop("schema_fields", UNSET))

        mode = d.pop("mode", UNSET)

        output = d.pop("output", UNSET)

        _constraints = d.pop("constraints", UNSET)
        constraints: QueryBuilderRequestConstraints | Unset
        if isinstance(_constraints, Unset):
            constraints = UNSET
        else:
            constraints = QueryBuilderRequestConstraints.from_dict(_constraints)

        _generator = d.pop("generator", UNSET)
        generator: GeneratorConfig | Unset
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
            mode=mode,
            output=output,
            constraints=constraints,
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
