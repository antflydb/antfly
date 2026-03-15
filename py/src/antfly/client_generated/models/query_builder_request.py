from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="QueryBuilderRequest")


@_attrs_define
class QueryBuilderRequest:
    r"""
    Attributes:
        intent (str): Natural language description of the search intent Example: Find all published articles about
            machine learning from the last year.
        table (Union[Unset, str]): Name of the table to build query for. If provided, uses table schema for field
            context. Example: articles.
        schema_fields (Union[Unset, list[str]]): List of searchable field names to consider. Overrides table schema if
            provided. Example: ['title', 'content', 'status', 'published_at'].
        generator (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.

            Generators can be configured with custom prompts using templates. Templates use
            Handlebars syntax and support various built-in helpers for formatting and data manipulation.

            **Template System:**
            - **Syntax**: Handlebars templating (https://handlebarsjs.com/guide/)
            - **Caching**: Templates are automatically cached with configurable TTL (default: 5 minutes)
            - **Context**: Templates receive the full context data passed to the generator

            **Built-in Helpers:**

            1. **scrubHtml** - Remove script/style tags and extract clean text from HTML
               ```handlebars
               {{scrubHtml html_content}}
               ```
               - Removes `<script>` and `<style>` tags
               - Adds newlines after block elements (p, div, h1-h6, li, etc.)
               - Returns plain text with preserved readability
               - Useful for cleaning web content before summarization

            2. **eq** - Equality comparison for conditionals
               ```handlebars
               {{#if (eq status "active")}}Active{{/if}}
               {{#if (eq @key "special")}}Special field{{/if}}
               ```
               - Use in `{{#if}}` blocks for conditional logic
               - Compares any two values for equality

            3. **media** - GenKit dotprompt media directive for multimodal content
               ```handlebars
               {{media url=imageDataURI}}
               {{media url=this.image_url}}
               {{media url="https://example.com/image.jpg"}}
               {{media url="s3://endpoint/bucket/image.png"}}
               {{media url="file:///path/to/image.jpg"}}
               ```

               **Supported URL Schemes:**
               - `data:` - Base64 encoded data URIs (e.g., `data:image/jpeg;base64,...`)
               - `http://` / `https://` - Web URLs with automatic content type detection
               - `file://` - Local filesystem paths
               - `s3://` - S3-compatible storage (format: `s3://endpoint/bucket/key`)

               **Automatic Content Processing:**
               - **Images**: Downloaded, resized (if needed), converted to data URIs
               - **PDFs**: Text extracted or first page rendered as image
               - **HTML**: Readable text extracted using Mozilla Readability

               **Security Controls:**
               Downloads are protected by content security settings (see Configuration Reference):
               - Allowed host whitelist
               - Private IP blocking (prevents SSRF attacks)
               - Download size limits (default: 100MB)
               - Download timeouts (default: 30s)
               - Image dimension limits (default: 2048px, auto-resized)

               See: https://antfly.io/docs/configuration#security--cors

            4. **encodeToon** - Encode data in TOON format (Token-Oriented Object Notation)
               ```handlebars
               {{encodeToon this.fields}}
               {{encodeToon this.fields lengthMarker=false indent=4}}
               {{encodeToon this.fields delimiter="\t"}}
               ```

               **What is TOON?**
               TOON is a compact, human-readable format designed for passing structured data to LLMs.
               It provides **30-60% token reduction** compared to JSON while maintaining high LLM
               comprehension accuracy.

               **Key Features:**
               - Compact syntax using `:` for key-value pairs
               - Array length markers: `tags[#3]: ai,search,ml`
               - Tabular format for uniform data structures
               - Optimized for LLM parsing and understanding
               - Maintains human readability

               **Benefits:**
               - **Lower API costs** - Reduced token usage means lower LLM API costs
               - **Faster responses** - Less tokens to process
               - **More context** - Fit more documents within token limits

               **Options:**
               - `lengthMarker` (bool): Add # prefix to array counts like `[#3]` (default: true)
               - `indent` (int): Indentation spacing for nested objects (default: 2)
               - `delimiter` (string): Field separator for tabular arrays (default: none, use `"\t"` for tabs)

               **Example output:**
               ```
               title: Introduction to Vector Search
               author: Jane Doe
               tags[#3]: ai,search,ml
               metadata:
                 edition: 2
                 pages: 450
               ```

               **Default in RAG:** TOON is the default format for document rendering in RAG queries.

               **References:**
               - TOON Specification: https://github.com/toon-format/toon
               - Go Implementation: https://github.com/alpkeskin/gotoon

            **Template Examples:**

            RAG summarization with document references:
            ```handlebars
            Based on these documents, provide a comprehensive summary:

            {{#each documents}}
            Document {{this.id}}:
            {{scrubHtml this.content}}

            {{/each}}

            Valid document IDs: {{#each documents}}{{this.id}}{{#unless @last}}, {{/unless}}{{/each}}
            ```

            Conditional formatting:
            ```handlebars
            {{#if system_prompt}}System: {{system_prompt}}{{/if}}

            User Query: {{query}}

            {{#if context}}
            Context:
            {{#each context}}
            - {{this}}
            {{/each}}
            {{/if}}
            ```

            Multimodal prompt with images:
            ```handlebars
            Analyze this image:
            {{media url=image_url}}

            Focus on: {{focus_area}}
            ```

            Structured data encoding:
            ```handlebars
            User Profile:
            {{encodeToon user_data indent=2 lengthMarker=true}}

            Please analyze this profile.
            ```

            **Common Use Cases:**
            - **RAG (Retrieval-Augmented Generation)**: Format retrieved documents with citations
            - **Summarization**: Clean HTML content and structure summaries
            - **Query Classification**: Format queries with metadata for better classification
            - **Multimodal**: Include images/audio/video in prompts
            - **Data Formatting**: Convert structured data to readable text

            **Best Practices:**
            - Keep templates simple - complex logic belongs in application code
            - Use clear, descriptive field names in context
            - Handle missing fields gracefully (templates use "missingkey=zero" by default)
            - Test templates with representative data before production use Example: {'provider': 'openai', 'model':
            'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
    """

    intent: str
    table: Union[Unset, str] = UNSET
    schema_fields: Union[Unset, list[str]] = UNSET
    generator: Union[Unset, "GeneratorConfig"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        intent = self.intent

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
        if table is not UNSET:
            field_dict["table"] = table
        if schema_fields is not UNSET:
            field_dict["schema_fields"] = schema_fields
        if generator is not UNSET:
            field_dict["generator"] = generator

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        intent = d.pop("intent")

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
