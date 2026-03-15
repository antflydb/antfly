from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.edge_type_config import EdgeTypeConfig
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="GraphIndexConfig")


@_attrs_define
class GraphIndexConfig:
    r"""Configuration for graph index type

    Attributes:
        summarizer (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.

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
        template (Union[Unset, str]): Handlebars template for generating summarizer input text.
            Uses document fields as template variables.
            Same pattern as EmbeddingsConfig template.
             Example: {{title}}
            {{content}}.
        edge_types (Union[Unset, list['EdgeTypeConfig']]): List of edge types with their configurations
        max_edges_per_document (Union[Unset, int]): Maximum number of edges per document (0 = unlimited)
    """

    summarizer: Union[Unset, "GeneratorConfig"] = UNSET
    template: Union[Unset, str] = UNSET
    edge_types: Union[Unset, list["EdgeTypeConfig"]] = UNSET
    max_edges_per_document: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        summarizer: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.summarizer, Unset):
            summarizer = self.summarizer.to_dict()

        template = self.template

        edge_types: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = []
            for edge_types_item_data in self.edge_types:
                edge_types_item = edge_types_item_data.to_dict()
                edge_types.append(edge_types_item)

        max_edges_per_document = self.max_edges_per_document

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if summarizer is not UNSET:
            field_dict["summarizer"] = summarizer
        if template is not UNSET:
            field_dict["template"] = template
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if max_edges_per_document is not UNSET:
            field_dict["max_edges_per_document"] = max_edges_per_document

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge_type_config import EdgeTypeConfig
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        _summarizer = d.pop("summarizer", UNSET)
        summarizer: Union[Unset, GeneratorConfig]
        if isinstance(_summarizer, Unset):
            summarizer = UNSET
        else:
            summarizer = GeneratorConfig.from_dict(_summarizer)

        template = d.pop("template", UNSET)

        edge_types = []
        _edge_types = d.pop("edge_types", UNSET)
        for edge_types_item_data in _edge_types or []:
            edge_types_item = EdgeTypeConfig.from_dict(edge_types_item_data)

            edge_types.append(edge_types_item)

        max_edges_per_document = d.pop("max_edges_per_document", UNSET)

        graph_index_config = cls(
            summarizer=summarizer,
            template=template,
            edge_types=edge_types,
            max_edges_per_document=max_edges_per_document,
        )

        graph_index_config.additional_properties = d
        return graph_index_config

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
