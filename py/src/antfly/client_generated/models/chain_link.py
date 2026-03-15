from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chain_condition import ChainCondition
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.generator_config import GeneratorConfig
    from ..models.retry_config import RetryConfig


T = TypeVar("T", bound="ChainLink")


@_attrs_define
class ChainLink:
    r"""A single link in a generator chain with optional retry and condition

    Attributes:
        generator (GeneratorConfig): A unified configuration for a generative AI provider.

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
        retry (Union[Unset, RetryConfig]): Retry configuration for generator calls
        condition (Union[Unset, ChainCondition]): Condition for trying the next generator in chain:
            - always: Always try next regardless of outcome
            - on_error: Try next on any error (default)
            - on_timeout: Try next only on timeout errors
            - on_rate_limit: Try next only on rate limit errors
    """

    generator: "GeneratorConfig"
    retry: Union[Unset, "RetryConfig"] = UNSET
    condition: Union[Unset, ChainCondition] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        generator = self.generator.to_dict()

        retry: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.retry, Unset):
            retry = self.retry.to_dict()

        condition: Union[Unset, str] = UNSET
        if not isinstance(self.condition, Unset):
            condition = self.condition.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "generator": generator,
            }
        )
        if retry is not UNSET:
            field_dict["retry"] = retry
        if condition is not UNSET:
            field_dict["condition"] = condition

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.generator_config import GeneratorConfig
        from ..models.retry_config import RetryConfig

        d = dict(src_dict)
        generator = GeneratorConfig.from_dict(d.pop("generator"))

        _retry = d.pop("retry", UNSET)
        retry: Union[Unset, RetryConfig]
        if isinstance(_retry, Unset):
            retry = UNSET
        else:
            retry = RetryConfig.from_dict(_retry)

        _condition = d.pop("condition", UNSET)
        condition: Union[Unset, ChainCondition]
        if isinstance(_condition, Unset):
            condition = UNSET
        else:
            condition = ChainCondition(_condition)

        chain_link = cls(
            generator=generator,
            retry=retry,
            condition=condition,
        )

        chain_link.additional_properties = d
        return chain_link

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
