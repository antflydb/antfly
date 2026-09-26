from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.text_analysis_config_analyzers import TextAnalysisConfigAnalyzers
    from ..models.text_analysis_config_char_filters import TextAnalysisConfigCharFilters
    from ..models.text_analysis_config_date_time_parsers import TextAnalysisConfigDateTimeParsers
    from ..models.text_analysis_config_field_analyzers import TextAnalysisConfigFieldAnalyzers
    from ..models.text_analysis_config_field_date_time_parsers import TextAnalysisConfigFieldDateTimeParsers
    from ..models.text_analysis_config_token_filters import TextAnalysisConfigTokenFilters
    from ..models.text_analysis_config_tokenizers import TextAnalysisConfigTokenizers


T = TypeVar("T", bound="TextAnalysisConfig")


@_attrs_define
class TextAnalysisConfig:
    """Custom text analysis for a full-text index. Component maps are keyed
    by the name that analyzers and `field_analyzers` reference. Built-in
    analyzers (`standard`, `simple`, `keyword`, `html`, `search_as_you_type`,
    `substring`, and the language analyzers such as `german`) are always
    available without declaring them.

    Example: split camelCase identifiers and match them as substrings.

    ```json
    {
      "analysis_config": {
        "field_analyzers": {"symbol": "code"},
        "token_filters": {
          "tails": {"type": "suffix", "config": {"min": 3, "max": 24}}
        },
        "analyzers": {
          "code": {
            "type": "custom",
            "config": {
              "tokenizer": "whitespace",
              "token_filters": ["camel_case", "unique", "tails"]
            }
          }
        }
      }
    }
    ```

        Attributes:
            field_analyzers (TextAnalysisConfigFieldAnalyzers | Unset): Map of indexed field name to analyzer name.
                Overrides the analyzer derived from the table schema for that field.
            char_filters (TextAnalysisConfigCharFilters | Unset): Named character filters. Types: `html_strip` (alias
                `html`), `ascii_fold`, `zero_width_non_joiner`.
            tokenizers (TextAnalysisConfigTokenizers | Unset): Named tokenizers. Types: `unicode` (alias `unicode_words`),
                `whitespace`, `keyword`, `character`, `ngram` (`config.min`, `config.max`), `edge_ngram` (`config.min`,
                `config.max`, `config.side` of `front` or `back`).
            token_filters (TextAnalysisConfigTokenFilters | Unset): Named token filters. Types: `lowercase` (alias
                `to_lower`), `stop_words` (alias `stop`; optional `config.language`), `stemmer` (optional `config.language`),
                `ngram` and `edge_ngram` (`config.min`, `config.max`), `shingle` (`config.min`, `config.max`, `config.separator`
                of `space` or `none`), `suffix` (`config.min`, `config.max`; emits every suffix of each token so prefix queries
                answer containment), `length` (`config.min`, `config.max`), `truncate` (`config.length`), `camel_case`,
                `unique`, `reverse`, `elision`, `apostrophe`. Languages: english, german, french, spanish, italian, portuguese,
                dutch, swedish, norwegian, danish, finnish.
            analyzers (TextAnalysisConfigAnalyzers | Unset): Named analyzers of type `custom`. `config.tokenizer` names a
                built-in or declared tokenizer; `config.char_filters` and `config.token_filters` list built-in or declared
                component names in application order. Configuration-free filters (`lowercase`, `stop_words`, `stemmer`,
                `camel_case`, `unique`, `reverse`, `elision`, `apostrophe`, `suffix`) can be listed by name without declaring
                them.
            default_datetime_parser (str | Unset): Name of the date-time parser applied to datetime fields without a field-
                specific parser.
            field_date_time_parsers (TextAnalysisConfigFieldDateTimeParsers | Unset): Map of field name to date-time parser
                name.
            date_time_parsers (TextAnalysisConfigDateTimeParsers | Unset): Named date-time parsers. Type `sanitizedgo`
                accepts `config.layouts`, a list of Go reference-time layouts tried in order.
    """

    field_analyzers: TextAnalysisConfigFieldAnalyzers | Unset = UNSET
    char_filters: TextAnalysisConfigCharFilters | Unset = UNSET
    tokenizers: TextAnalysisConfigTokenizers | Unset = UNSET
    token_filters: TextAnalysisConfigTokenFilters | Unset = UNSET
    analyzers: TextAnalysisConfigAnalyzers | Unset = UNSET
    default_datetime_parser: str | Unset = UNSET
    field_date_time_parsers: TextAnalysisConfigFieldDateTimeParsers | Unset = UNSET
    date_time_parsers: TextAnalysisConfigDateTimeParsers | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_analyzers: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_analyzers, Unset):
            field_analyzers = self.field_analyzers.to_dict()

        char_filters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.char_filters, Unset):
            char_filters = self.char_filters.to_dict()

        tokenizers: dict[str, Any] | Unset = UNSET
        if not isinstance(self.tokenizers, Unset):
            tokenizers = self.tokenizers.to_dict()

        token_filters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.token_filters, Unset):
            token_filters = self.token_filters.to_dict()

        analyzers: dict[str, Any] | Unset = UNSET
        if not isinstance(self.analyzers, Unset):
            analyzers = self.analyzers.to_dict()

        default_datetime_parser = self.default_datetime_parser

        field_date_time_parsers: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_date_time_parsers, Unset):
            field_date_time_parsers = self.field_date_time_parsers.to_dict()

        date_time_parsers: dict[str, Any] | Unset = UNSET
        if not isinstance(self.date_time_parsers, Unset):
            date_time_parsers = self.date_time_parsers.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if field_analyzers is not UNSET:
            field_dict["field_analyzers"] = field_analyzers
        if char_filters is not UNSET:
            field_dict["char_filters"] = char_filters
        if tokenizers is not UNSET:
            field_dict["tokenizers"] = tokenizers
        if token_filters is not UNSET:
            field_dict["token_filters"] = token_filters
        if analyzers is not UNSET:
            field_dict["analyzers"] = analyzers
        if default_datetime_parser is not UNSET:
            field_dict["default_datetime_parser"] = default_datetime_parser
        if field_date_time_parsers is not UNSET:
            field_dict["field_date_time_parsers"] = field_date_time_parsers
        if date_time_parsers is not UNSET:
            field_dict["date_time_parsers"] = date_time_parsers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.text_analysis_config_analyzers import TextAnalysisConfigAnalyzers
        from ..models.text_analysis_config_char_filters import TextAnalysisConfigCharFilters
        from ..models.text_analysis_config_date_time_parsers import TextAnalysisConfigDateTimeParsers
        from ..models.text_analysis_config_field_analyzers import TextAnalysisConfigFieldAnalyzers
        from ..models.text_analysis_config_field_date_time_parsers import TextAnalysisConfigFieldDateTimeParsers
        from ..models.text_analysis_config_token_filters import TextAnalysisConfigTokenFilters
        from ..models.text_analysis_config_tokenizers import TextAnalysisConfigTokenizers

        d = dict(src_dict)
        _field_analyzers = d.pop("field_analyzers", UNSET)
        field_analyzers: TextAnalysisConfigFieldAnalyzers | Unset
        if isinstance(_field_analyzers, Unset):
            field_analyzers = UNSET
        else:
            field_analyzers = TextAnalysisConfigFieldAnalyzers.from_dict(_field_analyzers)

        _char_filters = d.pop("char_filters", UNSET)
        char_filters: TextAnalysisConfigCharFilters | Unset
        if isinstance(_char_filters, Unset):
            char_filters = UNSET
        else:
            char_filters = TextAnalysisConfigCharFilters.from_dict(_char_filters)

        _tokenizers = d.pop("tokenizers", UNSET)
        tokenizers: TextAnalysisConfigTokenizers | Unset
        if isinstance(_tokenizers, Unset):
            tokenizers = UNSET
        else:
            tokenizers = TextAnalysisConfigTokenizers.from_dict(_tokenizers)

        _token_filters = d.pop("token_filters", UNSET)
        token_filters: TextAnalysisConfigTokenFilters | Unset
        if isinstance(_token_filters, Unset):
            token_filters = UNSET
        else:
            token_filters = TextAnalysisConfigTokenFilters.from_dict(_token_filters)

        _analyzers = d.pop("analyzers", UNSET)
        analyzers: TextAnalysisConfigAnalyzers | Unset
        if isinstance(_analyzers, Unset):
            analyzers = UNSET
        else:
            analyzers = TextAnalysisConfigAnalyzers.from_dict(_analyzers)

        default_datetime_parser = d.pop("default_datetime_parser", UNSET)

        _field_date_time_parsers = d.pop("field_date_time_parsers", UNSET)
        field_date_time_parsers: TextAnalysisConfigFieldDateTimeParsers | Unset
        if isinstance(_field_date_time_parsers, Unset):
            field_date_time_parsers = UNSET
        else:
            field_date_time_parsers = TextAnalysisConfigFieldDateTimeParsers.from_dict(_field_date_time_parsers)

        _date_time_parsers = d.pop("date_time_parsers", UNSET)
        date_time_parsers: TextAnalysisConfigDateTimeParsers | Unset
        if isinstance(_date_time_parsers, Unset):
            date_time_parsers = UNSET
        else:
            date_time_parsers = TextAnalysisConfigDateTimeParsers.from_dict(_date_time_parsers)

        text_analysis_config = cls(
            field_analyzers=field_analyzers,
            char_filters=char_filters,
            tokenizers=tokenizers,
            token_filters=token_filters,
            analyzers=analyzers,
            default_datetime_parser=default_datetime_parser,
            field_date_time_parsers=field_date_time_parsers,
            date_time_parsers=date_time_parsers,
        )

        text_analysis_config.additional_properties = d
        return text_analysis_config

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
