package schema

import (
	"maps"

	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/char/html"
	"github.com/blevesearch/bleve/v2/analysis/char/zerowidthnonjoiner"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/sanitized"
	_ "github.com/blevesearch/bleve/v2/analysis/token/ngram"
	"github.com/blevesearch/bleve/v2/analysis/token/edgengram"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
)

func cloneAnalysisComponentConfig(src AnalysisComponentConfig) AnalysisComponentConfig {
	dst := AnalysisComponentConfig{
		Type: src.Type,
	}
	if len(src.Config) > 0 {
		dst.Config = maps.Clone(src.Config)
	}
	return dst
}

func cloneAnalysisConfig(src *AnalysisConfig) *AnalysisConfig {
	if src == nil {
		return nil
	}
	dst := &AnalysisConfig{
		DefaultDateTimeParser: src.DefaultDateTimeParser,
	}
	if len(src.FieldDateTimeParsers) > 0 {
		dst.FieldDateTimeParsers = maps.Clone(src.FieldDateTimeParsers)
	}
	if len(src.FieldAnalyzers) > 0 {
		dst.FieldAnalyzers = maps.Clone(src.FieldAnalyzers)
	}
	if len(src.DateTimeParsers) > 0 {
		dst.DateTimeParsers = make(map[string]AnalysisComponentConfig, len(src.DateTimeParsers))
		for name, cfg := range src.DateTimeParsers {
			dst.DateTimeParsers[name] = cloneAnalysisComponentConfig(cfg)
		}
	}
	if len(src.CharFilters) > 0 {
		dst.CharFilters = make(map[string]AnalysisComponentConfig, len(src.CharFilters))
		for name, cfg := range src.CharFilters {
			dst.CharFilters[name] = cloneAnalysisComponentConfig(cfg)
		}
	}
	if len(src.TokenFilters) > 0 {
		dst.TokenFilters = make(map[string]AnalysisComponentConfig, len(src.TokenFilters))
		for name, cfg := range src.TokenFilters {
			dst.TokenFilters[name] = cloneAnalysisComponentConfig(cfg)
		}
	}
	if len(src.Tokenizers) > 0 {
		dst.Tokenizers = make(map[string]AnalysisComponentConfig, len(src.Tokenizers))
		for name, cfg := range src.Tokenizers {
			dst.Tokenizers[name] = cloneAnalysisComponentConfig(cfg)
		}
	}
	if len(src.Analyzers) > 0 {
		dst.Analyzers = make(map[string]AnalysisComponentConfig, len(src.Analyzers))
		for name, cfg := range src.Analyzers {
			dst.Analyzers[name] = cloneAnalysisComponentConfig(cfg)
		}
	}
	return dst
}

func analysisConfigFromSchema(tableSchema *TableSchema) *AnalysisConfig {
	compiled := cloneAnalysisConfig(tableSchema.AnalysisConfig)
	if compiled == nil {
		compiled = &AnalysisConfig{}
	}
	if compiled.DefaultDateTimeParser == "" {
		compiled.DefaultDateTimeParser = blevequery.QueryDateTimeParser
	}

	var searchAsYouTypeAnalyzerNeeded bool
	var htmlAnalyzerNeeded bool
	for _, docSchema := range tableSchema.DocumentSchemas {
		mergeAnalysisConfigDirectives(compiled, docSchema.Schema)
		collectFieldDateTimeParsers(compiled, "", docSchema.Schema)
		collectFieldAnalyzers(compiled, "", docSchema.Schema)
		var includeInAll []string
		if includeInAllI, ok := docSchema.Schema[XAntflyIncludeInAll]; ok {
			switch v := includeInAllI.(type) {
			case []any:
				for _, fieldName := range v {
					if fieldStr, ok := fieldName.(string); ok {
						includeInAll = append(includeInAll, fieldStr)
					}
				}
			case []string:
				includeInAll = append(includeInAll, v...)
			}
		}
		_, analyzerNeeded, htmlNeeded, _ := buildMappingFromJSONSchema(docSchema.Schema, includeInAll)
		searchAsYouTypeAnalyzerNeeded = searchAsYouTypeAnalyzerNeeded || analyzerNeeded
		htmlAnalyzerNeeded = htmlAnalyzerNeeded || htmlNeeded
	}

	if searchAsYouTypeAnalyzerNeeded {
		if compiled.TokenFilters == nil {
			compiled.TokenFilters = make(map[string]AnalysisComponentConfig)
		}
		if _, ok := compiled.TokenFilters[EdgeNgramTokenFilter]; !ok {
			compiled.TokenFilters[EdgeNgramTokenFilter] = AnalysisComponentConfig{
				Type: edgengram.Name,
				Config: map[string]any{
					"min": 2.0,
					"max": 4.0,
				},
			}
		}
		if compiled.Analyzers == nil {
			compiled.Analyzers = make(map[string]AnalysisComponentConfig)
		}
		if _, ok := compiled.Analyzers[SearchAsYouTypeAnalyzer]; !ok {
			compiled.Analyzers[SearchAsYouTypeAnalyzer] = AnalysisComponentConfig{
				Type: custom.Name,
				Config: map[string]any{
					"tokenizer": unicode.Name,
					"char_filters": []any{
						zerowidthnonjoiner.Name,
					},
					"token_filters": []any{
						"to_lower",
						"stop_en",
						EdgeNgramTokenFilter,
					},
				},
			}
		}
	}

	if htmlAnalyzerNeeded {
		if compiled.Analyzers == nil {
			compiled.Analyzers = make(map[string]AnalysisComponentConfig)
		}
		if _, ok := compiled.Analyzers[HTMLAnalyzer]; !ok {
			compiled.Analyzers[HTMLAnalyzer] = AnalysisComponentConfig{
				Type: custom.Name,
				Config: map[string]any{
					"tokenizer": unicode.Name,
					"char_filters": []any{
						html.Name,
					},
					"token_filters": []any{
						"to_lower",
						"stop_en",
					},
				},
			}
		}
	}

	return compiled
}

func collectFieldDateTimeParsers(compiled *AnalysisConfig, prefix string, schemaDef map[string]any) {
	if compiled == nil || len(schemaDef) == 0 {
		return
	}
	if parserName, ok := schemaDef[XAntflyDateTimeParser].(string); ok && parserName != "" && prefix != "" {
		if compiled.FieldDateTimeParsers == nil {
			compiled.FieldDateTimeParsers = make(map[string]string)
		}
		compiled.FieldDateTimeParsers[prefix] = parserName
	}

	properties, _ := schemaDef["properties"].(map[string]any)
	for fieldName, rawField := range properties {
		fieldSchema, ok := rawField.(map[string]any)
		if !ok {
			continue
		}
		fieldPath := fieldName
		if prefix != "" {
			fieldPath = prefix + "." + fieldName
		}
		collectFieldDateTimeParsers(compiled, fieldPath, fieldSchema)
	}
}

func mergeAnalysisConfigDirectives(compiled *AnalysisConfig, schemaDef map[string]any) {
	if compiled == nil || len(schemaDef) == 0 {
		return
	}
	if defaultParser, ok := schemaDef[XAntflyDefaultDateTimeParser].(string); ok && defaultParser != "" {
		compiled.DefaultDateTimeParser = defaultParser
	}
	mergeNamedComponents(&compiled.DateTimeParsers, schemaDef[XAntflyDateTimeParsers])
	mergeNamedComponents(&compiled.CharFilters, schemaDef[XAntflyCharFilters])
	mergeNamedComponents(&compiled.TokenFilters, schemaDef[XAntflyTokenFilters])
	mergeNamedComponents(&compiled.Tokenizers, schemaDef[XAntflyTokenizers])
	mergeNamedComponents(&compiled.Analyzers, schemaDef[XAntflyAnalyzers])
}

func normalizedAnalysisComponentConfig(raw map[string]any) (AnalysisComponentConfig, bool) {
	typeName, ok := raw["type"].(string)
	if !ok || typeName == "" {
		return AnalysisComponentConfig{}, false
	}
	component := AnalysisComponentConfig{
		Type:   typeName,
		Config: make(map[string]any, len(raw)-1),
	}
	for k, v := range raw {
		if k == "type" {
			continue
		}
		component.Config[k] = v
	}
	return component, true
}

func mergeNamedComponents(target *map[string]AnalysisComponentConfig, raw any) {
	rawComponents, ok := raw.(map[string]any)
	if !ok || len(rawComponents) == 0 {
		return
	}
	if *target == nil {
		*target = make(map[string]AnalysisComponentConfig)
	}
	for name, rawConfig := range rawComponents {
		configMap, ok := rawConfig.(map[string]any)
		if !ok {
			continue
		}
		component, ok := normalizedAnalysisComponentConfig(configMap)
		if !ok {
			continue
		}
		(*target)[name] = component
	}
}

func collectFieldAnalyzers(compiled *AnalysisConfig, prefix string, schemaDef map[string]any) {
	if compiled == nil || len(schemaDef) == 0 {
		return
	}
	if prefix != "" {
		if analyzerName, ok := schemaDef[XAntflyAnalyzer].(string); ok && analyzerName != "" {
			if compiled.FieldAnalyzers == nil {
				compiled.FieldAnalyzers = make(map[string]string)
			}
			compiled.FieldAnalyzers[prefix] = analyzerName
		} else if derived := defaultFieldAnalyzer(schemaDef); derived != "" {
			if compiled.FieldAnalyzers == nil {
				compiled.FieldAnalyzers = make(map[string]string)
			}
			compiled.FieldAnalyzers[prefix] = derived
		}
	}

	properties, _ := schemaDef["properties"].(map[string]any)
	for fieldName, rawField := range properties {
		fieldSchema, ok := rawField.(map[string]any)
		if !ok {
			continue
		}
		fieldPath := fieldName
		if prefix != "" {
			fieldPath = prefix + "." + fieldName
		}
		collectFieldAnalyzers(compiled, fieldPath, fieldSchema)
	}
}

func defaultFieldAnalyzer(schemaDef map[string]any) string {
	typesRaw, ok := schemaDef[XAntflyTypes]
	if !ok {
		return ""
	}
	var antflyTypes []string
	switch v := typesRaw.(type) {
	case []string:
		antflyTypes = append(antflyTypes, v...)
	case []any:
		for _, item := range v {
			if name, ok := item.(string); ok {
				antflyTypes = append(antflyTypes, name)
			}
		}
	}
	for _, antflyType := range antflyTypes {
		switch AntflyType(antflyType) {
		case AntflyTypeSearchAsYouType:
			return SearchAsYouTypeAnalyzer
		case AntflyTypeHtml:
			return HTMLAnalyzer
		case AntflyTypeKeyword, AntflyTypeLink:
			return "keyword"
		}
	}
	return ""
}

// EnsureAnalysisConfig compiles schema-derived analysis metadata into TableSchema.AnalysisConfig.
func (s *TableSchema) EnsureAnalysisConfig() {
	if s == nil {
		return
	}
	s.AnalysisConfig = analysisConfigFromSchema(s)
}
