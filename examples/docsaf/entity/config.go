package entity

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/antflydb/antfly/pkg/docsaf"
	docsafentity "github.com/antflydb/antfly/pkg/docsaf/entity"
	termiteclient "github.com/antflydb/antfly/pkg/termite-client"
)

const defaultTermiteURL = "http://localhost:8088"

var promptTextPattern = regexp.MustCompile(`\{\{\{?\s*Text\b`)

type ExtractorKind string

const (
	ExtractorKindRecognizer ExtractorKind = "recognizer"
	ExtractorKindGenerator  ExtractorKind = "generator"
)

// Config configures docsaf extraction and enrichment for the example CLI.
type Config struct {
	Provider          string
	Kind              ExtractorKind
	Model             string
	TermiteURL        string
	IncludeRelations  bool
	PromptTemplate    string
	EntityThreshold   float64
	RelationThreshold float64
	BatchSize         int
	EntityLabels      []string
	RelationLabels    []string
}

type stringSliceFlag struct {
	target *[]string
}

func (s *stringSliceFlag) String() string {
	if s == nil || s.target == nil {
		return ""
	}
	return strings.Join(*s.target, ", ")
}

func (s *stringSliceFlag) Set(value string) error {
	if s == nil || s.target == nil {
		return fmt.Errorf("string slice flag target is nil")
	}
	*s.target = append(*s.target, value)
	return nil
}

// RegisterFlags constructs a default extraction config and binds CLI flags to it.
func RegisterFlags(fs *flag.FlagSet) *Config {
	cfg := DefaultConfig()
	cfg.BindFlags(fs)
	return &cfg
}

// DefaultConfig returns the default extraction config for the example CLI.
func DefaultConfig() Config {
	return Config{
		Provider:          "termite",
		Kind:              ExtractorKindRecognizer,
		TermiteURL:        defaultTermiteURL,
		EntityThreshold:   0.5,
		RelationThreshold: 0.5,
		BatchSize:         docsafentity.DefaultBatchSize,
	}
}

// BindFlags registers extraction flags on the given FlagSet.
func (c *Config) BindFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.TermiteURL, "termite-url", defaultTermiteURL, "Termite API URL for extraction")
	fs.StringVar(&c.Provider, "extractor-provider", "termite", "Extractor provider")
	fs.StringVar((*string)(&c.Kind), "extractor-kind", string(ExtractorKindRecognizer), "Extractor kind: recognizer or generator")
	fs.StringVar(&c.Model, "extractor-model", "", "Extractor model name")
	fs.BoolVar(&c.IncludeRelations, "extractor-relations", false, "Extract relations in addition to entities when supported")
	fs.StringVar(&c.PromptTemplate, "extractor-prompt-template", "", "Handlebars prompt template for generator-based extraction instructions")
	fs.Float64Var(&c.EntityThreshold, "entity-threshold", 0.5, "Minimum confidence score for extracted entities")
	fs.Float64Var(&c.RelationThreshold, "relation-threshold", 0.5, "Minimum confidence score for extracted relations")
	fs.IntVar(&c.BatchSize, "entity-batch-size", docsafentity.DefaultBatchSize, "Number of texts per extraction batch")
	fs.Var(&stringSliceFlag{target: &c.EntityLabels}, "entity-label", "Entity label hint (can be repeated)")
	fs.Var(&stringSliceFlag{target: &c.RelationLabels}, "relation-label", "Relation label hint (can be repeated)")
}

// Enabled returns true if extraction is configured.
func (c Config) Enabled() bool {
	return strings.TrimSpace(c.Model) != ""
}

// Print writes the extraction configuration to the given writer.
func (c Config) Print(w io.Writer) {
	if !c.Enabled() {
		return
	}
	fmt.Fprintf(w, "Extractor provider: %s\n", c.Provider)
	fmt.Fprintf(w, "Extractor kind: %s\n", c.Kind)
	fmt.Fprintf(w, "Extractor model: %s\n", c.Model)
	fmt.Fprintf(w, "Extractor relations: %v\n", c.IncludeRelations)
	if strings.TrimSpace(c.PromptTemplate) != "" {
		fmt.Fprintf(w, "Extractor prompt template: configured\n")
	}
	if len(c.EntityLabels) > 0 {
		fmt.Fprintf(w, "Entity labels: %v\n", c.EntityLabels)
	}
	if len(c.RelationLabels) > 0 {
		fmt.Fprintf(w, "Relation labels: %v\n", c.RelationLabels)
	}
	fmt.Fprintf(w, "Entity threshold: %.2f\n", c.EntityThreshold)
	if c.IncludeRelations {
		fmt.Fprintf(w, "Relation threshold: %.2f\n", c.RelationThreshold)
	}
}

// Validate checks whether the extraction config is internally consistent.
func (c Config) Validate() error {
	if !c.Enabled() {
		return nil
	}

	if c.Provider != "" && c.Provider != "termite" {
		return fmt.Errorf("unsupported --extractor-provider %q", c.Provider)
	}

	switch c.Kind {
	case "", ExtractorKindRecognizer:
		if len(c.EntityLabels) == 0 {
			return fmt.Errorf("at least one --entity-label is required for extractor-kind=recognizer")
		}
		if strings.TrimSpace(c.PromptTemplate) != "" {
			return fmt.Errorf("extractor-kind=recognizer does not support --extractor-prompt-template")
		}
	case ExtractorKindGenerator:
		if c.IncludeRelations {
			return fmt.Errorf("extractor-kind=generator does not support --extractor-relations yet")
		}
		if promptTextPattern.MatchString(c.PromptTemplate) {
			return fmt.Errorf("--extractor-prompt-template must define instructions only; source text is sent separately")
		}
	default:
		return fmt.Errorf("unsupported --extractor-kind %q (want recognizer or generator)", c.Kind)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("--entity-batch-size must be greater than 0")
	}
	if c.EntityThreshold < 0 || c.EntityThreshold > 1 {
		return fmt.Errorf("--entity-threshold must be between 0 and 1")
	}
	if c.RelationThreshold < 0 || c.RelationThreshold > 1 {
		return fmt.Errorf("--relation-threshold must be between 0 and 1")
	}

	return nil
}

// Run performs extraction if configured, returning nil when extraction is disabled.
func (c Config) Run(ctx context.Context, sections []docsaf.DocumentSection) (*docsafentity.Result, error) {
	if !c.Enabled() {
		return nil, nil
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}

	tc, err := termiteclient.NewTermiteClient(c.TermiteURL, http.DefaultClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create termite client: %w", err)
	}

	extractor, err := buildTermiteExtractor(tc, c)
	if err != nil {
		return nil, err
	}

	totalBatches := 0
	if len(sections) > 0 {
		totalBatches = (len(sections) + c.BatchSize - 1) / c.BatchSize
	}

	fmt.Printf("Extracting entities with %s via %s/%s\n", c.Model, c.Provider, c.Kind)
	fmt.Printf("Processing %d sections in %d batches of %d\n\n", len(sections), totalBatches, c.BatchSize)

	result, err := docsafentity.NewEnricher(
		extractor,
		docsafentity.WithEntityLabels(c.EntityLabels),
		docsafentity.WithRelationLabels(c.RelationLabels),
		docsafentity.WithEntityThreshold(float32(c.EntityThreshold)),
		docsafentity.WithRelationThreshold(float32(c.RelationThreshold)),
		docsafentity.WithBatchSize(c.BatchSize),
	).Enrich(ctx, sections)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Extraction complete: %d unique entities across %d sections\n",
		len(result.EntityRecords), len(result.SectionEntityKeys))
	fmt.Printf("Entity types:\n")
	for label, count := range result.EntityLabelCounts() {
		fmt.Printf("  - %s: %d\n", label, count)
	}
	if len(result.RelationRecords) > 0 {
		fmt.Printf("Relations: %d unique relations across %d sections\n",
			len(result.RelationRecords), len(result.SectionRelationKeys))
		fmt.Printf("Relation types:\n")
		for label, count := range result.RelationLabelCounts() {
			fmt.Printf("  - %s: %d\n", label, count)
		}
	}
	fmt.Printf("\n")

	return result, nil
}
