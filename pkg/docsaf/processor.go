package docsaf

import (
	"context"
	"log"
)

// ProcessorOption configures a Processor.
type ProcessorOption func(*Processor)

// WithIDTransform sets a function that transforms section IDs after generation.
// The function receives the file path and the default-generated ID, and should
// return the final ID. This is useful for adding prefixes, namespacing by
// source, or using a different ID format entirely.
//
// Example:
//
//	WithIDTransform(func(path, id string) string { return "source1/" + id })
func WithIDTransform(fn func(path, id string) string) ProcessorOption {
	return func(p *Processor) {
		p.idTransform = fn
	}
}

// Processor processes content from any source using registered processors.
// It abstracts the traversal mechanism, allowing the same processing logic
// to work with filesystem, web, and other content sources.
type Processor struct {
	source      ContentSource
	registry    ProcessorRegistry
	baseURL     string
	idTransform func(path, id string) string
}

// NewProcessor creates a new processor.
// The source provides content items, and the registry provides processors to handle them.
func NewProcessor(source ContentSource, registry ProcessorRegistry, opts ...ProcessorOption) *Processor {
	p := &Processor{
		source:   source,
		registry: registry,
		baseURL:  source.BaseURL(),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// SetBaseURL sets the base URL for generated links.
// This overrides the base URL from the source.
func (p *Processor) SetBaseURL(baseURL string) {
	p.baseURL = baseURL
}

// transformIDs applies the user's ID transform function to all sections, if set.
func (p *Processor) transformIDs(sections []DocumentSection) {
	if p.idTransform == nil {
		return
	}
	for i := range sections {
		sections[i].ID = p.idTransform(sections[i].FilePath, sections[i].ID)
	}
}

// Process traverses the source and processes all content items.
// Returns a slice of all extracted DocumentSections.
func (p *Processor) Process(ctx context.Context) ([]DocumentSection, error) {
	var allSections []DocumentSection

	items, errs := p.source.Traverse(ctx)

	for item := range items {
		processor := p.registry.GetProcessor(item.ContentType, item.Path)
		if processor == nil {
			continue
		}

		sections, err := processor.Process(
			item.Path,
			item.SourceURL,
			p.baseURL,
			item.Content,
		)
		if err != nil {
			log.Printf("Warning: Failed to process %s: %v", item.Path, err)
			continue
		}

		p.transformIDs(sections)
		allSections = append(allSections, sections...)
	}

	for err := range errs {
		if err != nil {
			return allSections, err
		}
	}

	return allSections, nil
}

// ProcessWithCallback traverses the source and calls the callback for each batch of sections.
// This is useful for streaming large amounts of content without holding everything in memory.
func (p *Processor) ProcessWithCallback(ctx context.Context, callback func([]DocumentSection) error) error {
	items, errs := p.source.Traverse(ctx)

	for item := range items {
		processor := p.registry.GetProcessor(item.ContentType, item.Path)
		if processor == nil {
			continue
		}

		sections, err := processor.Process(
			item.Path,
			item.SourceURL,
			p.baseURL,
			item.Content,
		)
		if err != nil {
			log.Printf("Warning: Failed to process %s: %v", item.Path, err)
			continue
		}

		p.transformIDs(sections)

		if len(sections) > 0 {
			if err := callback(sections); err != nil {
				return err
			}
		}
	}

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// SourceType returns the type of the underlying content source.
func (p *Processor) SourceType() string {
	return p.source.Type()
}
