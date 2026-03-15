package docsaf

import "github.com/antflydb/antfly/pkg/libaf/reading"

// RegistryOption configures a DefaultRegistry.
type RegistryOption func(*registryConfig)

type registryConfig struct {
	ocr         reading.Reader // PDF OCR fallback
	imageReader reading.Reader // image file reader
}

// WithOCR sets the reader used for OCR fallback on PDF pages with poor text
// extraction. Pages are rendered to PNG and passed to the reader.
func WithOCR(r reading.Reader) RegistryOption {
	return func(c *registryConfig) {
		c.ocr = r
	}
}

// WithImageReader sets the reader used for processing image files
// (PNG, JPEG, TIFF, WebP, BMP, GIF).
func WithImageReader(r reading.Reader) RegistryOption {
	return func(c *registryConfig) {
		c.imageReader = r
	}
}

// WithReader is a convenience that sets the same reader for both PDF OCR
// fallback and image file processing.
func WithReader(r reading.Reader) RegistryOption {
	return func(c *registryConfig) {
		c.ocr = r
		c.imageReader = r
	}
}

// registry is a simple implementation of ProcessorRegistry.
type registry struct {
	processors []ContentProcessor
}

// NewRegistry creates a new empty processor registry.
// Use this to build a custom registry with only the processors you need.
func NewRegistry() ProcessorRegistry {
	return &registry{
		processors: make([]ContentProcessor, 0),
	}
}

// DefaultRegistry creates a registry with all built-in processors registered.
// This always includes MarkdownProcessor, OpenAPIProcessor, HTMLProcessor, and
// PDFProcessor. Use options to configure OCR and image support:
//
//	docsaf.DefaultRegistry()                                        // no OCR
//	docsaf.DefaultRegistry(docsaf.WithReader(r))                    // same reader for PDF OCR and images
//	docsaf.DefaultRegistry(docsaf.WithOCR(ocr), docsaf.WithImageReader(vision)) // different readers
func DefaultRegistry(opts ...RegistryOption) ProcessorRegistry {
	var cfg registryConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	capacity := 6
	if cfg.imageReader != nil {
		capacity = 7
	}
	r := &registry{
		processors: make([]ContentProcessor, 0, capacity),
	}
	r.Register(&MarkdownProcessor{})
	r.Register(&OpenAPIProcessor{})
	r.Register(&HTMLProcessor{})
	r.Register(&PDFProcessor{OCR: cfg.ocr})
	r.Register(&DocxProcessor{})
	r.Register(&PptxProcessor{})
	if cfg.imageReader != nil {
		r.Register(&ImageProcessor{Reader: cfg.imageReader})
	}
	return r
}

// NewWholeFileRegistry creates a registry with only the WholeFileProcessor.
// This processor returns entire content without any chunking,
// allowing Antfly's internal chunking (e.g., Termite) to handle
// document segmentation during the embedding process.
func NewWholeFileRegistry() ProcessorRegistry {
	r := &registry{
		processors: make([]ContentProcessor, 0, 1),
	}
	r.Register(&WholeFileProcessor{})
	return r
}

// Register adds a processor to the registry.
func (r *registry) Register(processor ContentProcessor) {
	r.processors = append(r.processors, processor)
}

// GetProcessor returns the first processor that can handle the given content.
// Returns nil if no processor can handle the content.
func (r *registry) GetProcessor(contentType, path string) ContentProcessor {
	for _, processor := range r.processors {
		if processor.CanProcess(contentType, path) {
			return processor
		}
	}
	return nil
}

// Processors returns all registered processors.
func (r *registry) Processors() []ContentProcessor {
	return r.processors
}
