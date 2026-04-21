// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipelines

import (
	"context"
	"fmt"
	"image"
	"sort"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// assembleFullText joins recognized regions into a single string, using spaces
// for regions on the same line and newlines between different lines. Same-line
// detection reuses the tolerance logic from SortRegionsByReadingOrder.
func assembleFullText(regions []RecognizedRegion) string {
	if len(regions) == 0 {
		return ""
	}
	if len(regions) == 1 {
		return regions[0].Text
	}

	// Compute average line height for same-line tolerance.
	avgHeight := 0.0
	for _, r := range regions {
		avgHeight += r.BBox[3] - r.BBox[1]
	}
	avgHeight /= float64(len(regions))
	tolerance := avgHeight * 0.5

	var sb strings.Builder
	sb.WriteString(regions[0].Text)

	for i := 1; i < len(regions); i++ {
		prevY := (regions[i-1].BBox[1] + regions[i-1].BBox[3]) / 2
		curY := (regions[i].BBox[1] + regions[i].BBox[3]) / 2

		if abs(curY-prevY) < tolerance {
			sb.WriteByte(' ')
		} else {
			sb.WriteByte('\n')
		}
		sb.WriteString(regions[i].Text)
	}

	return sb.String()
}

// MultiStageOCRConfig configures a multi-stage OCR pipeline.
type MultiStageOCRConfig struct {
	// DetConfig configures the detection stage.
	DetConfig *DetectionConfig

	// RecConfig configures the recognition stage.
	RecConfig *RecognitionConfig

	// HasLayout indicates whether a layout model is available.
	HasLayout bool

	// HasOrder indicates whether a reading order model is available.
	HasOrder bool
}

// DetectionConfig configures the detection stage.
type DetectionConfig struct {
	// InputWidth is the detection model input width.
	InputWidth int
	// InputHeight is the detection model input height.
	InputHeight int
	// Threshold is the confidence threshold for detected regions.
	Threshold float32
	// MinBoxArea is the minimum bounding box area to keep.
	MinBoxArea int
}

// RecognitionConfig configures the recognition stage.
type RecognitionConfig struct {
	// InputHeight is the recognition model input height.
	InputHeight int
	// InputWidth is the recognition model input width.
	InputWidth int
}

// TextRegion represents a detected text region with bounding box.
type TextRegion struct {
	// BBox is the bounding box [x1, y1, x2, y2] in pixel coordinates.
	BBox [4]float64
	// Polygon is an optional set of polygon points defining the region boundary.
	Polygon [][2]float64
	// Confidence is the detection confidence score.
	Confidence float64
}

// RecognizedRegion is a TextRegion with recognized text.
type RecognizedRegion struct {
	TextRegion
	// Text is the recognized text within this region.
	Text string
	// RecConfidence is the recognition confidence score.
	RecConfidence float64
}

// LayoutRegion is a TextRegion with a semantic label.
type LayoutRegion struct {
	TextRegion
	// Label is the semantic label (e.g., "text", "title", "table", "figure").
	Label string
	// OrderIdx is the reading order index (set by order model or fallback sort).
	OrderIdx int
}

// MultiStageOCRResult holds the complete pipeline output.
type MultiStageOCRResult struct {
	// Regions contains recognized text regions with bounding boxes.
	Regions []RecognizedRegion
	// Layout contains layout regions with semantic labels (nil if no layout model).
	Layout []LayoutRegion
	// FullText is the concatenated text from all regions in reading order.
	FullText string
}

// DetectionPostProcessor converts raw model output to text regions.
type DetectionPostProcessor interface {
	// Process converts raw detection output to text regions.
	// output is the raw model output, width and height are the model input dimensions,
	// and originalBounds is the original image bounds for coordinate scaling.
	Process(output []float32, width, height int, originalBounds image.Rectangle) []TextRegion
}

// Recognizer abstracts over Vision2Seq (Surya) and CTC (PaddleOCR) recognition.
type Recognizer interface {
	// RecognizeImage recognizes text in a single cropped image region.
	// Returns the recognized text and confidence score.
	RecognizeImage(ctx context.Context, img image.Image) (string, float64, error)
	// Close releases recognizer resources.
	Close() error
}

// Vision2SeqRecognizer wraps an existing Vision2SeqPipeline for recognition.
// This reuses all existing encoder-decoder infrastructure with zero new inference code.
type Vision2SeqRecognizer struct {
	pipeline *Vision2SeqPipeline
}

// NewVision2SeqRecognizer creates a recognizer backed by a Vision2SeqPipeline.
func NewVision2SeqRecognizer(pipeline *Vision2SeqPipeline) *Vision2SeqRecognizer {
	return &Vision2SeqRecognizer{pipeline: pipeline}
}

// RecognizeImage runs the Vision2Seq pipeline on a cropped image region.
func (r *Vision2SeqRecognizer) RecognizeImage(ctx context.Context, img image.Image) (string, float64, error) {
	result, err := r.pipeline.Run(ctx, img)
	if err != nil {
		return "", 0, fmt.Errorf("vision2seq recognition: %w", err)
	}
	return strings.TrimSpace(result.Text), 1.0, nil
}

// Close releases the underlying pipeline resources.
func (r *Vision2SeqRecognizer) Close() error {
	return r.pipeline.Close()
}

// CTCRecognizer uses a single ONNX session + character dictionary for CTC decoding.
type CTCRecognizer struct {
	session        backends.Session
	charDict       []string
	imageProcessor *ImageProcessor
}

// NewCTCRecognizer creates a CTC-based recognizer.
func NewCTCRecognizer(session backends.Session, charDict []string, imgProc *ImageProcessor) *CTCRecognizer {
	return &CTCRecognizer{
		session:        session,
		charDict:       charDict,
		imageProcessor: imgProc,
	}
}

// RecognizeImage runs CTC recognition on a single cropped image region.
func (r *CTCRecognizer) RecognizeImage(ctx context.Context, img image.Image) (string, float64, error) {
	// Preprocess the image
	pixels, err := r.imageProcessor.Process(img)
	if err != nil {
		return "", 0, fmt.Errorf("preprocessing image: %w", err)
	}

	cfg := r.imageProcessor.Config
	batchSize := 1

	// Query actual input tensor name from the model.
	inputName := "x"
	if info := r.session.InputInfo(); len(info) > 0 {
		inputName = info[0].Name
	}

	inputs := []backends.NamedTensor{
		{
			Name:  inputName,
			Shape: []int64{int64(batchSize), int64(cfg.Channels), int64(cfg.Height), int64(cfg.Width)},
			Data:  pixels,
		},
	}

	outputs, err := r.session.Run(inputs)
	if err != nil {
		return "", 0, fmt.Errorf("running CTC recognition: %w", err)
	}

	if len(outputs) == 0 {
		return "", 0, fmt.Errorf("no output from CTC recognizer")
	}

	logits, ok := outputs[0].Data.([]float32)
	if !ok {
		return "", 0, fmt.Errorf("CTC output is not float32")
	}

	// Get output shape: [batch, time, vocab]
	shape := outputs[0].Shape
	if len(shape) != 3 {
		return "", 0, fmt.Errorf("unexpected CTC output shape: %v", shape)
	}
	timeSteps := int(shape[1])
	vocabSize := int(shape[2])

	// CTC decode
	text, confidence := CTCDecode(logits, timeSteps, vocabSize, r.charDict)
	return text, confidence, nil
}

// Close releases the CTC session.
func (r *CTCRecognizer) Close() error {
	return r.session.Close()
}

// MultiStageOCRPipeline coordinates detection, recognition, and optional
// layout/order models for multi-stage OCR.
// Unlike Florence-2 which implements backends.Model for autoregressive generation,
// this pipeline orchestrates independent models: detection is single-pass,
// layout is single-pass, and recognition may be a whole Vision2SeqPipeline.
type MultiStageOCRPipeline struct {
	detector     backends.Session
	recognizer   Recognizer
	layout       backends.Session // optional
	order        backends.Session // optional
	detProcessor DetectionPostProcessor
	detImgProc   *ImageProcessor
	config       *MultiStageOCRConfig
}

// NewMultiStageOCRPipeline creates a new multi-stage OCR pipeline.
func NewMultiStageOCRPipeline(
	detector backends.Session,
	recognizer Recognizer,
	detProcessor DetectionPostProcessor,
	detImgProc *ImageProcessor,
	config *MultiStageOCRConfig,
) *MultiStageOCRPipeline {
	return &MultiStageOCRPipeline{
		detector:     detector,
		recognizer:   recognizer,
		detProcessor: detProcessor,
		detImgProc:   detImgProc,
		config:       config,
	}
}

// SetLayout sets the optional layout analysis session.
func (p *MultiStageOCRPipeline) SetLayout(layout backends.Session) {
	p.layout = layout
}

// SetOrder sets the optional reading order session.
func (p *MultiStageOCRPipeline) SetOrder(order backends.Session) {
	p.order = order
}

// Run processes an image through the full multi-stage OCR pipeline.
func (p *MultiStageOCRPipeline) Run(ctx context.Context, img image.Image) (*MultiStageOCRResult, error) {
	// Step 1: Detect text regions
	regions, err := p.detect(ctx, img)
	if err != nil {
		return nil, fmt.Errorf("detection: %w", err)
	}

	if len(regions) == 0 {
		return &MultiStageOCRResult{}, nil
	}

	// Step 2: Layout analysis (optional)
	var layoutRegions []LayoutRegion
	if p.layout != nil {
		layoutRegions, err = p.analyzeLayout(ctx, img)
		if err != nil {
			return nil, fmt.Errorf("layout analysis: %w", err)
		}
	}

	// Step 3: Reading order (optional) or fallback sort
	if p.order != nil {
		regions, err = p.determineOrder(ctx, regions)
		if err != nil {
			return nil, fmt.Errorf("reading order: %w", err)
		}
	} else {
		SortRegionsByReadingOrder(regions)
	}

	// Step 4: Crop and recognize each region (or return detection-only results)
	recognized := make([]RecognizedRegion, 0, len(regions))

	if p.recognizer != nil {
		for _, region := range regions {
			cropped := CropBBox(img, region.BBox)

			text, conf, err := p.recognizer.RecognizeImage(ctx, cropped)
			if err != nil {
				// Log but continue with other regions
				continue
			}

			if text != "" {
				recognized = append(recognized, RecognizedRegion{
					TextRegion:    region,
					Text:          text,
					RecConfidence: conf,
				})
			}
		}
	} else {
		// Detection-only mode: return regions without recognized text
		for _, region := range regions {
			recognized = append(recognized, RecognizedRegion{
				TextRegion:    region,
				RecConfidence: region.Confidence,
			})
		}
	}

	return &MultiStageOCRResult{
		Regions:  recognized,
		Layout:   layoutRegions,
		FullText: assembleFullText(recognized),
	}, nil
}

// detect runs the detection model and post-processes results.
func (p *MultiStageOCRPipeline) detect(ctx context.Context, img image.Image) ([]TextRegion, error) {
	// Preprocess for detection
	pixels, err := p.detImgProc.Process(img)
	if err != nil {
		return nil, fmt.Errorf("preprocessing: %w", err)
	}

	cfg := p.detImgProc.Config
	batchSize := 1

	// Query actual input tensor name from the model (PaddleOCR uses "x",
	// Surya uses "pixel_values"). Follows the speech2seq.go pattern.
	inputName := "pixel_values"
	if info := p.detector.InputInfo(); len(info) > 0 {
		inputName = info[0].Name
	}

	inputs := []backends.NamedTensor{
		{
			Name:  inputName,
			Shape: []int64{int64(batchSize), int64(cfg.Channels), int64(cfg.Height), int64(cfg.Width)},
			Data:  pixels,
		},
	}

	outputs, err := p.detector.Run(inputs)
	if err != nil {
		return nil, fmt.Errorf("running detection: %w", err)
	}

	if len(outputs) == 0 {
		return nil, fmt.Errorf("no detection output")
	}

	outputData, ok := outputs[0].Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("detection output is not float32")
	}

	// Determine output spatial dimensions from shape.
	// Segmentation models output [batch, num_classes, outH, outW].
	// For single-channel heatmaps the shape is [batch, 1, outH, outW] or [batch, outH, outW].
	shape := outputs[0].Shape
	var outW, outH int
	var heatmap []float32

	switch len(shape) {
	case 4:
		// [batch, num_classes, H, W]
		numClasses := int(shape[1])
		outH = int(shape[2])
		outW = int(shape[3])
		planeSize := outH * outW

		if numClasses >= 2 {
			// Extract the text/foreground channel (class 1)
			heatmap = outputData[planeSize : 2*planeSize]
		} else {
			heatmap = outputData[:planeSize]
		}
	case 3:
		// [batch, H, W]
		outH = int(shape[1])
		outW = int(shape[2])
		heatmap = outputData[:outH*outW]
	default:
		// Flat output — assume same spatial dims as input
		outW = cfg.Width
		outH = cfg.Height
		heatmap = outputData
	}

	regions := p.detProcessor.Process(heatmap, outW, outH, img.Bounds())
	return regions, nil
}

// analyzeLayout runs the layout analysis model.
func (p *MultiStageOCRPipeline) analyzeLayout(ctx context.Context, img image.Image) ([]LayoutRegion, error) {
	pixels, err := p.detImgProc.Process(img)
	if err != nil {
		return nil, fmt.Errorf("preprocessing: %w", err)
	}

	cfg := p.detImgProc.Config
	batchSize := 1

	inputName := "pixel_values"
	if info := p.layout.InputInfo(); len(info) > 0 {
		inputName = info[0].Name
	}

	inputs := []backends.NamedTensor{
		{
			Name:  inputName,
			Shape: []int64{int64(batchSize), int64(cfg.Channels), int64(cfg.Height), int64(cfg.Width)},
			Data:  pixels,
		},
	}

	outputs, err := p.layout.Run(inputs)
	if err != nil {
		return nil, fmt.Errorf("running layout: %w", err)
	}

	if len(outputs) == 0 {
		return nil, nil
	}

	// Parse layout output - model-specific parsing handled by caller
	// For now, return the raw class heatmaps via connected components
	outputData, ok := outputs[0].Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("layout output is not float32")
	}

	shape := outputs[0].Shape
	if len(shape) != 4 {
		return nil, fmt.Errorf("unexpected layout output shape: %v", shape)
	}

	numClasses := int(shape[1])
	height := int(shape[2])
	width := int(shape[3])

	return parseLayoutOutput(outputData, numClasses, width, height, img.Bounds()), nil
}

// parseLayoutOutput converts class heatmaps to labeled layout regions.
func parseLayoutOutput(data []float32, numClasses, width, height int, originalBounds image.Rectangle) []LayoutRegion {
	// Layout class labels (Surya convention)
	classLabels := []string{
		"Caption", "Footnote", "Formula", "ListItem", "PageFooter",
		"PageHeader", "Picture", "SectionHeader", "Table", "Text", "Title",
	}

	var regions []LayoutRegion
	planeSize := width * height

	for c := 0; c < numClasses && c < len(classLabels); c++ {
		// Extract class heatmap
		heatmap := data[c*planeSize : (c+1)*planeSize]

		// Threshold and find connected components
		binary := make([]bool, planeSize)
		for i, v := range heatmap {
			binary[i] = v > 0.5
		}

		components := FindConnectedComponents(binary, width, height, 100)
		scaleX := float64(originalBounds.Dx()) / float64(width)
		scaleY := float64(originalBounds.Dy()) / float64(height)

		for _, comp := range components {
			regions = append(regions, LayoutRegion{
				TextRegion: TextRegion{
					BBox: [4]float64{
						float64(comp.MinX) * scaleX,
						float64(comp.MinY) * scaleY,
						float64(comp.MaxX+1) * scaleX,
						float64(comp.MaxY+1) * scaleY,
					},
					Confidence: 1.0,
				},
				Label: classLabels[c],
			})
		}
	}

	return regions
}

// determineOrder runs the reading order model to sort regions.
func (p *MultiStageOCRPipeline) determineOrder(ctx context.Context, regions []TextRegion) ([]TextRegion, error) {
	if len(regions) <= 1 {
		return regions, nil
	}

	// Prepare bbox input for order model: [batch, num_regions, 4]
	numRegions := len(regions)
	bboxData := make([]float32, numRegions*4)
	for i, r := range regions {
		bboxData[i*4+0] = float32(r.BBox[0])
		bboxData[i*4+1] = float32(r.BBox[1])
		bboxData[i*4+2] = float32(r.BBox[2])
		bboxData[i*4+3] = float32(r.BBox[3])
	}

	inputName := "boxes"
	if info := p.order.InputInfo(); len(info) > 0 {
		inputName = info[0].Name
	}

	inputs := []backends.NamedTensor{
		{
			Name:  inputName,
			Shape: []int64{1, int64(numRegions), 4},
			Data:  bboxData,
		},
	}

	outputs, err := p.order.Run(inputs)
	if err != nil {
		// Fall back to spatial sort
		SortRegionsByReadingOrder(regions)
		return regions, nil
	}

	if len(outputs) == 0 {
		SortRegionsByReadingOrder(regions)
		return regions, nil
	}

	// Parse order indices
	orderData, ok := outputs[0].Data.([]float32)
	if !ok {
		SortRegionsByReadingOrder(regions)
		return regions, nil
	}

	// Create index pairs and sort by predicted order
	type indexedRegion struct {
		region TextRegion
		order  float32
	}

	indexed := make([]indexedRegion, len(regions))
	for i, r := range regions {
		order := float32(0)
		if i < len(orderData) {
			order = orderData[i]
		}
		indexed[i] = indexedRegion{region: r, order: order}
	}

	sort.Slice(indexed, func(i, j int) bool {
		return indexed[i].order < indexed[j].order
	})

	sorted := make([]TextRegion, len(indexed))
	for i, ir := range indexed {
		sorted[i] = ir.region
	}

	return sorted, nil
}

// Close releases all pipeline resources following the encoder-decoder VLM
// multi-session teardown pattern (florence2.go).
func (p *MultiStageOCRPipeline) Close() error {
	var errs []error

	if p.detector != nil {
		if err := p.detector.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing detector: %w", err))
		}
		p.detector = nil
	}

	if p.recognizer != nil {
		if err := p.recognizer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing recognizer: %w", err))
		}
		p.recognizer = nil
	}

	if p.layout != nil {
		if err := p.layout.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing layout: %w", err))
		}
		p.layout = nil
	}

	if p.order != nil {
		if err := p.order.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing order: %w", err))
		}
		p.order = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pipeline: %v", errs)
	}
	return nil
}
