package docsaf

import (
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/ajroetker/pdf"
)

// LayoutAnalyzer provides advanced PDF text extraction with column detection,
// table recognition, and improved reading order reconstruction.
type LayoutAnalyzer struct {
	// Configuration
	ColumnGapThreshold  float64 // Minimum gap width to consider as column separator (in points)
	RowTolerance        float64 // Y-coordinate tolerance for grouping into rows
	TableCellMinWidth   float64 // Minimum cell width to consider for table detection
	WordSpaceMultiplier float64 // Multiplier of font size to detect word boundaries

	// Extended options
	MinRowsForColumnPct int  // Minimum percentage of rows that must have gap for column detection (default 25)
	FilterLineNumbers   bool // Whether to filter out line number columns (for depositions)
	AutoDetectLayout    bool // Automatically detect and use optimal layout settings
	UseAdaptiveSpacing  bool // Use dynamic spacing threshold based on actual character spacing (default true)
}

// NewLayoutAnalyzer creates a LayoutAnalyzer with sensible defaults.
func NewLayoutAnalyzer() *LayoutAnalyzer {
	return &LayoutAnalyzer{
		ColumnGapThreshold:  30.0, // 30pt gap suggests column boundary
		RowTolerance:        3.0,  // 3pt Y tolerance for same row
		TableCellMinWidth:   20.0, // 20pt minimum cell width
		WordSpaceMultiplier: 0.3,  // 30% of font size = space
		MinRowsForColumnPct: 25,   // 25% of rows must have gap
		FilterLineNumbers:   false,
		AutoDetectLayout:    true, // Enable auto-detection by default
		UseAdaptiveSpacing:  true, // Enable adaptive spacing by default
	}
}

// NewLayoutAnalyzerWithConfig creates a LayoutAnalyzer with custom configuration.
// Note: UseAdaptiveSpacing defaults to false when using explicit configuration.
// Set it to true manually if you want adaptive spacing with custom settings.
func NewLayoutAnalyzerWithConfig(cfg LayoutConfig) *LayoutAnalyzer {
	return &LayoutAnalyzer{
		ColumnGapThreshold:  cfg.ColumnGapThreshold,
		RowTolerance:        cfg.RowTolerance,
		TableCellMinWidth:   20.0,
		WordSpaceMultiplier: 0.3,
		MinRowsForColumnPct: cfg.MinRowsForColumnPct,
		FilterLineNumbers:   cfg.FilterLineNumbers,
		AutoDetectLayout:    false, // Explicit config disables auto-detect
		UseAdaptiveSpacing:  false, // Explicit config uses fixed threshold
	}
}

// WithDepositionMode configures the analyzer for deposition transcript extraction.
// This uses tighter column detection and filters out line number columns.
func (la *LayoutAnalyzer) WithDepositionMode() *LayoutAnalyzer {
	la.ColumnGapThreshold = 12.0 // Narrow gaps for line number columns
	la.RowTolerance = 2.0        // Tight row grouping
	la.MinRowsForColumnPct = 75  // Line numbers on most rows
	la.FilterLineNumbers = true  // Remove line number column from output
	la.AutoDetectLayout = false  // Explicit mode
	return la
}

// TextBlock represents a block of text with position and content.
type TextBlock struct {
	X, Y          float64
	Width, Height float64
	Text          string
	FontSize      float64
	Chars         []pdf.Text // Original characters
}

// Column represents a detected column region.
type Column struct {
	Left, Right float64
	Blocks      []TextBlock
}

// TableCell represents a cell in a detected table.
type TableCell struct {
	Row, Col      int
	X, Y          float64
	Width, Height float64
	Text          string
}

// Table represents a detected table structure.
type Table struct {
	X, Y          float64
	Width, Height float64
	Rows          int
	Cols          int
	Cells         [][]TableCell
}

// detectColumns identifies column boundaries based on consistent vertical gaps.
func (la *LayoutAnalyzer) detectColumns(texts []pdf.Text, pageLeft, pageRight float64) []Column {
	// Group texts into rows first
	rows := la.groupIntoRows(texts)
	if len(rows) == 0 {
		return nil
	}

	// Build a gap histogram across all rows
	// Each gap is recorded with its X position and width
	gapCounts := make(map[int]int) // Bucketed X position -> count

	bucketSize := 20.0 // 20pt buckets for gap positions (tolerates variable column widths)

	for _, row := range rows {
		// Sort row by X position
		sortedRow := make([]pdf.Text, len(row))
		copy(sortedRow, row)
		sort.Slice(sortedRow, func(i, j int) bool {
			return sortedRow[i].X < sortedRow[j].X
		})

		// Find gaps between consecutive text elements
		for i := 0; i < len(sortedRow)-1; i++ {
			gapLeft := sortedRow[i].X + sortedRow[i].W
			gapRight := sortedRow[i+1].X
			gapWidth := gapRight - gapLeft

			// Only consider significant gaps
			if gapWidth >= la.ColumnGapThreshold {
				// Use the center of the gap as the bucket key
				gapCenter := (gapLeft + gapRight) / 2
				bucket := int(gapCenter / bucketSize)
				gapCounts[bucket]++
			}
		}
	}

	// Find gaps that appear in many rows (consistent column boundaries)
	// Use configurable percentage (default 25%, deposition mode uses 75%)
	pct := la.MinRowsForColumnPct
	if pct <= 0 {
		pct = 25
	}
	minRowsForColumn := max(len(rows)*pct/100, 3)

	var columnBoundaries []float64
	for bucket, count := range gapCounts {
		if count >= minRowsForColumn {
			columnBoundaries = append(columnBoundaries, float64(bucket)*bucketSize+bucketSize/2)
		}
	}

	// Sort boundaries
	sort.Float64s(columnBoundaries)

	// Merge nearby boundaries (within bucketSize*2)
	mergedBoundaries := []float64{}
	for _, b := range columnBoundaries {
		if len(mergedBoundaries) == 0 || b-mergedBoundaries[len(mergedBoundaries)-1] > bucketSize*2 {
			mergedBoundaries = append(mergedBoundaries, b)
		}
	}

	// Create columns based on boundaries
	if len(mergedBoundaries) == 0 {
		// No column boundaries detected - single column
		return []Column{{
			Left:   pageLeft,
			Right:  pageRight,
			Blocks: la.textsToBlocks(texts),
		}}
	}

	// Create column regions
	columns := make([]Column, len(mergedBoundaries)+1)
	columns[0] = Column{Left: pageLeft, Right: mergedBoundaries[0]}
	for i := 0; i < len(mergedBoundaries)-1; i++ {
		columns[i+1] = Column{Left: mergedBoundaries[i], Right: mergedBoundaries[i+1]}
	}
	columns[len(mergedBoundaries)] = Column{Left: mergedBoundaries[len(mergedBoundaries)-1], Right: pageRight}

	// Collect texts for each column
	columnTexts := make([][]pdf.Text, len(columns))
	for _, t := range texts {
		centerX := t.X + t.W/2
		for i := range columns {
			if centerX >= columns[i].Left && centerX <= columns[i].Right {
				columnTexts[i] = append(columnTexts[i], t)
				break
			}
		}
	}

	// Convert texts to blocks for each column using textsToBlocks
	// This properly groups characters into words and handles interleaved rows
	for i := range columns {
		if len(columnTexts[i]) > 0 {
			columns[i].Blocks = la.textsToBlocks(columnTexts[i])
		}
	}

	// Remove empty columns and sort blocks within each column
	nonEmptyColumns := make([]Column, 0, len(columns))
	for _, col := range columns {
		if len(col.Blocks) > 0 {
			// Sort blocks top-to-bottom, left-to-right
			sort.Slice(col.Blocks, func(i, j int) bool {
				if math.Abs(col.Blocks[i].Y-col.Blocks[j].Y) < la.RowTolerance {
					return col.Blocks[i].X < col.Blocks[j].X
				}
				return col.Blocks[i].Y > col.Blocks[j].Y // Higher Y = higher on page
			})
			nonEmptyColumns = append(nonEmptyColumns, col)
		}
	}

	return nonEmptyColumns
}

// groupIntoRows groups text elements by Y coordinate.
func (la *LayoutAnalyzer) groupIntoRows(texts []pdf.Text) [][]pdf.Text {
	if len(texts) == 0 {
		return nil
	}

	type rowBucket struct {
		yMin, yMax float64
		texts      []pdf.Text
	}

	var buckets []rowBucket

	for _, t := range texts {
		found := false
		for i := range buckets {
			if t.Y >= buckets[i].yMin-la.RowTolerance && t.Y <= buckets[i].yMax+la.RowTolerance {
				buckets[i].texts = append(buckets[i].texts, t)
				if t.Y < buckets[i].yMin {
					buckets[i].yMin = t.Y
				}
				if t.Y > buckets[i].yMax {
					buckets[i].yMax = t.Y
				}
				found = true
				break
			}
		}
		if !found {
			buckets = append(buckets, rowBucket{yMin: t.Y, yMax: t.Y, texts: []pdf.Text{t}})
		}
	}

	// Post-process: split buckets that have multiple distinct Y clusters.
	// This handles cases where two rows at similar Y (e.g., Y=767 and Y=769.5)
	// get merged but would interleave when sorted by X.
	var finalBuckets []rowBucket
	for _, bucket := range buckets {
		split := la.splitInterleavedRows(bucket.texts)
		for _, row := range split {
			if len(row) == 0 {
				continue
			}
			yMin, yMax := row[0].Y, row[0].Y
			for _, t := range row[1:] {
				if t.Y < yMin {
					yMin = t.Y
				}
				if t.Y > yMax {
					yMax = t.Y
				}
			}
			finalBuckets = append(finalBuckets, rowBucket{yMin: yMin, yMax: yMax, texts: row})
		}
	}

	// Sort buckets by Y (top to bottom = higher Y first)
	sort.Slice(finalBuckets, func(i, j int) bool {
		return finalBuckets[i].yMax > finalBuckets[j].yMax
	})

	rows := make([][]pdf.Text, len(finalBuckets))
	for i, bucket := range finalBuckets {
		rows[i] = bucket.texts
	}

	return rows
}

// splitInterleavedRows checks if a row has multiple distinct Y values that would
// cause interleaving when sorted by X. If so, it splits them into separate rows.
// This handles PDFs with multiple header lines at similar Y positions.
func (la *LayoutAnalyzer) splitInterleavedRows(texts []pdf.Text) [][]pdf.Text {
	if len(texts) < 4 {
		return [][]pdf.Text{texts}
	}

	// Find distinct Y values
	yValues := make(map[float64][]pdf.Text)
	for _, t := range texts {
		// Round Y to 0.1 precision to group very close values
		roundedY := math.Round(t.Y*10) / 10
		yValues[roundedY] = append(yValues[roundedY], t)
	}

	// If only one distinct Y, no splitting needed
	if len(yValues) <= 1 {
		return [][]pdf.Text{texts}
	}

	// Check if this would cause interleaving:
	// If different Y values have overlapping X ranges, they'll interleave when sorted by X
	type yRange struct {
		y         float64
		xMin      float64
		xMax      float64
		texts     []pdf.Text
		charCount int
	}

	var ranges []yRange
	for y, yTexts := range yValues {
		if len(yTexts) < 3 {
			// Very few chars at this Y, probably noise
			continue
		}
		xMin, xMax := yTexts[0].X, yTexts[0].X
		for _, t := range yTexts[1:] {
			if t.X < xMin {
				xMin = t.X
			}
			if t.X > xMax {
				xMax = t.X
			}
		}
		ranges = append(ranges, yRange{y: y, xMin: xMin, xMax: xMax, texts: yTexts, charCount: len(yTexts)})
	}

	if len(ranges) <= 1 {
		return [][]pdf.Text{texts}
	}

	// Check for X overlap between any two Y ranges
	hasOverlap := false
	for i := 0; i < len(ranges) && !hasOverlap; i++ {
		for j := i + 1; j < len(ranges); j++ {
			// Check if ranges overlap: NOT (r1.max < r2.min OR r2.max < r1.min)
			if !(ranges[i].xMax < ranges[j].xMin || ranges[j].xMax < ranges[i].xMin) {
				// They overlap - check if both have some content
				// Use low threshold (3) since column detection may split rows
				if ranges[i].charCount >= 3 && ranges[j].charCount >= 3 {
					hasOverlap = true
					break
				}
			}
		}
	}

	if !hasOverlap {
		// No significant overlap, keep as single row
		return [][]pdf.Text{texts}
	}

	// Split into separate rows by rounded Y value
	result := make([][]pdf.Text, 0, len(yValues))
	for _, yTexts := range yValues {
		if len(yTexts) > 0 {
			result = append(result, yTexts)
		}
	}

	return result
}

// calculateMedianCharSpacing calculates the median spacing between consecutive characters
// on the same row. This provides a data-driven baseline for determining word boundaries.
// Returns 0 if there's insufficient data to calculate a reliable median.
func (la *LayoutAnalyzer) calculateMedianCharSpacing(texts []pdf.Text) float64 {
	if len(texts) < 10 {
		return 0
	}

	var spacings []float64
	for i := 1; i < len(texts); i++ {
		// Only consider characters on the same row (within Y tolerance)
		if math.Abs(texts[i].Y-texts[i-1].Y) < la.RowTolerance {
			spacing := texts[i].X - (texts[i-1].X + texts[i-1].W)
			// Only consider positive spacings under a reasonable limit
			// (10x font size covers even very wide letter spacing)
			if spacing > 0 && spacing < texts[i].FontSize*10 {
				spacings = append(spacings, spacing)
			}
		}
	}

	if len(spacings) < 5 {
		return 0
	}

	// Return median spacing (more robust than mean)
	sort.Float64s(spacings)
	return spacings[len(spacings)/2]
}

// calculateWordSpacingThreshold analyzes gap distribution to find the optimal word boundary.
// It looks for a bimodal distribution (intra-word gaps vs inter-word gaps) and returns
// the threshold that separates them. Returns 0 if no clear separation is found.
func (la *LayoutAnalyzer) calculateWordSpacingThreshold(texts []pdf.Text) float64 {
	if len(texts) < 20 {
		return 0
	}

	// Collect all gaps with font size context
	type gapInfo struct {
		gap      float64
		fontSize float64
	}
	var gaps []gapInfo

	for i := 1; i < len(texts); i++ {
		if math.Abs(texts[i].Y-texts[i-1].Y) < la.RowTolerance {
			spacing := texts[i].X - (texts[i-1].X + texts[i-1].W)
			fontSize := texts[i].FontSize
			if fontSize == 0 {
				fontSize = 10.0
			}
			// Normalize to font size for comparison
			if spacing > 0 && spacing < fontSize*5 {
				gaps = append(gaps, gapInfo{gap: spacing, fontSize: fontSize})
			}
		}
	}

	if len(gaps) < 10 {
		return 0
	}

	// Sort gaps by size
	sort.Slice(gaps, func(i, j int) bool {
		return gaps[i].gap < gaps[j].gap
	})

	// Look for a significant jump in gap sizes (bimodal distribution)
	// The largest relative increase between consecutive gaps likely marks the word boundary
	bestRatio := 0.0
	bestThreshold := 0.0

	for i := len(gaps) / 4; i < len(gaps)*3/4; i++ { // Skip extremes
		if gaps[i].gap > 0 {
			ratio := gaps[i+1].gap / gaps[i].gap
			if ratio > bestRatio && ratio > 1.5 { // Need at least 1.5x jump
				bestRatio = ratio
				// Use midpoint as threshold
				bestThreshold = (gaps[i].gap + gaps[i+1].gap) / 2
			}
		}
	}

	// If we found a clear bimodal separation, use it
	if bestRatio > 1.5 {
		return bestThreshold
	}

	// Fallback: use percentile-based approach
	// Word spaces are typically larger than 75th percentile of gaps
	p75 := gaps[len(gaps)*3/4].gap
	avgFontSize := 0.0
	for _, g := range gaps {
		avgFontSize += g.fontSize
	}
	avgFontSize /= float64(len(gaps))

	// If 75th percentile is very small relative to font, spacing is uniform
	// This indicates the PDF has no clear word boundaries in positioning
	if p75 < avgFontSize*0.15 {
		return 0 // Signal that post-processing word segmentation may be needed
	}

	return p75 * 1.5
}

// textsToBlocks converts pdf.Text elements to TextBlocks, merging adjacent chars into words.
func (la *LayoutAnalyzer) textsToBlocks(texts []pdf.Text) []TextBlock {
	if len(texts) == 0 {
		return nil
	}

	// Calculate adaptive word spacing threshold if enabled
	var adaptiveThreshold float64
	if la.UseAdaptiveSpacing {
		// Try bimodal gap analysis first (more accurate for varied documents)
		adaptiveThreshold = la.calculateWordSpacingThreshold(texts)

		// If bimodal analysis returns 0, it indicates uniform spacing (no clear word boundaries)
		// Fall back to median-based approach
		if adaptiveThreshold == 0 {
			medianSpacing := la.calculateMedianCharSpacing(texts)
			if medianSpacing > 0 {
				// Use 5x median character spacing as word break threshold
				adaptiveThreshold = medianSpacing * 5.0
			}
		}
	}

	// Group into rows first
	rows := la.groupIntoRows(texts)

	var blocks []TextBlock
	for _, row := range rows {
		// Sort by X within row
		sort.Slice(row, func(i, j int) bool {
			return row[i].X < row[j].X
		})

		// Merge characters into words/blocks
		var currentBlock *TextBlock
		for _, t := range row {
			if currentBlock == nil {
				currentBlock = &TextBlock{
					X:        t.X,
					Y:        t.Y,
					Width:    t.W,
					Height:   t.FontSize,
					Text:     t.S,
					FontSize: t.FontSize,
					Chars:    []pdf.Text{t},
				}
				continue
			}

			// Check if this character should be part of the current block
			gap := t.X - (currentBlock.X + currentBlock.Width)

			// Use adaptive threshold if available, otherwise fall back to font-based threshold
			var threshold float64
			if adaptiveThreshold > 0 {
				threshold = adaptiveThreshold
			} else {
				threshold = la.WordSpaceMultiplier * currentBlock.FontSize
				if currentBlock.FontSize == 0 {
					threshold = 3.0 // fallback
				}
			}

			// Check if gap suggests word break
			isWordBreak := gap > threshold

			// Apply lowercase heuristic ONLY when not using adaptive spacing.
			// Adaptive spacing already properly detects word boundaries based on
			// the gap distribution, so we shouldn't override it.
			// This heuristic handles PDFs with unusual character spacing like "do cu me nt"
			// when using fixed threshold mode.
			if !la.UseAdaptiveSpacing && isWordBreak && len(currentBlock.Text) > 0 && len(t.S) > 0 {
				lastChar := rune(currentBlock.Text[len(currentBlock.Text)-1])
				nextChar := rune(t.S[0])
				bothLowercase := (lastChar >= 'a' && lastChar <= 'z') && (nextChar >= 'a' && nextChar <= 'z')
				// Also check for lowercase followed by uppercase that looks like fragment
				// e.g., "Virg" + "In" where 'g' is lowercase and 'I' is uppercase
				lowerThenUpper := (lastChar >= 'a' && lastChar <= 'z') && (nextChar >= 'A' && nextChar <= 'Z')

				if bothLowercase || lowerThenUpper {
					// Use 3x threshold for mid-word gaps to be more conservative
					if gap <= threshold*3 {
						isWordBreak = false
					}
				}
			}

			if !isWordBreak {
				// Same word/block - append
				currentBlock.Width = t.X + t.W - currentBlock.X
				currentBlock.Text += t.S
				currentBlock.Chars = append(currentBlock.Chars, t)
			} else {
				// New word - save current and start new
				blocks = append(blocks, *currentBlock)
				currentBlock = &TextBlock{
					X:        t.X,
					Y:        t.Y,
					Width:    t.W,
					Height:   t.FontSize,
					Text:     t.S,
					FontSize: t.FontSize,
					Chars:    []pdf.Text{t},
				}
			}
		}
		if currentBlock != nil {
			blocks = append(blocks, *currentBlock)
		}
	}

	// Apply post-processing word segmentation to blocks that look like merged words
	// This runs regardless of needsPostProcessingSegmentation because even with bimodal
	// threshold detection, some blocks may have internal zero-gaps that merge words
	if len(blocks) > 0 {
		tr := NewTextRepair()
		for i := range blocks {
			text := blocks[i].Text
			// Skip short blocks and blocks that already have spaces
			if len(text) < 15 {
				continue
			}
			// Check if block looks like merged words: long text with no spaces
			// and contains lowercase letters (not just abbreviations/codes)
			hasSpace := false
			hasLower := false
			for _, r := range text {
				if r == ' ' {
					hasSpace = true
					break
				}
				if r >= 'a' && r <= 'z' {
					hasLower = true
				}
			}
			if !hasSpace && hasLower {
				segmented := tr.SegmentWords(text)
				if segmented != text {
					blocks[i].Text = segmented
				}
			}
		}
	}

	return blocks
}

// detectTables identifies table structures from text blocks.
func (la *LayoutAnalyzer) detectTables(blocks []TextBlock) []Table {
	if len(blocks) < 4 { // Need at least 2x2 for a table
		return nil
	}

	// Find horizontal and vertical alignment patterns
	xPositions := make(map[int][]TextBlock) // Bucketed X -> blocks at that X
	yPositions := make(map[int][]TextBlock) // Bucketed Y -> blocks at that Y

	xBucket := 5.0 // 5pt buckets
	yBucket := 3.0 // 3pt buckets

	for _, b := range blocks {
		xKey := int(b.X / xBucket)
		yKey := int(b.Y / yBucket)
		xPositions[xKey] = append(xPositions[xKey], b)
		yPositions[yKey] = append(yPositions[yKey], b)
	}

	// Find column positions (X positions with multiple aligned blocks)
	var columnXs []float64
	for xKey, aligned := range xPositions {
		if len(aligned) >= 3 { // At least 3 vertically aligned blocks
			columnXs = append(columnXs, float64(xKey)*xBucket)
		}
	}

	// Find row positions (Y positions with multiple aligned blocks)
	var rowYs []float64
	for yKey, aligned := range yPositions {
		if len(aligned) >= 2 { // At least 2 horizontally aligned blocks
			rowYs = append(rowYs, float64(yKey)*yBucket)
		}
	}

	// Need at least 2 columns and 2 rows for a table
	if len(columnXs) < 2 || len(rowYs) < 2 {
		return nil
	}

	sort.Float64s(columnXs)
	sort.Float64s(rowYs)
	// Reverse rowYs so top row is first (higher Y = higher on page)
	for i, j := 0, len(rowYs)-1; i < j; i, j = i+1, j-1 {
		rowYs[i], rowYs[j] = rowYs[j], rowYs[i]
	}

	// Check for consistent spacing (table-like structure)
	if !la.hasConsistentSpacing(columnXs, 0.3) || !la.hasConsistentSpacing(rowYs, 0.3) {
		return nil
	}

	// Build the table
	table := Table{
		X:     columnXs[0],
		Y:     rowYs[0],
		Width: columnXs[len(columnXs)-1] - columnXs[0] + la.TableCellMinWidth,
		Rows:  len(rowYs),
		Cols:  len(columnXs),
		Cells: make([][]TableCell, len(rowYs)),
	}

	for r := range table.Cells {
		table.Cells[r] = make([]TableCell, len(columnXs))
		for c := range table.Cells[r] {
			table.Cells[r][c] = TableCell{
				Row: r,
				Col: c,
				X:   columnXs[c],
				Y:   rowYs[r],
			}
			if c < len(columnXs)-1 {
				table.Cells[r][c].Width = columnXs[c+1] - columnXs[c]
			} else {
				table.Cells[r][c].Width = la.TableCellMinWidth
			}
		}
	}

	// Assign blocks to cells
	for _, b := range blocks {
		// Find matching row
		rowIdx := -1
		for r, rowY := range rowYs {
			if math.Abs(b.Y-rowY) < yBucket*2 {
				rowIdx = r
				break
			}
		}
		if rowIdx == -1 {
			continue
		}

		// Find matching column
		colIdx := -1
		for c, colX := range columnXs {
			if math.Abs(b.X-colX) < xBucket*2 {
				colIdx = c
				break
			}
		}
		if colIdx == -1 {
			continue
		}

		// Add text to cell
		cell := &table.Cells[rowIdx][colIdx]
		if cell.Text != "" {
			cell.Text += " "
		}
		cell.Text += b.Text
	}

	return []Table{table}
}

// hasConsistentSpacing checks if positions have relatively consistent spacing.
func (la *LayoutAnalyzer) hasConsistentSpacing(positions []float64, tolerance float64) bool {
	if len(positions) < 2 {
		return false
	}

	// Calculate spacings
	spacings := make([]float64, len(positions)-1)
	for i := 0; i < len(positions)-1; i++ {
		spacings[i] = math.Abs(positions[i+1] - positions[i])
	}

	// Calculate mean
	var sum float64
	for _, s := range spacings {
		sum += s
	}
	mean := sum / float64(len(spacings))

	if mean == 0 {
		return false
	}

	// Check if all spacings are within tolerance of mean
	for _, s := range spacings {
		if math.Abs(s-mean)/mean > tolerance {
			return false
		}
	}

	return true
}

// formatBlocks converts blocks to text with proper spacing and line breaks.
func (la *LayoutAnalyzer) formatBlocks(blocks []TextBlock) string {
	if len(blocks) == 0 {
		return ""
	}

	var result strings.Builder
	var lastY float64 = -1
	var lastX float64 = -1

	for i, b := range blocks {
		if i == 0 {
			result.WriteString(b.Text)
			lastY = b.Y
			lastX = b.X + b.Width
			continue
		}

		// Check if we're on a new line (Y changed significantly)
		yDiff := math.Abs(lastY - b.Y)
		if yDiff > la.RowTolerance {
			result.WriteString("\n")
			lastX = -1 //nolint:ineffassign // reset for clarity of intent, used in next iteration
		} else if lastX >= 0 {
			// Same line - add space if there's a gap
			gap := b.X - lastX
			if gap > la.WordSpaceMultiplier*b.FontSize || gap > 3.0 {
				result.WriteString(" ")
			}
		}

		result.WriteString(b.Text)
		lastY = b.Y
		lastX = b.X + b.Width
	}

	return result.String()
}

// formatTable renders a table as aligned text.
func (la *LayoutAnalyzer) formatTable(table Table) string {
	if table.Rows == 0 || table.Cols == 0 {
		return ""
	}

	// Calculate column widths
	colWidths := make([]int, table.Cols)
	for c := 0; c < table.Cols; c++ {
		for r := 0; r < table.Rows; r++ {
			cellLen := len(table.Cells[r][c].Text)
			if cellLen > colWidths[c] {
				colWidths[c] = cellLen
			}
		}
		// Minimum width
		if colWidths[c] < 3 {
			colWidths[c] = 3
		}
	}

	var result strings.Builder

	for r := 0; r < table.Rows; r++ {
		if r > 0 {
			result.WriteString("\n")
		}
		for c := 0; c < table.Cols; c++ {
			if c > 0 {
				result.WriteString(" | ")
			}
			cell := table.Cells[r][c].Text
			// Pad cell to column width
			result.WriteString(cell)
			for i := len(cell); i < colWidths[c]; i++ {
				result.WriteString(" ")
			}
		}
	}

	return result.String()
}

// FontDecoder handles font encoding issues common in PDFs.
type FontDecoder struct {
	// Common substitution maps for problematic fonts
	ligatures     map[rune]string
	substitutions map[rune]rune
}

// NewFontDecoder creates a FontDecoder with common substitutions.
func NewFontDecoder() *FontDecoder {
	return &FontDecoder{
		ligatures: map[rune]string{
			'\uFB00': "ff",
			'\uFB01': "fi",
			'\uFB02': "fl",
			'\uFB03': "ffi",
			'\uFB04': "ffl",
			'\uFB05': "st", // long s + t
			'\uFB06': "st",
			'\u0132': "IJ", // Dutch IJ
			'\u0133': "ij",
			'\u0152': "OE", // French OE
			'\u0153': "oe",
			'\u00C6': "AE", // AE ligature
			'\u00E6': "ae",
		},
		substitutions: map[rune]rune{
			// Smart quotes to ASCII
			'\u2018': '\'', // left single quote
			'\u2019': '\'', // right single quote
			'\u201C': '"',  // left double quote
			'\u201D': '"',  // right double quote
			// Dashes
			'\u2013': '-', // en dash
			'\u2014': '-', // em dash
			'\u2015': '-', // horizontal bar
			'\u2212': '-', // minus sign
			// Spaces
			'\u00A0': ' ', // non-breaking space
			'\u2000': ' ', // en quad
			'\u2001': ' ', // em quad
			'\u2002': ' ', // en space
			'\u2003': ' ', // em space
			'\u2004': ' ', // three-per-em space
			'\u2005': ' ', // four-per-em space
			'\u2006': ' ', // six-per-em space
			'\u2007': ' ', // figure space
			'\u2008': ' ', // punctuation space
			'\u2009': ' ', // thin space
			'\u200A': ' ', // hair space
			'\u202F': ' ', // narrow no-break space
			'\u205F': ' ', // medium mathematical space
			'\u3000': ' ', // ideographic space
			// Other common substitutions
			'\u2022': '*', // bullet
			'\u2023': '>', // triangular bullet
			'\u2043': '-', // hyphen bullet
			'\u00B7': '.', // middle dot
			// Note: '\u2026' (ellipsis) is handled specially in Decode()
		},
	}
}

// Decode normalizes text by expanding ligatures and fixing encoding issues.
func (fd *FontDecoder) Decode(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		// Check for ligatures first
		if expanded, ok := fd.ligatures[r]; ok {
			result.WriteString(expanded)
			continue
		}

		// Check for simple substitutions
		if sub, ok := fd.substitutions[r]; ok {
			result.WriteRune(sub)
			continue
		}

		// Handle ellipsis specially
		if r == '\u2026' {
			result.WriteString("...")
			continue
		}

		// Pass through normal characters
		result.WriteRune(r)
	}

	return result.String()
}

// DecodeROT3 attempts to decode ROT3-encoded text (common in some PDFs).
// ROT3 shifts each letter by 3 positions in the alphabet.
func (fd *FontDecoder) DecodeROT3(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if r >= 'A' && r <= 'Z' {
			// Shift uppercase back by 3
			shifted := 'A' + (r-'A'+23)%26
			result.WriteRune(shifted)
		} else if r >= 'a' && r <= 'z' {
			// Shift lowercase back by 3
			shifted := 'a' + (r-'a'+23)%26
			result.WriteRune(shifted)
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// IsLikelyROT3 checks if text appears to be ROT3 encoded.
func (fd *FontDecoder) IsLikelyROT3(text string) bool {
	// ROT3 encoded English has characteristic patterns
	// Common words like "the" become "wkh", "and" becomes "dqg"
	lower := strings.ToLower(text)
	rot3Patterns := []string{"wkh", "dqg", "iru", "zlwk", "wklv", "iurp"}

	matchCount := 0
	for _, pattern := range rot3Patterns {
		if strings.Contains(lower, pattern) {
			matchCount++
		}
	}

	// If multiple ROT3 patterns found, likely encoded
	return matchCount >= 2
}

// GlyphMapper handles Private Use Area (PUA) character mapping.
// Many PDFs map custom fonts to PUA characters (U+E000-U+F8FF).
type GlyphMapper struct {
	// Maps PUA codepoints to their likely ASCII equivalents
	puaToASCII map[rune]string
}

// NewGlyphMapper creates a GlyphMapper with common PUA mappings.
func NewGlyphMapper() *GlyphMapper {
	gm := &GlyphMapper{
		puaToASCII: make(map[rune]string),
	}

	// Pre-populate with common Symbol font PUA mappings
	// Some PDFs map Symbol font to U+F000 + original code point
	// Add Greek letters and math symbols from Symbol encoding
	symbolMappings := map[rune]rune{
		0xF041: 0x0391, // Alpha
		0xF042: 0x0392, // Beta
		0xF047: 0x0393, // Gamma
		0xF044: 0x0394, // Delta
		0xF053: 0x03A3, // Sigma
		0xF057: 0x03A9, // Omega
		0xF061: 0x03B1, // alpha
		0xF062: 0x03B2, // beta
		0xF067: 0x03B3, // gamma
		0xF064: 0x03B4, // delta
		0xF070: 0x03C0, // pi
		0xF073: 0x03C3, // sigma
		0xF077: 0x03C9, // omega
		// Common math symbols
		0xF0B1: 0x00B1, // plus-minus
		0xF0B4: 0x00D7, // multiplication
		0xF0B8: 0x00F7, // division
		0xF0B9: 0x2260, // not equal
		0xF0A3: 0x2264, // less than or equal
		0xF0B3: 0x2265, // greater than or equal
		0xF0A5: 0x221E, // infinity
		0xF0D6: 0x221A, // square root
	}

	for pua, unicode := range symbolMappings {
		gm.puaToASCII[pua] = string(unicode)
	}

	return gm
}

// LearnFromContext tries to learn PUA mappings from surrounding context.
// This is a heuristic approach that looks for patterns.
func (gm *GlyphMapper) LearnFromContext(texts []pdf.Text) {
	// Look for sequences like "PUA + known_word" or "known_word + PUA"
	// and try to infer what the PUA character represents

	// Common patterns in legal documents
	knownPhrases := []struct {
		pattern string
		expect  rune
	}{
		{"Plaintiff", 'P'},
		{"Defendant", 'D'},
		{"Court", 'C'},
		{"Judge", 'J'},
		{"Section", 'S'},
		{"Article", 'A'},
	}

	for i := 0; i < len(texts)-1; i++ {
		currRunes := []rune(texts[i].S)
		if len(currRunes) != 1 {
			continue
		}

		r := currRunes[0]
		if !gm.isPUA(r) {
			continue
		}

		// Check if next text starts a known phrase
		nextText := texts[i+1].S
		for _, kp := range knownPhrases {
			if strings.HasPrefix(nextText, kp.pattern[1:]) {
				gm.puaToASCII[r] = string(kp.expect)
				break
			}
		}
	}
}

func (gm *GlyphMapper) isPUA(r rune) bool {
	return r >= '\uE000' && r <= '\uF8FF'
}

// Map converts PUA characters to their ASCII equivalents if known.
func (gm *GlyphMapper) Map(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if gm.isPUA(r) {
			if mapped, ok := gm.puaToASCII[r]; ok {
				result.WriteString(mapped)
			} else if r >= 0xF000 && r <= 0xF0FF {
				// Try Symbol encoding table for F0XX range
				// These are often Symbol font chars mapped to PUA
				idx := int(r - 0xF000)
				if idx < len(SymbolEncoding) && SymbolEncoding[idx] != 0 {
					result.WriteRune(SymbolEncoding[idx])
				} else {
					result.WriteRune(' ') // Unknown Symbol char -> space
				}
			} else if r >= 0xF100 && r <= 0xF1FF {
				// Try ZapfDingbats encoding for F1XX range
				idx := int(r - 0xF100)
				if idx < len(ZapfDingbatsEncoding) && ZapfDingbatsEncoding[idx] != 0 {
					result.WriteRune(ZapfDingbatsEncoding[idx])
				} else {
					result.WriteRune(' ') // Unknown Dingbats char -> space
				}
			} else {
				result.WriteRune(' ') // Unknown PUA -> space
			}
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// EnhancedTextCleaner provides more aggressive text cleanup.
type EnhancedTextCleaner struct {
	fontDecoder     *FontDecoder
	glyphMapper     *GlyphMapper
	layoutAnalyzer  *LayoutAnalyzer
	textRepair      *TextRepair
	encodingDecoder *EncodingFallbackDecoder
}

// NewEnhancedTextCleaner creates a cleaner with all components initialized.
func NewEnhancedTextCleaner() *EnhancedTextCleaner {
	return &EnhancedTextCleaner{
		fontDecoder:     NewFontDecoder(),
		glyphMapper:     NewGlyphMapper(),
		layoutAnalyzer:  NewLayoutAnalyzer(),
		textRepair:      NewTextRepair(),
		encodingDecoder: NewEncodingFallbackDecoder(),
	}
}

// Clean applies all cleaning steps to extracted text.
func (etc *EnhancedTextCleaner) Clean(text string) string {
	// Step 1: Remove zero-width and invisible characters early
	// These can interfere with word boundary detection
	text = CleanZeroWidthChars(text)

	// Step 2: Decode ligatures and normalize characters
	text = etc.fontDecoder.Decode(text)

	// Step 3: Handle PUA characters
	text = etc.glyphMapper.Map(text)

	// Step 4: Auto-detect and fix encoding issues (ROT-N, symbol substitution)
	// Uses frequency analysis for more accurate detection than pattern matching
	decoded, fixType := etc.textRepair.AutoDecodeText(text)
	if fixType != "" {
		text = decoded
	} else if etc.fontDecoder.IsLikelyROT3(text) {
		// Fallback to pattern-based ROT3 detection
		text = etc.fontDecoder.DecodeROT3(text)
	}

	// Step 5: Try PDF standard encoding fallback for remaining issues
	// This handles fonts that use StandardEncoding, SymbolEncoding, or ZapfDingbats
	decoded, _ = etc.encodingDecoder.DecodeWithFallback(text)
	text = decoded

	// Step 6: Remove interleaved U+FFFD characters (pattern: char, FFFD, char, FFFD)
	// This fixes pages where font encoding produced "C·O·N·F·I·D·E·N·T·I·A·L" patterns
	text = etc.textRepair.RemoveInterleavedReplacements(text)

	// Step 7: Repair Symbol font Greek letters back to Latin
	// This handles PDFs where Symbol font was used to encode English text
	text = RepairSymbolGreekText(text)

	// Step 8: Clean box drawing characters that appear as artifacts
	text = CleanBoxDrawingChars(text)

	// Step 9: Join hyphenated words split across lines
	// Example: "state-\nment" → "statement"
	text = JoinHyphenatedWords(text)

	// Step 10: Apply Unicode NFC normalization for consistency
	// Converts combining character sequences to composed forms
	text = NormalizeUnicode(text)

	// Step 11: Final whitespace cleanup
	text = etc.normalizeWhitespace(text)

	return text
}

// CleanWithParagraphs applies all cleaning steps plus paragraph detection.
// Use this when semantic paragraph structure is desired.
func (etc *EnhancedTextCleaner) CleanWithParagraphs(text string) string {
	// Apply standard cleaning
	text = etc.Clean(text)

	// Detect and mark paragraph boundaries
	text = DetectParagraphBreaks(text)

	return text
}

// CleanWithEnhancedParagraphs applies cleaning with sophisticated paragraph detection.
// Uses ML-like heuristics to detect headers, lists, indentation, and spacing patterns.
func (etc *EnhancedTextCleaner) CleanWithEnhancedParagraphs(text string) string {
	// Apply standard cleaning
	text = etc.Clean(text)

	// Apply enhanced paragraph detection with default config
	text = EnhancedParagraphDetection(text, DefaultParagraphConfig())

	return text
}

// CleanWithEnhancedParagraphsConfig applies cleaning with configurable paragraph detection.
func (etc *EnhancedTextCleaner) CleanWithEnhancedParagraphsConfig(text string, config ParagraphConfig) string {
	// Apply standard cleaning
	text = etc.Clean(text)

	// Apply enhanced paragraph detection
	text = EnhancedParagraphDetection(text, config)

	return text
}

// CleanForSearch applies all cleaning steps plus normalization for text search.
// This normalizes subscripts/superscripts so that H₂O matches H2O.
func (etc *EnhancedTextCleaner) CleanForSearch(text string) string {
	// Apply standard cleaning
	text = etc.Clean(text)

	// Normalize subscripts and superscripts for search indexing
	text = NormalizeSubSuperscripts(text)

	return text
}

// CleanWithFootnotes applies all cleaning and expands footnote references.
// Example: "statement¹" → "statement[1]"
func (etc *EnhancedTextCleaner) CleanWithFootnotes(text string) string {
	// Apply standard cleaning
	text = etc.Clean(text)

	// Expand footnote superscripts to bracketed format
	text = ExpandFootnoteReferences(text)

	return text
}

// CleanWithLists applies cleaning and normalizes list formatting.
// Ensures consistent bullet styles and indentation.
func (etc *EnhancedTextCleaner) CleanWithLists(text string) string {
	// Apply standard cleaning
	text = etc.Clean(text)

	// Detect and normalize list formatting
	text = DetectAndFormatLists(text)

	return text
}

// CleanFull applies all Phase 1 and Phase 2 cleaning for maximum text quality.
// Includes: basic cleaning, enhanced paragraphs, list formatting, and footnote expansion.
func (etc *EnhancedTextCleaner) CleanFull(text string) string {
	// Apply standard cleaning
	text = etc.Clean(text)

	// Normalize list formatting
	text = DetectAndFormatLists(text)

	// Expand footnote references
	text = ExpandFootnoteReferences(text)

	// Apply enhanced paragraph detection
	text = EnhancedParagraphDetection(text, DefaultParagraphConfig())

	return text
}

func (etc *EnhancedTextCleaner) normalizeWhitespace(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	prevSpace := false
	for _, r := range text {
		if unicode.IsSpace(r) {
			if r == '\n' {
				result.WriteRune('\n')
				prevSpace = true
			} else if !prevSpace {
				result.WriteRune(' ')
				prevSpace = true
			}
		} else {
			result.WriteRune(r)
			prevSpace = false
		}
	}

	return strings.TrimSpace(result.String())
}
