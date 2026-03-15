/*
Copyright 2025 The Antfly Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cli

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	antfly "github.com/antflydb/antfly/pkg/client"
	json "github.com/antflydb/antfly/pkg/libaf/json"
)

// outputFormat represents the CLI output format.
type outputFormat string

const (
	outputTable outputFormat = "table"
	outputJSON  outputFormat = "json"
	outputJSONL outputFormat = "jsonl"
)

// parseOutputFormat parses and validates the --output flag value.
func parseOutputFormat(s string) (outputFormat, error) {
	switch strings.ToLower(s) {
	case "table", "":
		return outputTable, nil
	case "json":
		return outputJSON, nil
	case "jsonl":
		return outputJSONL, nil
	default:
		return "", fmt.Errorf("unknown output format %q: valid options are table, json, jsonl", s)
	}
}

// writeJSON writes any value as pretty-printed JSON to stdout.
func writeJSON(v any) error {
	b, err := json.EncodeIndented(v, "", "  ", json.SortMapKeys)
	if err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}
	fmt.Println(string(b))
	return nil
}

// formatQueryResults writes query results in the given format to w.
func formatQueryResults(w io.Writer, res *antfly.QueryResponses, format outputFormat) error {
	switch format {
	case outputJSON:
		return formatJSON(w, res)
	case outputJSONL:
		return formatJSONL(w, res)
	default:
		return formatTable(w, res)
	}
}

// formatJSON writes the full response as pretty-printed JSON.
func formatJSON(w io.Writer, res *antfly.QueryResponses) error {
	b, err := json.EncodeIndented(res, "", "  ", json.SortMapKeys)
	if err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}
	_, err = fmt.Fprintln(w, string(b))
	return err
}

// formatJSONL writes one JSON object per hit (newline-delimited).
func formatJSONL(w io.Writer, res *antfly.QueryResponses) error {
	for _, resp := range res.Responses {
		for _, hit := range resp.Hits.Hits {
			b, err := json.Marshal(hit)
			if err != nil {
				return fmt.Errorf("encoding hit: %w", err)
			}
			if _, err := fmt.Fprintln(w, string(b)); err != nil {
				return err
			}
		}
	}
	return nil
}

// formatTable writes a human-readable table of hits.
func formatTable(w io.Writer, res *antfly.QueryResponses) error {
	for i, resp := range res.Responses {
		if resp.Error != "" {
			_, _ = fmt.Fprintf(w, "Error: %s\n", resp.Error)
			continue
		}

		total := resp.Hits.Total
		count := len(resp.Hits.Hits)
		if count == 0 {
			_, _ = fmt.Fprintln(w, "No results found.")
			continue
		}

		_, _ = fmt.Fprintf(w, "Found %d hit(s) (total: %d)\n\n", count, total)

		// Collect source field names across all hits (preserving order of first appearance).
		fieldOrder, fieldSet := collectSourceFields(resp.Hits.Hits)

		// Build columns: #, _id, _score, then source fields.
		numCol := col{header: "#"}
		idCol := col{header: "_id"}
		scoreCol := col{header: "_score"}
		srcCols := make([]col, len(fieldOrder))
		for j, f := range fieldOrder {
			srcCols[j] = col{header: f}
		}

		// Populate rows.
		for row, hit := range resp.Hits.Hits {
			numCol.values = append(numCol.values, fmt.Sprintf("%d", row+1))
			idCol.values = append(idCol.values, hit.ID)
			scoreCol.values = append(scoreCol.values, fmt.Sprintf("%.3f", hit.Score))
			for j, f := range fieldOrder {
				val := formatSourceValue(hit.Source[f])
				srcCols[j].values = append(srcCols[j].values, val)
			}
		}

		// Compute column widths.
		computeWidth := func(c *col, maxWidth int) {
			c.width = utf8.RuneCountInString(c.header)
			for _, v := range c.values {
				n := utf8.RuneCountInString(v)
				if n > c.width {
					c.width = n
				}
			}
			if maxWidth > 0 && c.width > maxWidth {
				c.width = maxWidth
			}
		}

		computeWidth(&numCol, 0)
		computeWidth(&idCol, 30)
		computeWidth(&scoreCol, 0)
		for j := range srcCols {
			computeWidth(&srcCols[j], 40)
		}

		// Only show source columns if they exist.
		_ = fieldSet

		// Print header.
		printRow(w, &numCol, &idCol, &scoreCol, srcCols, -1)

		// Print separator.
		printSep(w, &numCol, &idCol, &scoreCol, srcCols)

		// Print data rows.
		for row := range resp.Hits.Hits {
			printRow(w, &numCol, &idCol, &scoreCol, srcCols, row)
		}

		_, _ = fmt.Fprintln(w)

		// Print aggregations if present.
		if len(resp.Aggregations) > 0 {
			_, _ = fmt.Fprintln(w, "Aggregations:")
			b, err := json.EncodeIndented(resp.Aggregations, "  ", "  ", json.SortMapKeys)
			if err == nil {
				_, _ = fmt.Fprintf(w, "  %s\n", string(b))
			}
		}

		if i < len(res.Responses)-1 {
			_, _ = fmt.Fprintln(w, "---")
		}
	}
	return nil
}

type col struct {
	header string
	width  int
	values []string
}

// printRow prints a single table row (header if row == -1).
func printRow(w io.Writer, num, id, score *col, src []col, row int) {
	pad := func(c *col, idx int) string {
		var v string
		if idx < 0 {
			v = c.header
		} else {
			v = c.values[idx]
		}
		return truncPad(v, c.width)
	}

	_, _ = fmt.Fprintf(w, "  %s  %s  %s", pad(num, row), pad(id, row), pad(score, row))
	for j := range src {
		_, _ = fmt.Fprintf(w, "  %s", pad(&src[j], row))
	}
	_, _ = fmt.Fprintln(w)
}

// printSep prints the separator line under the header.
func printSep(w io.Writer, num, id, score *col, src []col) {
	dash := func(c *col) string { return strings.Repeat("-", c.width) }
	_, _ = fmt.Fprintf(w, "  %s  %s  %s", dash(num), dash(id), dash(score))
	for j := range src {
		_, _ = fmt.Fprintf(w, "  %s", dash(&src[j]))
	}
	_, _ = fmt.Fprintln(w)
}

// truncPad right-pads s to width, truncating with "…" if too long.
func truncPad(s string, width int) string {
	n := utf8.RuneCountInString(s)
	if n > width && width > 1 {
		// Truncate to width-1 runes + "…"
		runes := []rune(s)
		s = string(runes[:width-1]) + "…"
		n = width
	}
	if n < width {
		s += strings.Repeat(" ", width-n)
	}
	return s
}

// collectSourceFields gathers field names from all hits in order of first appearance.
func collectSourceFields(hits []antfly.Hit) ([]string, map[string]bool) {
	seen := map[string]bool{}
	var order []string
	for _, hit := range hits {
		for k := range hit.Source {
			if !seen[k] {
				seen[k] = true
				order = append(order, k)
			}
		}
	}
	return order, seen
}

// formatSourceValue converts an arbitrary source field value to a display string.
func formatSourceValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		// Collapse whitespace and limit length for display
		s := strings.Join(strings.Fields(val), " ")
		return s
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%.4g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case []any:
		return fmt.Sprintf("[%d items]", len(val))
	case map[string]any:
		return fmt.Sprintf("{%d keys}", len(val))
	default:
		return fmt.Sprintf("%v", val)
	}
}
