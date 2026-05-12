package structured

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Format identifies the source document format for a mapping.
type Format string

const (
	FormatJSON   Format = "json"
	FormatYAML   Format = "yaml"
	FormatXML    Format = "xml"
	FormatCSV    Format = "csv"
	FormatNDJSON Format = "ndjson"
)

// FieldType controls optional value coercion for mapped fields.
type FieldType string

const (
	FieldTypeString FieldType = ""
	FieldTypeBool   FieldType = "bool"
	FieldTypeInt    FieldType = "int"
	FieldTypeFloat  FieldType = "float"
	FieldTypeRaw    FieldType = "raw"
)

// MappingConfig describes how to convert structured input records into Antfly-ready documents.
type MappingConfig struct {
	Format         Format                  `json:"format" yaml:"format"`
	RecordSelector string                  `json:"record_selector" yaml:"record_selector"`
	Fields         map[string]FieldMapping `json:"fields" yaml:"fields"`
	Options        MappingOptions          `json:"options,omitempty" yaml:"options,omitempty"`
}

// MappingOptions contains format-specific mapping controls.
type MappingOptions struct {
	CSVHasHeader bool   `json:"csv_has_header,omitempty" yaml:"csv_has_header,omitempty"`
	CSVDelimiter string `json:"csv_delimiter,omitempty" yaml:"csv_delimiter,omitempty"`
	IDField      string `json:"id_field,omitempty" yaml:"id_field,omitempty"`
}

// FieldMapping describes how to extract one output field.
type FieldMapping struct {
	Path     string    `json:"path,omitempty" yaml:"path,omitempty"`
	Paths    []string  `json:"paths,omitempty" yaml:"paths,omitempty"`
	Fallback []string  `json:"fallback,omitempty" yaml:"fallback,omitempty"`
	Multiple bool      `json:"multiple,omitempty" yaml:"multiple,omitempty"`
	Required bool      `json:"required,omitempty" yaml:"required,omitempty"`
	Join     string    `json:"join,omitempty" yaml:"join,omitempty"`
	Type     FieldType `json:"type,omitempty" yaml:"type,omitempty"`
	Default  any       `json:"default,omitempty" yaml:"default,omitempty"`
}

// Record is one mapped source record.
type Record struct {
	ID       string         `json:"id"`
	Document map[string]any `json:"document"`
}

// DiagnosticLevel describes diagnostic severity.
type DiagnosticLevel string

const (
	DiagnosticInfo    DiagnosticLevel = "info"
	DiagnosticWarning DiagnosticLevel = "warning"
	DiagnosticError   DiagnosticLevel = "error"
)

// MappingDiagnostic describes a mapping problem or noteworthy fallback.
type MappingDiagnostic struct {
	Level  DiagnosticLevel `json:"level"`
	Field  string          `json:"field,omitempty"`
	Record int             `json:"record,omitempty"`
	Path   string          `json:"path,omitempty"`
	Code   string          `json:"code"`
	Detail string          `json:"detail"`
}

// Preview is a bounded mapping result with diagnostics for UI inspection.
type Preview struct {
	Records      []Record            `json:"records"`
	Diagnostics  []MappingDiagnostic `json:"diagnostics"`
	TotalRecords int                 `json:"total_records"`
	Skipped      int                 `json:"skipped"`
}

// RecordMapper maps structured inputs into records.
type RecordMapper interface {
	Validate(MappingConfig) error
	Preview(context.Context, []byte, MappingConfig, int) (*Preview, error)
	Map(context.Context, []byte, MappingConfig) ([]Record, error)
}

// Mapper is the default RecordMapper implementation.
type Mapper struct{}

// NewMapper creates a structured record mapper.
func NewMapper() *Mapper {
	return &Mapper{}
}

// Validate validates a mapping config.
func (m *Mapper) Validate(config MappingConfig) error {
	if !slices.Contains([]Format{FormatJSON, FormatYAML, FormatXML, FormatCSV, FormatNDJSON}, config.Format) {
		return fmt.Errorf("unsupported format %q", config.Format)
	}
	if strings.TrimSpace(config.RecordSelector) == "" && config.Format != FormatCSV && config.Format != FormatNDJSON {
		return errors.New("record_selector is required")
	}
	if len(config.Fields) == 0 {
		return errors.New("at least one field mapping is required")
	}
	for name, field := range config.Fields {
		if strings.TrimSpace(name) == "" {
			return errors.New("field names cannot be empty")
		}
		if strings.Contains(name, ".") {
			return fmt.Errorf("field %q cannot contain dots", name)
		}
		if len(field.selectorCandidates()) == 0 && field.Default == nil {
			return fmt.Errorf("field %q must define path, paths, fallback, or default", name)
		}
	}
	return nil
}

// Map maps all records from input.
func (m *Mapper) Map(ctx context.Context, input []byte, config MappingConfig) ([]Record, error) {
	result, err := m.mapWithDiagnostics(ctx, input, config, 0)
	if err != nil {
		return nil, err
	}
	return result.Records, nil
}

// Preview maps records up to limit and returns diagnostics.
func (m *Mapper) Preview(ctx context.Context, input []byte, config MappingConfig, limit int) (*Preview, error) {
	if limit <= 0 {
		limit = 10
	}
	return m.mapWithDiagnostics(ctx, input, config, limit)
}

func (m *Mapper) mapWithDiagnostics(ctx context.Context, input []byte, config MappingConfig, limit int) (*Preview, error) {
	if err := m.Validate(config); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	source, err := parseSource(input, config)
	if err != nil {
		return nil, err
	}
	recordNodes, err := source.selectRecords(config.RecordSelector)
	if err != nil {
		return nil, err
	}

	preview := &Preview{TotalRecords: len(recordNodes)}
	for i, node := range recordNodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, diagnostics, skip := mapRecord(source, node, i+1, config)
		preview.Diagnostics = append(preview.Diagnostics, diagnostics...)
		if skip {
			preview.Skipped++
			continue
		}
		if limit == 0 || len(preview.Records) < limit {
			preview.Records = append(preview.Records, record)
		}
	}

	return preview, nil
}

func mapRecord(source parsedSource, node any, ordinal int, config MappingConfig) (Record, []MappingDiagnostic, bool) {
	doc := make(map[string]any, len(config.Fields))
	var diagnostics []MappingDiagnostic
	skip := false

	fieldNames := make([]string, 0, len(config.Fields))
	for name := range config.Fields {
		fieldNames = append(fieldNames, name)
	}
	slices.Sort(fieldNames)

	for _, name := range fieldNames {
		mapping := config.Fields[name]
		value, found, usedPath, err := extractFieldValue(source, node, mapping)
		if err != nil {
			diagnostics = append(diagnostics, MappingDiagnostic{
				Level:  DiagnosticError,
				Field:  name,
				Record: ordinal,
				Path:   usedPath,
				Code:   "coercion_failed",
				Detail: err.Error(),
			})
			if mapping.Required {
				skip = true
			}
			continue
		}
		if !found {
			if mapping.Default != nil {
				value = mapping.Default
				found = true
				diagnostics = append(diagnostics, MappingDiagnostic{
					Level:  DiagnosticInfo,
					Field:  name,
					Record: ordinal,
					Code:   "default_used",
					Detail: "field default was used",
				})
			} else if mapping.Required {
				diagnostics = append(diagnostics, MappingDiagnostic{
					Level:  DiagnosticError,
					Field:  name,
					Record: ordinal,
					Code:   "required_missing",
					Detail: "required field did not match any value",
				})
				skip = true
			}
		}
		if found {
			doc[name] = value
		}
	}

	idField := config.Options.IDField
	if idField == "" {
		idField = "id"
	}
	id, ok := stringify(doc[idField])
	if !ok || strings.TrimSpace(id) == "" {
		id = fmt.Sprintf("record-%d", ordinal)
		doc[idField] = id
		diagnostics = append(diagnostics, MappingDiagnostic{
			Level:  DiagnosticWarning,
			Field:  idField,
			Record: ordinal,
			Code:   "generated_id",
			Detail: "record ID was generated because no usable ID field was mapped",
		})
	}

	return Record{ID: id, Document: doc}, diagnostics, skip
}

func extractFieldValue(source parsedSource, node any, mapping FieldMapping) (any, bool, string, error) {
	if len(mapping.Paths) > 0 {
		values, usedPath, err := extractPathGroup(source, node, mapping.Paths, mapping.Type)
		if err != nil {
			return nil, false, usedPath, err
		}
		values = compactEmpty(values)
		if len(values) > 0 {
			if mapping.Join != "" {
				return joinValues(values, mapping.Join), true, usedPath, nil
			}
			return values, true, usedPath, nil
		}
	}

	var alternatives []string
	if mapping.Path != "" {
		alternatives = append(alternatives, mapping.Path)
	}
	alternatives = append(alternatives, mapping.Fallback...)
	for _, selector := range alternatives {
		values, err := source.selectValues(node, selector)
		if err != nil {
			return nil, false, selector, err
		}
		values = compactEmpty(values)
		if len(values) == 0 {
			continue
		}

		if mapping.Multiple || len(mapping.Paths) > 0 {
			coerced := make([]any, 0, len(values))
			for _, value := range values {
				v, err := coerceValue(value, mapping.Type)
				if err != nil {
					return nil, false, selector, err
				}
				coerced = append(coerced, v)
			}
			if mapping.Join != "" {
				return joinValues(coerced, mapping.Join), true, selector, nil
			}
			return coerced, true, selector, nil
		}

		value, err := coerceValue(values[0], mapping.Type)
		if err != nil {
			return nil, false, selector, err
		}
		return value, true, selector, nil
	}
	return nil, false, "", nil
}

func extractPathGroup(source parsedSource, node any, paths []string, typ FieldType) ([]any, string, error) {
	var out []any
	var used []string
	for _, selector := range paths {
		values, err := source.selectValues(node, selector)
		if err != nil {
			return nil, selector, err
		}
		values = compactEmpty(values)
		if len(values) == 0 {
			continue
		}
		used = append(used, selector)
		for _, value := range values {
			coerced, err := coerceValue(value, typ)
			if err != nil {
				return nil, selector, err
			}
			out = append(out, coerced)
		}
	}
	return out, strings.Join(used, ","), nil
}

func (m FieldMapping) selectorCandidates() []string {
	var selectors []string
	selectors = append(selectors, m.Paths...)
	if m.Path != "" {
		selectors = append(selectors, m.Path)
	}
	selectors = append(selectors, m.Fallback...)
	return selectors
}

func parseSource(input []byte, config MappingConfig) (parsedSource, error) {
	switch config.Format {
	case FormatJSON:
		var value any
		if err := json.Unmarshal(input, &value); err != nil {
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
		return objectSource{root: value}, nil
	case FormatYAML:
		var value any
		if err := yaml.Unmarshal(input, &value); err != nil {
			return nil, fmt.Errorf("parse YAML: %w", err)
		}
		return objectSource{root: normalizeYAML(value)}, nil
	case FormatXML:
		root, err := parseXML(input)
		if err != nil {
			return nil, err
		}
		return xmlSource{root: root}, nil
	case FormatCSV:
		rows, err := parseCSV(input, config.Options)
		if err != nil {
			return nil, err
		}
		return csvSource{rows: rows}, nil
	case FormatNDJSON:
		rows, err := parseNDJSON(input)
		if err != nil {
			return nil, err
		}
		return csvSource{rows: rows}, nil
	default:
		return nil, fmt.Errorf("unsupported format %q", config.Format)
	}
}

type parsedSource interface {
	selectRecords(selector string) ([]any, error)
	selectValues(record any, selector string) ([]any, error)
}

type objectSource struct {
	root any
}

func (s objectSource) selectRecords(selector string) ([]any, error) {
	values, err := selectObjectPath(s.root, selector)
	if err != nil {
		return nil, err
	}
	return flattenRecordValues(values), nil
}

func (s objectSource) selectValues(record any, selector string) ([]any, error) {
	root := record
	if strings.HasPrefix(selector, "$") {
		root = s.root
	}
	return selectObjectPath(root, selector)
}

func selectObjectPath(root any, selector string) ([]any, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" || selector == "." || selector == "$" {
		return []any{root}, nil
	}
	selector = strings.TrimPrefix(selector, "$.")
	selector = strings.TrimPrefix(selector, "$")
	selector = strings.TrimPrefix(selector, "./")
	selector = strings.TrimPrefix(selector, ".")
	if selector == "" {
		return []any{root}, nil
	}

	current := []any{root}
	for _, part := range strings.Split(selector, ".") {
		if part == "" {
			continue
		}
		name := part
		wantAll := false
		if strings.HasSuffix(part, "[*]") {
			wantAll = true
			name = strings.TrimSuffix(part, "[*]")
		}

		var next []any
		for _, item := range current {
			value, ok := objectChild(item, name)
			if !ok {
				continue
			}
			if wantAll {
				if arr, ok := value.([]any); ok {
					next = append(next, arr...)
				}
				continue
			}
			next = append(next, value)
		}
		current = next
	}
	return current, nil
}

func objectChild(value any, name string) (any, bool) {
	if name == "" {
		return value, true
	}
	switch typed := value.(type) {
	case map[string]any:
		v, ok := typed[name]
		return v, ok
	case []any:
		idx, err := strconv.Atoi(name)
		if err != nil || idx < 0 || idx >= len(typed) {
			return nil, false
		}
		return typed[idx], true
	default:
		return nil, false
	}
}

func flattenRecordValues(values []any) []any {
	var records []any
	for _, value := range values {
		if arr, ok := value.([]any); ok {
			records = append(records, arr...)
			continue
		}
		records = append(records, value)
	}
	return records
}

type xmlSource struct {
	root *xmlNode
}

type xmlNode struct {
	name     xml.Name
	attrs    []xml.Attr
	text     string
	children []*xmlNode
	parent   *xmlNode
}

func parseXML(input []byte) (*xmlNode, error) {
	decoder := xml.NewDecoder(bytes.NewReader(input))
	var stack []*xmlNode
	var root *xmlNode

	for {
		tok, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("parse XML: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			node := &xmlNode{name: t.Name, attrs: append([]xml.Attr(nil), t.Attr...)}
			if len(stack) > 0 {
				node.parent = stack[len(stack)-1]
				node.parent.children = append(node.parent.children, node)
			} else {
				root = node
			}
			stack = append(stack, node)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			if len(stack) > 0 {
				text := strings.TrimSpace(string(t))
				if text != "" {
					current := stack[len(stack)-1]
					if current.text != "" {
						current.text += " "
					}
					current.text += text
				}
			}
		}
	}
	if root == nil {
		return nil, errors.New("parse XML: document is empty")
	}
	return root, nil
}

func (s xmlSource) selectRecords(selector string) ([]any, error) {
	nodes := selectXMLPath([]*xmlNode{s.root}, selector)
	result := make([]any, len(nodes))
	for i, node := range nodes {
		result[i] = node
	}
	return result, nil
}

func (s xmlSource) selectValues(record any, selector string) ([]any, error) {
	node, ok := record.(*xmlNode)
	if !ok {
		return nil, fmt.Errorf("expected XML record node, got %T", record)
	}
	selected := selectXMLPath([]*xmlNode{node}, selector)
	values := make([]any, 0, len(selected))
	for _, item := range selected {
		values = append(values, item.textValue())
	}
	return values, nil
}

func selectXMLPath(nodes []*xmlNode, selector string) []*xmlNode {
	selector = strings.TrimSpace(selector)
	if selector == "" || selector == "." {
		return nodes
	}
	if strings.HasPrefix(selector, "./") {
		selector = strings.TrimPrefix(selector, "./")
	} else if strings.HasPrefix(selector, "/") {
		selector = strings.TrimPrefix(selector, "/")
		for len(nodes) > 0 && selector != "" {
			first, rest, _ := strings.Cut(selector, "/")
			if normalizeName(nodes[0].name.Local) == normalizeName(first) {
				selector = rest
				break
			}
			break
		}
	}

	current := nodes
	for _, part := range strings.Split(selector, "/") {
		if part == "" || part == "." {
			continue
		}
		var next []*xmlNode
		for _, node := range current {
			if strings.HasPrefix(part, "@") {
				attr := node.attr(strings.TrimPrefix(part, "@"))
				if attr != nil {
					next = append(next, attr)
				}
				continue
			}
			for _, child := range node.children {
				if part == "*" || normalizeName(child.name.Local) == normalizeName(part) {
					next = append(next, child)
				}
			}
		}
		current = next
	}
	return current
}

func (n *xmlNode) attr(name string) *xmlNode {
	for _, attr := range n.attrs {
		if normalizeName(attr.Name.Local) == normalizeName(name) {
			return &xmlNode{name: attr.Name, text: attr.Value}
		}
	}
	return nil
}

func (n *xmlNode) textValue() string {
	var parts []string
	n.collectText(&parts)
	return strings.Join(parts, "\n\n")
}

func (n *xmlNode) collectText(parts *[]string) {
	if n.text != "" {
		*parts = append(*parts, n.text)
	}
	for _, child := range n.children {
		child.collectText(parts)
	}
}

type csvSource struct {
	rows []map[string]any
}

func (s csvSource) selectRecords(string) ([]any, error) {
	records := make([]any, len(s.rows))
	for i := range s.rows {
		records[i] = s.rows[i]
	}
	return records, nil
}

func (s csvSource) selectValues(record any, selector string) ([]any, error) {
	return objectSource{root: s.rows}.selectValues(record, selector)
}

func parseCSV(input []byte, opts MappingOptions) ([]map[string]any, error) {
	reader := csv.NewReader(bytes.NewReader(input))
	if opts.CSVDelimiter != "" {
		runes := []rune(opts.CSVDelimiter)
		if len(runes) != 1 {
			return nil, errors.New("csv_delimiter must be one character")
		}
		reader.Comma = runes[0]
	}
	allRows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse CSV: %w", err)
	}
	if len(allRows) == 0 {
		return nil, nil
	}

	var headers []string
	start := 0
	if opts.CSVHasHeader {
		headers = allRows[0]
		start = 1
	} else {
		headers = make([]string, len(allRows[0]))
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i+1)
		}
	}

	rows := make([]map[string]any, 0, len(allRows)-start)
	for _, row := range allRows[start:] {
		item := make(map[string]any, len(headers))
		for i, header := range headers {
			if i < len(row) {
				item[header] = row[i]
			}
		}
		rows = append(rows, item)
	}
	return rows, nil
}

func parseNDJSON(input []byte) ([]map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	var rows []map[string]any
	for {
		var item map[string]any
		if err := decoder.Decode(&item); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("parse NDJSON: %w", err)
		}
		rows = append(rows, item)
	}
	return rows, nil
}

func compactEmpty(values []any) []any {
	var out []any
	for _, value := range values {
		if str, ok := stringify(value); ok && strings.TrimSpace(str) == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func coerceValue(value any, typ FieldType) (any, error) {
	if typ == FieldTypeRaw {
		return value, nil
	}
	str, ok := stringify(value)
	if !ok {
		return value, nil
	}
	switch typ {
	case FieldTypeString:
		return str, nil
	case FieldTypeBool:
		return strconv.ParseBool(str)
	case FieldTypeInt:
		return strconv.Atoi(str)
	case FieldTypeFloat:
		return strconv.ParseFloat(str, 64)
	default:
		return nil, fmt.Errorf("unsupported field type %q", typ)
	}
}

func stringify(value any) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "", false
	case string:
		return strings.TrimSpace(typed), true
	case json.Number:
		return typed.String(), true
	case bool:
		return strconv.FormatBool(typed), true
	case int:
		return strconv.Itoa(typed), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), true
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), true
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return "", false
		}
		return string(data), true
	}
}

func joinValues(values []any, separator string) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		if str, ok := stringify(value); ok {
			parts = append(parts, str)
		}
	}
	return strings.Join(parts, separator)
}

func normalizeYAML(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, v := range typed {
			out[k] = normalizeYAML(v)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for k, v := range typed {
			out[fmt.Sprint(k)] = normalizeYAML(v)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, v := range typed {
			out[i] = normalizeYAML(v)
		}
		return out
	default:
		return value
	}
}

func normalizeName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "-", "")
	name = strings.ReplaceAll(name, "_", "")
	return name
}
