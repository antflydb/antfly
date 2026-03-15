package logging

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var bufferPool = buffer.NewPool()

// flattenValue converts a value to dot-notation key=value pairs.
// This is the industry standard for representing nested structs in logfmt.
// Example: config.raft.url=http://localhost config.raft.replicas=3
func flattenValue(val any) string {
	var pairs []string
	flatten("", reflect.ValueOf(val), &pairs)
	return strings.Join(pairs, " ")
}

// flatten recursively walks a value and builds dot-notation key=value pairs.
// Skips zero values to keep output concise.
func flatten(prefix string, v reflect.Value, pairs *[]string) {
	// Handle pointers and interfaces
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		// Special handling for time.Time
		if v.Type() == reflect.TypeFor[time.Time]() {
			t := v.Interface().(time.Time)
			if !t.IsZero() {
				*pairs = append(*pairs, formatPair(prefix, t.Format(time.RFC3339)))
			}
			return
		}
		// Recurse into struct fields
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}
			fieldVal := v.Field(i)
			key := field.Name
			if prefix != "" {
				key = prefix + "." + key
			}
			flatten(key, fieldVal, pairs)
		}

	case reflect.Map:
		if v.Len() == 0 {
			return
		}
		iter := v.MapRange()
		for iter.Next() {
			k := fmt.Sprintf("%v", iter.Key().Interface())
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			flatten(key, iter.Value(), pairs)
		}

	case reflect.Slice, reflect.Array:
		if v.Len() == 0 {
			return
		}
		// For simple types, join as comma-separated
		if isSimpleSlice(v) {
			var items []string
			for i := 0; i < v.Len(); i++ {
				items = append(items, fmt.Sprintf("%v", v.Index(i).Interface()))
			}
			*pairs = append(*pairs, formatPair(prefix, "["+strings.Join(items, ",")+"]"))
			return
		}
		// For complex types, flatten each element with index
		for i := 0; i < v.Len(); i++ {
			key := fmt.Sprintf("%s[%d]", prefix, i)
			flatten(key, v.Index(i), pairs)
		}

	case reflect.String:
		s := v.String()
		if s != "" {
			*pairs = append(*pairs, formatPair(prefix, s))
		}

	case reflect.Bool:
		if v.Bool() {
			*pairs = append(*pairs, formatPair(prefix, "true"))
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.Int() != 0 {
			*pairs = append(*pairs, formatPair(prefix, strconv.FormatInt(v.Int(), 10)))
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.Uint() != 0 {
			*pairs = append(*pairs, formatPair(prefix, strconv.FormatUint(v.Uint(), 10)))
		}

	case reflect.Float32, reflect.Float64:
		if v.Float() != 0 {
			*pairs = append(*pairs, formatPair(prefix, strconv.FormatFloat(v.Float(), 'f', -1, 64)))
		}

	default:
		// Fallback for other types
		s := fmt.Sprintf("%v", v.Interface())
		if s != "" && s != "<nil>" {
			*pairs = append(*pairs, formatPair(prefix, s))
		}
	}
}

// isSimpleSlice returns true if the slice contains simple (non-struct) types.
func isSimpleSlice(v reflect.Value) bool {
	if v.Len() == 0 {
		return true
	}
	elem := v.Index(0)
	for elem.Kind() == reflect.Ptr || elem.Kind() == reflect.Interface {
		if elem.IsNil() {
			return true
		}
		elem = elem.Elem()
	}
	switch elem.Kind() {
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
		return false
	default:
		return true
	}
}

// formatPair formats a key=value pair, quoting the value if needed.
func formatPair(key, value string) string {
	if strings.ContainsAny(value, " \t\n\r\"=") {
		// Quote and escape
		value = `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
	}
	return key + "=" + value
}

// logfmtEncoder implements zapcore.Encoder for logfmt output.
// Output format: ts=15:04:05 lvl=info caller=file.go:42 msg="message" key=value
type logfmtEncoder struct {
	*zapcore.EncoderConfig
	buf *buffer.Buffer
}

// NewLogfmtEncoder creates a new logfmt encoder.
func NewLogfmtEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder {
	return &logfmtEncoder{
		EncoderConfig: &cfg,
		buf:           bufferPool.Get(),
	}
}

func (e *logfmtEncoder) Clone() zapcore.Encoder {
	clone := &logfmtEncoder{
		EncoderConfig: e.EncoderConfig,
		buf:           bufferPool.Get(),
	}
	_, _ = clone.buf.Write(e.buf.Bytes())
	return clone
}

func (e *logfmtEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := bufferPool.Get()

	// Time
	if e.TimeKey != "" {
		e.appendKey(final, e.TimeKey)
		final.AppendString(ent.Time.Format("15:04:05"))
	}

	// Level
	if e.LevelKey != "" {
		e.appendKey(final, e.LevelKey)
		final.AppendString(strings.ToLower(ent.Level.String()))
	}

	// Caller
	if e.CallerKey != "" && ent.Caller.Defined {
		e.appendKey(final, e.CallerKey)
		final.AppendString(ent.Caller.TrimmedPath())
	}

	// Message
	if e.MessageKey != "" {
		e.appendKey(final, e.MessageKey)
		e.appendString(final, ent.Message)
	}

	// Pre-encoded fields from context
	if e.buf.Len() > 0 {
		final.AppendByte(' ')
		_, _ = final.Write(e.buf.Bytes())
	}

	// Encode additional fields
	for _, f := range fields {
		e.appendField(final, f)
	}

	final.AppendString(e.LineEnding)
	return final, nil
}

func (e *logfmtEncoder) appendKey(buf *buffer.Buffer, key string) {
	if buf.Len() > 0 {
		buf.AppendByte(' ')
	}
	buf.AppendString(key)
	buf.AppendByte('=')
}

func (e *logfmtEncoder) appendString(buf *buffer.Buffer, s string) {
	needsQuote := strings.ContainsAny(s, " \t\n\r\"=")
	if needsQuote {
		buf.AppendByte('"')
		for _, r := range s {
			switch r {
			case '"':
				buf.AppendString(`\"`)
			case '\\':
				buf.AppendString(`\\`)
			case '\n':
				buf.AppendString(`\n`)
			case '\r':
				buf.AppendString(`\r`)
			case '\t':
				buf.AppendString(`\t`)
			default:
				buf.AppendString(string(r))
			}
		}
		buf.AppendByte('"')
	} else {
		buf.AppendString(s)
	}
}

func (e *logfmtEncoder) appendField(buf *buffer.Buffer, f zapcore.Field) {
	switch f.Type {
	case zapcore.StringType:
		e.appendKey(buf, f.Key)
		e.appendString(buf, f.String)
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type:
		e.appendKey(buf, f.Key)
		buf.AppendString(strconv.FormatInt(f.Integer, 10))
	case zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type:
		e.appendKey(buf, f.Key)
		buf.AppendString(strconv.FormatUint(uint64(f.Integer), 10)) //nolint:gosec // zap stores uint values in int64 field
	case zapcore.Float64Type:
		e.appendKey(buf, f.Key)
		buf.AppendString(strconv.FormatFloat(math.Float64frombits(uint64(f.Integer)), 'f', -1, 64)) //nolint:gosec // zap stores float64 bits in int64 field
	case zapcore.Float32Type:
		e.appendKey(buf, f.Key)
		buf.AppendString(strconv.FormatFloat(float64(math.Float32frombits(uint32(f.Integer))), 'f', -1, 32)) //nolint:gosec // zap stores float32 bits in int64 field
	case zapcore.BoolType:
		e.appendKey(buf, f.Key)
		if f.Integer == 1 {
			buf.AppendString("true")
		} else {
			buf.AppendString("false")
		}
	case zapcore.TimeType:
		e.appendKey(buf, f.Key)
		if f.Interface != nil {
			buf.AppendString(time.Unix(0, f.Integer).In(f.Interface.(*time.Location)).Format(time.RFC3339))
		} else {
			buf.AppendString(time.Unix(0, f.Integer).Format(time.RFC3339))
		}
	case zapcore.DurationType:
		e.appendKey(buf, f.Key)
		buf.AppendString(time.Duration(f.Integer).String())
	case zapcore.ErrorType:
		if err, ok := f.Interface.(error); ok {
			e.appendKey(buf, f.Key)
			e.appendString(buf, err.Error())
		}
	case zapcore.StringerType:
		e.appendKey(buf, f.Key)
		e.appendString(buf, f.Interface.(fmt.Stringer).String())
	default:
		// For complex types, flatten to dot-notation key=value pairs
		if f.Interface != nil {
			flattened := flattenValue(f.Interface)
			if flattened != "" {
				// Prefix each flattened key with the field key
				parts := strings.SplitSeq(flattened, " ")
				for part := range parts {
					if part == "" {
						continue
					}
					// part is "Key=value", prepend field name
					buf.AppendByte(' ')
					buf.AppendString(f.Key)
					buf.AppendByte('.')
					buf.AppendString(part)
				}
			}
		}
	}
}

// Implement ObjectEncoder interface for adding fields
func (e *logfmtEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	flattened := flattenValue(arr)
	if flattened != "" {
		for part := range strings.SplitSeq(flattened, " ") {
			if part == "" {
				continue
			}
			e.buf.AppendByte(' ')
			e.buf.AppendString(key)
			e.buf.AppendByte('.')
			e.buf.AppendString(part)
		}
	}
	return nil
}

func (e *logfmtEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	flattened := flattenValue(obj)
	if flattened != "" {
		for part := range strings.SplitSeq(flattened, " ") {
			if part == "" {
				continue
			}
			e.buf.AppendByte(' ')
			e.buf.AppendString(key)
			e.buf.AppendByte('.')
			e.buf.AppendString(part)
		}
	}
	return nil
}

func (e *logfmtEncoder) AddBinary(key string, val []byte) {
	e.appendKey(e.buf, key)
	e.appendString(e.buf, string(val))
}

func (e *logfmtEncoder) AddByteString(key string, val []byte) {
	e.appendKey(e.buf, key)
	e.appendString(e.buf, string(val))
}

func (e *logfmtEncoder) AddBool(key string, val bool) {
	e.appendKey(e.buf, key)
	if val {
		e.buf.AppendString("true")
	} else {
		e.buf.AppendString("false")
	}
}

func (e *logfmtEncoder) AddComplex128(key string, val complex128) {
	e.appendKey(e.buf, key)
	e.appendString(e.buf, fmt.Sprintf("%v", val))
}

func (e *logfmtEncoder) AddComplex64(key string, val complex64) {
	e.appendKey(e.buf, key)
	e.appendString(e.buf, fmt.Sprintf("%v", val))
}

func (e *logfmtEncoder) AddDuration(key string, val time.Duration) {
	e.appendKey(e.buf, key)
	e.buf.AppendString(val.String())
}

func (e *logfmtEncoder) AddFloat64(key string, val float64) {
	e.appendKey(e.buf, key)
	e.buf.AppendString(strconv.FormatFloat(val, 'f', -1, 64))
}

func (e *logfmtEncoder) AddFloat32(key string, val float32) {
	e.appendKey(e.buf, key)
	e.buf.AppendString(strconv.FormatFloat(float64(val), 'f', -1, 32))
}

func (e *logfmtEncoder) AddInt(key string, val int) {
	e.appendKey(e.buf, key)
	e.buf.AppendString(strconv.Itoa(val))
}

func (e *logfmtEncoder) AddInt64(key string, val int64) {
	e.appendKey(e.buf, key)
	e.buf.AppendString(strconv.FormatInt(val, 10))
}

func (e *logfmtEncoder) AddInt32(key string, val int32) {
	e.AddInt64(key, int64(val))
}

func (e *logfmtEncoder) AddInt16(key string, val int16) {
	e.AddInt64(key, int64(val))
}

func (e *logfmtEncoder) AddInt8(key string, val int8) {
	e.AddInt64(key, int64(val))
}

func (e *logfmtEncoder) AddString(key, val string) {
	e.appendKey(e.buf, key)
	e.appendString(e.buf, val)
}

func (e *logfmtEncoder) AddTime(key string, val time.Time) {
	e.appendKey(e.buf, key)
	e.buf.AppendString(val.Format(time.RFC3339))
}

func (e *logfmtEncoder) AddUint(key string, val uint) {
	e.AddUint64(key, uint64(val))
}

func (e *logfmtEncoder) AddUint64(key string, val uint64) {
	e.appendKey(e.buf, key)
	e.buf.AppendString(strconv.FormatUint(val, 10))
}

func (e *logfmtEncoder) AddUint32(key string, val uint32) {
	e.AddUint64(key, uint64(val))
}

func (e *logfmtEncoder) AddUint16(key string, val uint16) {
	e.AddUint64(key, uint64(val))
}

func (e *logfmtEncoder) AddUint8(key string, val uint8) {
	e.AddUint64(key, uint64(val))
}

func (e *logfmtEncoder) AddUintptr(key string, val uintptr) {
	e.AddUint64(key, uint64(val))
}

func (e *logfmtEncoder) AddReflected(key string, val any) error {
	flattened := flattenValue(val)
	if flattened != "" {
		for part := range strings.SplitSeq(flattened, " ") {
			if part == "" {
				continue
			}
			e.buf.AppendByte(' ')
			e.buf.AppendString(key)
			e.buf.AppendByte('.')
			e.buf.AppendString(part)
		}
	}
	return nil
}

func (e *logfmtEncoder) OpenNamespace(key string) {
	// For logfmt, namespaces are represented as key prefixes
	// This is a simplified implementation - nested fields will use dot notation
	e.appendKey(e.buf, key)
	e.buf.AppendString("{")
}

var _ zapcore.Encoder = (*logfmtEncoder)(nil)
