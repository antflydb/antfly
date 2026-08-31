package query

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestQueryRetainedDecodeExtensions(t *testing.T) {
	query := TermQuery{Term: "published", Field: "status"}.ToQuery()
	var probe struct {
		Term  string `json:"term"`
		Field string `json:"field"`
	}
	if err := query.DecodeInto(&probe); err != nil {
		t.Fatal(err)
	}
	if probe.Term != "published" || probe.Field != "status" {
		t.Fatalf("probe = %#v", probe)
	}

	var strict TermQuery
	if err := query.DecodeStrictInto(&strict); err != nil {
		t.Fatal(err)
	}
	if strict.Term != "published" || strict.Field != "status" {
		t.Fatalf("strict = %#v", strict)
	}

	var malformed Query
	if err := json.Unmarshal([]byte(`{"term":"published","field":"status","unexpected":true}`), &malformed); err != nil {
		t.Fatal(err)
	}
	if err := malformed.DecodeStrictInto(&strict); err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("expected strict decode to reject unknown member, got %v", err)
	}
}

func TestNewDisjunctionDistinguishesOmittedAndExplicitZero(t *testing.T) {
	clauses := []Query{NewTerm("draft", "status"), NewTerm("pending", "status")}

	conventional := NewDisjunction(clauses)
	if conventional.Min != nil {
		t.Fatalf("conventional disjunction minimum = %v, want omitted", *conventional.Min)
	}

	optional := NewDisjunctionWithMinimum(clauses, 0)
	if optional.Min == nil || *optional.Min != 0 {
		t.Fatalf("explicit disjunction minimum = %v, want 0", optional.Min)
	}
}

func TestDateRangeStringQueryNormalizesEverySerializationPath(t *testing.T) {
	start := time.Date(2300, time.January, 1, 0, 0, 0, 0, time.FixedZone("east-seconds", 30))
	end := time.Date(2300, time.January, 2, 0, 0, 0, 0, time.FixedZone("west-seconds", -45))
	input := DateRangeStringQuery{Field: "created_at", Start: &start, End: &end}

	decode := func(encoded []byte) DateRangeStringQuery {
		t.Helper()
		var decoded DateRangeStringQuery
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	assertNormalized := func(name string, decoded DateRangeStringQuery) {
		t.Helper()
		wantStart := time.Date(2299, time.December, 31, 23, 59, 30, 0, time.UTC)
		wantEnd := time.Date(2300, time.January, 2, 0, 0, 45, 0, time.UTC)
		if decoded.Start == nil || !decoded.Start.Equal(wantStart) || decoded.Start.Location() != time.UTC {
			t.Fatalf("%s start = %v, want %v in UTC", name, decoded.Start, wantStart)
		}
		if decoded.End == nil || !decoded.End.Equal(wantEnd) || decoded.End.Location() != time.UTC {
			t.Fatalf("%s end = %v, want %v in UTC", name, decoded.End, wantEnd)
		}
	}

	direct, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	assertNormalized("json.Marshal", decode(direct))

	var from Query
	if err := from.FromDateRangeStringQuery(input); err != nil {
		t.Fatal(err)
	}
	fromDecoded, err := from.AsDateRangeStringQuery()
	if err != nil {
		t.Fatal(err)
	}
	assertNormalized("FromDateRangeStringQuery", fromDecoded)

	var merged Query
	if err := merged.MergeDateRangeStringQuery(input); err != nil {
		t.Fatal(err)
	}
	mergedDecoded, err := merged.AsDateRangeStringQuery()
	if err != nil {
		t.Fatal(err)
	}
	assertNormalized("MergeDateRangeStringQuery", mergedDecoded)

	toQueryDecoded, err := input.ToQuery().AsDateRangeStringQuery()
	if err != nil {
		t.Fatal(err)
	}
	assertNormalized("ToQuery", toQueryDecoded)

	if start.Location() == time.UTC || end.Location() == time.UTC {
		t.Fatal("serialization mutated caller-owned date bounds")
	}
}
