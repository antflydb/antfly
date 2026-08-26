package query

import (
	"testing"
	"time"
)

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

func TestDateRangeStringQueryNormalizesBeforeUnionSerialization(t *testing.T) {
	start := time.Date(2300, time.January, 1, 0, 0, 0, 0, time.FixedZone("east-seconds", 30))
	end := time.Date(2300, time.January, 2, 0, 0, 0, 0, time.FixedZone("west-seconds", -45))

	decoded, err := (DateRangeStringQuery{
		Field: "created_at",
		Start: &start,
		End:   &end,
	}.ToQuery()).AsDateRangeStringQuery()
	if err != nil {
		t.Fatal(err)
	}

	wantStart := time.Date(2299, time.December, 31, 23, 59, 30, 0, time.UTC)
	wantEnd := time.Date(2300, time.January, 2, 0, 0, 45, 0, time.UTC)
	if decoded.Start == nil || !decoded.Start.Equal(wantStart) || decoded.Start.Location() != time.UTC {
		t.Fatalf("start = %v, want %v in UTC", decoded.Start, wantStart)
	}
	if decoded.End == nil || !decoded.End.Equal(wantEnd) || decoded.End.Location() != time.UTC {
		t.Fatalf("end = %v, want %v in UTC", decoded.End, wantEnd)
	}
}
