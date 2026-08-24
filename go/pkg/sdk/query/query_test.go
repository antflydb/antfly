package query

import "testing"

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
