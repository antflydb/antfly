package oapi

import "testing"

func TestCatalogPaginationOptionalQueryParameters(t *testing.T) {
	unpaged, err := NewListTablesRequest("http://localhost", &ListTablesParams{Prefix: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if unpaged.URL.Query().Has("limit") || unpaged.URL.Query().Has("cursor") {
		t.Fatalf("unpaged request unexpectedly opts into pagination: %s", unpaged.URL)
	}
	limit := int32(25)
	cursor := "opaque-token"
	paged, err := NewListNamespaceTablesRequest("http://localhost", "tenant", "public", &ListNamespaceTablesParams{Limit: &limit, Cursor: &cursor})
	if err != nil {
		t.Fatal(err)
	}
	if paged.URL.Query().Get("limit") != "25" || paged.URL.Query().Get("cursor") != cursor {
		t.Fatalf("pagination parameters lost: %s", paged.URL)
	}
}
