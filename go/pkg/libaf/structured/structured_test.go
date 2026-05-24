package structured

import (
	"context"
	"testing"
)

func TestMapperMapsXML(t *testing.T) {
	mapper := NewMapper()
	config := MappingConfig{
		Format:         FormatXML,
		RecordSelector: "/archive/article",
		Fields: map[string]FieldMapping{
			"id": {
				Path: "./@id",
			},
			"title": {
				Path: "./headline",
				Fallback: []string{
					"./title",
				},
				Required: true,
			},
			"body": {
				Paths: []string{"./body/p"},
				Join:  "\n\n",
			},
			"brand": {
				Path: "./brand",
			},
			"tags": {
				Path:     "./tags/tag",
				Multiple: true,
			},
			"published_at": {
				Path: "./published_at",
			},
		},
	}

	input := []byte(`<archive>
  <article id="a1">
    <headline>New resorts in Cancun</headline>
    <brand>Travel Weekly</brand>
    <published_at>2026-04-12</published_at>
    <body>
      <p>Luxury resorts are opening in Cancun.</p>
      <p>Several brands are expanding.</p>
    </body>
    <tags><tag>Mexico</tag><tag>Luxury</tag></tags>
  </article>
</archive>`)

	records, err := mapper.Map(context.Background(), input, config)
	if err != nil {
		t.Fatalf("Map returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	record := records[0]
	if record.ID != "a1" {
		t.Fatalf("record.ID = %q", record.ID)
	}
	if got := record.Document["title"]; got != "New resorts in Cancun" {
		t.Fatalf("title = %#v", got)
	}
	if got := record.Document["body"]; got != "Luxury resorts are opening in Cancun.\n\nSeveral brands are expanding." {
		t.Fatalf("body = %#v", got)
	}
	tags, ok := record.Document["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "Mexico" || tags[1] != "Luxury" {
		t.Fatalf("tags = %#v", record.Document["tags"])
	}
}

func TestMapperMapsJSON(t *testing.T) {
	mapper := NewMapper()
	config := MappingConfig{
		Format:         FormatJSON,
		RecordSelector: "$.articles[*]",
		Fields: map[string]FieldMapping{
			"id":    {Path: "./id"},
			"title": {Path: "./headline"},
			"body":  {Path: "./body"},
			"tags": {
				Path:     "./tags[*]",
				Multiple: true,
			},
		},
	}
	input := []byte(`{"articles":[{"id":"a1","headline":"Airline trends","body":"Routes are changing.","tags":["air","business"]}]}`)

	preview, err := mapper.Preview(context.Background(), input, config, 10)
	if err != nil {
		t.Fatalf("Preview returned error: %v", err)
	}
	if preview.TotalRecords != 1 || len(preview.Records) != 1 {
		t.Fatalf("preview = %#v", preview)
	}
	if preview.Records[0].Document["title"] != "Airline trends" {
		t.Fatalf("title = %#v", preview.Records[0].Document["title"])
	}
}

func TestMapperMapsYAML(t *testing.T) {
	mapper := NewMapper()
	config := MappingConfig{
		Format:         FormatYAML,
		RecordSelector: "$.articles[*]",
		Fields: map[string]FieldMapping{
			"id":    {Path: "./id"},
			"title": {Path: "./title"},
			"views": {
				Path: "./views",
				Type: FieldTypeInt,
			},
		},
	}
	input := []byte(`articles:
  - id: a1
    title: Meetings rebound
    views: "42"
`)

	records, err := mapper.Map(context.Background(), input, config)
	if err != nil {
		t.Fatalf("Map returned error: %v", err)
	}
	if got := records[0].Document["views"]; got != 42 {
		t.Fatalf("views = %#v", got)
	}
}

func TestMapperDiagnosticsAndGeneratedID(t *testing.T) {
	mapper := NewMapper()
	config := MappingConfig{
		Format:         FormatJSON,
		RecordSelector: "$.articles[*]",
		Fields: map[string]FieldMapping{
			"title": {Path: "./headline", Required: true},
			"body":  {Path: "./body"},
		},
	}
	input := []byte(`{"articles":[{"headline":"No ID","body":"Generated ID expected."},{"body":"missing title"}]}`)

	preview, err := mapper.Preview(context.Background(), input, config, 10)
	if err != nil {
		t.Fatalf("Preview returned error: %v", err)
	}
	if preview.TotalRecords != 2 {
		t.Fatalf("TotalRecords = %d", preview.TotalRecords)
	}
	if preview.Skipped != 1 {
		t.Fatalf("Skipped = %d", preview.Skipped)
	}
	if len(preview.Records) != 1 || preview.Records[0].ID != "record-1" {
		t.Fatalf("records = %#v", preview.Records)
	}

	var generatedID, requiredMissing bool
	for _, diag := range preview.Diagnostics {
		if diag.Code == "generated_id" {
			generatedID = true
		}
		if diag.Code == "required_missing" {
			requiredMissing = true
		}
	}
	if !generatedID || !requiredMissing {
		t.Fatalf("diagnostics = %#v", preview.Diagnostics)
	}
}

func TestMapperMapsCSVAndNDJSON(t *testing.T) {
	mapper := NewMapper()
	csvConfig := MappingConfig{
		Format: FormatCSV,
		Options: MappingOptions{
			CSVHasHeader: true,
		},
		Fields: map[string]FieldMapping{
			"id":    {Path: "./id"},
			"title": {Path: "./title"},
		},
	}
	csvRecords, err := mapper.Map(context.Background(), []byte("id,title\na1,CSV article\n"), csvConfig)
	if err != nil {
		t.Fatalf("CSV Map returned error: %v", err)
	}
	if len(csvRecords) != 1 || csvRecords[0].Document["title"] != "CSV article" {
		t.Fatalf("csvRecords = %#v", csvRecords)
	}

	ndjsonConfig := MappingConfig{
		Format: FormatNDJSON,
		Fields: map[string]FieldMapping{
			"id":    {Path: "./id"},
			"title": {Path: "./title"},
		},
	}
	ndjsonRecords, err := mapper.Map(context.Background(), []byte("{\"id\":\"n1\",\"title\":\"NDJSON article\"}\n"), ndjsonConfig)
	if err != nil {
		t.Fatalf("NDJSON Map returned error: %v", err)
	}
	if len(ndjsonRecords) != 1 || ndjsonRecords[0].Document["title"] != "NDJSON article" {
		t.Fatalf("ndjsonRecords = %#v", ndjsonRecords)
	}
}
