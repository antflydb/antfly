package docsaf

import (
	"strings"
	"testing"
)

func TestXMLProcessor_CanProcess(t *testing.T) {
	xp := &XMLProcessor{}

	tests := []struct {
		name        string
		contentType string
		path        string
		want        bool
	}{
		{"XML MIME", "application/xml", "feed", true},
		{"Text XML MIME", "text/xml; charset=utf-8", "feed", true},
		{"RSS MIME", "application/rss+xml", "feed", true},
		{"XML extension", "", "articles.xml", true},
		{"HTML excluded", "text/html", "page.html", false},
		{"Markdown", "text/markdown", "doc.md", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := xp.CanProcess(tt.contentType, tt.path); got != tt.want {
				t.Fatalf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestXMLProcessor_ProcessArticleElements(t *testing.T) {
	xp := &XMLProcessor{}
	content := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<archive>
  <article id="a1">
    <title>New resorts in Cancun</title>
    <url>https://example.com/cancun</url>
    <published_at>2026-04-12</published_at>
    <brand>Travel Weekly</brand>
    <body>
      <p>Luxury resorts are opening in Cancun.</p>
      <p>Several brands are expanding all-inclusive offerings.</p>
    </body>
  </article>
  <article id="a2">
    <headline>Airline trends</headline>
    <content>Airlines are changing routes for business travel demand.</content>
  </article>
</archive>`)

	sections, err := xp.Process("northstar/articles.xml", "", "https://feeds.example.com", content)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if len(sections) != 2 {
		t.Fatalf("got %d sections, want 2", len(sections))
	}

	first := sections[0]
	if first.Type != "xml_element" {
		t.Fatalf("first.Type = %q, want xml_element", first.Type)
	}
	if first.Title != "New resorts in Cancun" {
		t.Fatalf("first.Title = %q", first.Title)
	}
	if first.URL != "https://example.com/cancun" {
		t.Fatalf("first.URL = %q", first.URL)
	}
	if !strings.Contains(first.Content, "Luxury resorts are opening") {
		t.Fatalf("first.Content = %q", first.Content)
	}
	if got := first.Metadata["root_element"]; got != "archive" {
		t.Fatalf("root_element metadata = %v", got)
	}
	if got := first.Metadata["xml_brand"]; got != "Travel Weekly" {
		t.Fatalf("xml_brand metadata = %v", got)
	}
	if got := first.Metadata["date"]; got != "2026-04-12" {
		t.Fatalf("date metadata = %v", got)
	}
	attrs, ok := first.Metadata["attributes"].(map[string]string)
	if !ok || attrs["id"] != "a1" {
		t.Fatalf("attributes metadata = %#v", first.Metadata["attributes"])
	}

	second := sections[1]
	if second.Title != "Airline trends" {
		t.Fatalf("second.Title = %q", second.Title)
	}
	if second.URL != "https://feeds.example.com/northstar/articles#airline-trends" {
		t.Fatalf("second.URL = %q", second.URL)
	}
}

func TestXMLProcessor_ProcessFallbackWholeDocument(t *testing.T) {
	xp := &XMLProcessor{}
	content := []byte(`<profile><title>Supplier Profile</title><summary>Important supplier details.</summary></profile>`)

	sections, err := xp.Process("profiles/supplier.xml", "s3://bucket/profiles/supplier.xml", "https://example.com/docs", content)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}

	section := sections[0]
	if section.Type != "xml_document" {
		t.Fatalf("section.Type = %q, want xml_document", section.Type)
	}
	if section.Title != "Supplier Profile" {
		t.Fatalf("section.Title = %q", section.Title)
	}
	if section.URL != "https://example.com/docs/profiles/supplier#supplier-profile" {
		t.Fatalf("section.URL = %q", section.URL)
	}
	if got := section.Metadata["source_url"]; got != "s3://bucket/profiles/supplier.xml" {
		t.Fatalf("source_url metadata = %v", got)
	}
}

func TestXMLProcessor_RegisteredInDefaultRegistry(t *testing.T) {
	registry := DefaultRegistry()
	processor := registry.GetProcessor("application/xml", "articles.xml")
	if _, ok := processor.(*XMLProcessor); !ok {
		t.Fatalf("processor = %T, want *XMLProcessor", processor)
	}
}

func TestTransformXMLPath(t *testing.T) {
	if got := transformXMLPath("feeds/articles.xml"); got != "feeds/articles" {
		t.Fatalf("transformXMLPath() = %q", got)
	}
}
