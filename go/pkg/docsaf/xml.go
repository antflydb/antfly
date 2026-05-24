package docsaf

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"strings"
)

// XMLProcessor processes generic XML documents.
// It prefers article-like elements as sections and falls back to one whole-document section.
type XMLProcessor struct{}

type xmlNode struct {
	name     xml.Name
	attrs    []xml.Attr
	text     string
	children []*xmlNode
	parent   *xmlNode
}

var xmlSectionElementNames = map[string]bool{
	"article":  true,
	"content":  true,
	"document": true,
	"entry":    true,
	"item":     true,
	"page":     true,
	"post":     true,
	"record":   true,
	"section":  true,
	"story":    true,
}

var xmlTitleElementNames = []string{"title", "headline", "heading", "name", "subject"}
var xmlBodyElementNames = []string{"body", "content", "articlebody", "description", "summary", "text"}
var xmlURLElementNames = []string{"url", "link", "canonical_url", "canonicalurl", "permalink"}
var xmlDateElementNames = []string{"published_at", "publishedat", "pubdate", "date", "updated_at", "updatedat", "lastmod"}

// CanProcess returns true for XML content types or .xml extensions.
func (xp *XMLProcessor) CanProcess(contentType, path string) bool {
	lowerContentType := strings.ToLower(contentType)
	if strings.Contains(lowerContentType, "xml") && !strings.Contains(lowerContentType, "html") {
		return true
	}
	return strings.HasSuffix(strings.ToLower(path), ".xml")
}

// Process processes XML content and returns document sections.
func (xp *XMLProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	root, err := parseXMLNodeTree(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse XML: %w", err)
	}
	if root == nil {
		return nil, fmt.Errorf("XML document is empty")
	}

	docMetadata := xp.extractDocumentMetadata(root, path)
	if sourceURL != "" {
		docMetadata["source_url"] = sourceURL
	}

	sectionNodes := xp.findSectionNodes(root)
	if len(sectionNodes) == 0 {
		sectionNodes = []*xmlNode{root}
	}

	sections := make([]DocumentSection, 0, len(sectionNodes))
	totalSections := len(sectionNodes)
	slugs := newSlugCounter()
	for i, node := range sectionNodes {
		section := xp.buildSection(node, path, baseURL, docMetadata, i+1, totalSections, slugs)
		if strings.TrimSpace(section.Content) == "" {
			continue
		}
		sections = append(sections, section)
	}

	return sections, nil
}

func parseXMLNodeTree(content []byte) (*xmlNode, error) {
	decoder := xml.NewDecoder(bytes.NewReader(content))
	var stack []*xmlNode
	var root *xmlNode

	for {
		tok, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
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

	return root, nil
}

func (xp *XMLProcessor) findSectionNodes(root *xmlNode) []*xmlNode {
	var sections []*xmlNode
	collectXMLSectionNodes(root, root, &sections)

	if len(sections) > 0 {
		return sections
	}

	repeated := repeatedChildSections(root)
	if len(repeated) > 1 {
		return repeated
	}

	return nil
}

func collectXMLSectionNodes(root, node *xmlNode, sections *[]*xmlNode) {
	if node != root && xmlSectionElementNames[normalizeXMLName(node.name.Local)] && strings.TrimSpace(node.collectText()) != "" {
		*sections = append(*sections, node)
		return
	}
	for _, child := range node.children {
		collectXMLSectionNodes(root, child, sections)
	}
}

func repeatedChildSections(root *xmlNode) []*xmlNode {
	counts := make(map[string]int)
	for _, child := range root.children {
		if strings.TrimSpace(child.collectText()) == "" {
			continue
		}
		counts[normalizeXMLName(child.name.Local)]++
	}

	var sectionName string
	for name, count := range counts {
		if count > 1 {
			sectionName = name
			break
		}
	}
	if sectionName == "" {
		return nil
	}

	var sections []*xmlNode
	for _, child := range root.children {
		if normalizeXMLName(child.name.Local) == sectionName {
			sections = append(sections, child)
		}
	}
	return sections
}

func (xp *XMLProcessor) buildSection(node *xmlNode, path, baseURL string, docMetadata map[string]any, ordinal, totalSections int, slugs *slugCounter) DocumentSection {
	title := firstNonEmpty(
		node.findFirstText(xmlTitleElementNames...),
		node.attrValue("title"),
		node.attrValue("name"),
	)
	if title == "" {
		if ordinal == 1 {
			if docTitle, ok := docMetadata["title"].(string); ok && docTitle != "" {
				title = docTitle
			}
		}
	}
	if title == "" {
		title = fmt.Sprintf("%s %d", node.name.Local, ordinal)
	}

	content := node.findFirstText(xmlBodyElementNames...)
	if content == "" {
		content = node.collectText()
	}

	sectionPath := node.path()
	identifier := strings.Join(append(sectionPath, title), " > ")
	slug := slugs.unique(generateSlug(title))

	url := node.findFirstText(xmlURLElementNames...)
	if url == "" && baseURL != "" {
		url = baseURL + "/" + transformXMLPath(path)
		if slug != "" {
			url += "#" + slug
		}
	}

	metadata := make(map[string]any)
	maps.Copy(metadata, docMetadata)
	maps.Copy(metadata, node.simpleChildMetadata())
	maps.Copy(metadata, map[string]any{
		"element_name":   node.name.Local,
		"element_path":   strings.Join(sectionPath, "/"),
		"section_number": ordinal,
		"total_sections": totalSections,
	})
	if len(node.attrs) > 0 {
		metadata["attributes"] = attrsToMap(node.attrs)
	}
	if date := node.findFirstText(xmlDateElementNames...); date != "" {
		metadata["date"] = date
	}

	docType := "xml_element"
	if node.parent == nil {
		docType = "xml_document"
	}

	return DocumentSection{
		ID:          generateID(path, identifier),
		FilePath:    path,
		Title:       title,
		Content:     strings.TrimSpace(content),
		Type:        docType,
		URL:         url,
		SectionPath: sectionPath,
		Metadata:    metadata,
	}
}

func (xp *XMLProcessor) extractDocumentMetadata(root *xmlNode, path string) map[string]any {
	metadata := map[string]any{
		"root_element": root.name.Local,
		"title":        filepath.Base(path),
	}
	if title := root.findFirstText(xmlTitleElementNames...); title != "" {
		metadata["title"] = title
	}
	if date := root.findFirstText(xmlDateElementNames...); date != "" {
		metadata["date"] = date
	}
	return metadata
}

func (n *xmlNode) collectText() string {
	var parts []string
	n.collectTextParts(&parts)
	return strings.Join(parts, "\n\n")
}

func (n *xmlNode) collectTextParts(parts *[]string) {
	if n.text != "" {
		*parts = append(*parts, n.text)
	}
	for _, child := range n.children {
		child.collectTextParts(parts)
	}
}

func (n *xmlNode) findFirstText(names ...string) string {
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[normalizeXMLName(name)] = true
	}

	var found string
	walkXML(n, func(node *xmlNode) {
		if found != "" {
			return
		}
		if wanted[normalizeXMLName(node.name.Local)] {
			found = strings.TrimSpace(node.collectText())
		}
	})
	return found
}

func (n *xmlNode) attrValue(name string) string {
	normalized := normalizeXMLName(name)
	for _, attr := range n.attrs {
		if normalizeXMLName(attr.Name.Local) == normalized {
			return strings.TrimSpace(attr.Value)
		}
	}
	return ""
}

func (n *xmlNode) path() []string {
	var reversed []string
	for current := n; current != nil; current = current.parent {
		reversed = append(reversed, current.name.Local)
	}
	path := make([]string, len(reversed))
	for i := range reversed {
		path[i] = reversed[len(reversed)-1-i]
	}
	return path
}

func (n *xmlNode) simpleChildMetadata() map[string]any {
	metadata := make(map[string]any)
	for _, child := range n.children {
		if len(child.children) > 0 {
			continue
		}
		value := strings.TrimSpace(child.text)
		if value == "" || len(value) > 512 {
			continue
		}
		key := "xml_" + normalizeXMLName(child.name.Local)
		if _, exists := metadata[key]; !exists {
			metadata[key] = value
		}
	}
	return metadata
}

func attrsToMap(attrs []xml.Attr) map[string]string {
	result := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		key := attr.Name.Local
		if attr.Name.Space != "" {
			key = attr.Name.Space + ":" + attr.Name.Local
		}
		result[key] = attr.Value
	}
	return result
}

func walkXML(node *xmlNode, fn func(*xmlNode)) {
	if node == nil {
		return
	}
	fn(node)
	for _, child := range node.children {
		walkXML(child, fn)
	}
}

func normalizeXMLName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "-", "")
	name = strings.ReplaceAll(name, "_", "")
	return name
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// transformXMLPath removes .xml extensions from the path for cleaner URLs.
func transformXMLPath(path string) string {
	return strings.TrimSuffix(path, ".xml")
}
