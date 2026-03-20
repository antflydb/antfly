package entity

import (
	docsafpkg "github.com/antflydb/antfly/pkg/docsaf"
	docsafentity "github.com/antflydb/antfly/pkg/docsaf/entity"
)

// BuildRecords converts sections to a records map and enriches them with extracted entities.
func BuildRecords(sections []docsafpkg.DocumentSection, entityResult *docsafentity.Result) map[string]any {
	records := make(map[string]any)
	for _, section := range sections {
		doc := section.ToDocument()
		if entityResult != nil {
			if keys, ok := entityResult.SectionEntityKeys[section.ID]; ok {
				doc["entities"] = keys
			}
			if keys, ok := entityResult.SectionRelationKeys[section.ID]; ok {
				doc["relations"] = keys
			}
		}
		records[section.ID] = doc
	}

	if entityResult != nil {
		for key, entityRecord := range entityResult.EntityRecords {
			records[key] = entityRecord.ToDocument()
		}
		for key, relationRecord := range entityResult.RelationRecords {
			records[key] = relationRecord.ToDocument()
		}
	}

	return records
}

// HasEntityRecords reports whether the records map contains entity documents.
func HasEntityRecords(records map[string]any) bool {
	for _, v := range records {
		doc, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if docType, ok := doc["_type"]; ok && docType == "entity" {
			return true
		}
	}
	return false
}
