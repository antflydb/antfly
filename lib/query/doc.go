// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Package query provides schema description utilities for Bleve search query generation.
//
// This package defines types and formatting functions that describe a table schema
// in a way that is useful for LLM-powered query builders. The actual query format
// used is native Bleve query JSON.
//
// # Schema Description
//
// Use [SchemaDescription] and [FieldInfo] to describe available fields:
//
//	schema := query.SchemaDescription{
//	    Fields: []query.FieldInfo{
//	        {Name: "title", Type: "text", Searchable: true},
//	        {Name: "status", Type: "keyword", Searchable: true},
//	        {Name: "price", Type: "numeric", Searchable: true},
//	    },
//	}
//
// # LLM Integration
//
// Format schema descriptions for LLM prompts:
//
//	prompt := query.FormatSchemaForLLM(schema)
//	jsonStr, _ := query.SchemaToJSON(schema)
//
// # Schema Extraction
//
// Extract schema descriptions from JSON Schema:
//
//	schema := query.ExtractSchemaDescription(jsonSchema)
package query
