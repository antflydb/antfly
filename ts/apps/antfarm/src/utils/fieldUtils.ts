export interface SearchableField {
  displayName: string;
  searchField: string;
  originalField: string;
  schemaTypes: string[]; // JSON schema types like "string", "number", etc.
  antflyTypes: string[]; // Antfly types like "text", "keyword", etc.
  variation: string;
}

export interface BasicField {
  displayName: string;
  fieldName: string;
  schemaType: string;
}

export function generateSearchableFields(
  field: string,
  schemaTypes: string[],
  antflyTypes: string[]
): SearchableField[] {
  const searchableFields: SearchableField[] = [];
  const hasMultipleAntflyTypes = antflyTypes.length > 1;

  // Add base text field (always available for text type or when it's the only type)
  if (antflyTypes.includes("text") || antflyTypes.length === 0) {
    searchableFields.push({
      displayName: hasMultipleAntflyTypes ? `${field} (text)` : field,
      searchField: field,
      originalField: field,
      schemaTypes,
      antflyTypes,
      variation: "text",
    });
  }

  // Add keyword variation if multiple types and includes keyword
  if (hasMultipleAntflyTypes && antflyTypes.includes("keyword")) {
    searchableFields.push({
      displayName: `${field} (keyword)`,
      searchField: `${field}__keyword`,
      originalField: field,
      schemaTypes,
      antflyTypes,
      variation: "keyword",
    });
  }

  // Add search_as_you_type variation (creates _2gram field)
  if (antflyTypes.includes("search_as_you_type")) {
    searchableFields.push({
      displayName: `${field} (egdegram)`,
      searchField: `${field}__2gram`,
      originalField: field,
      schemaTypes,
      antflyTypes,
      variation: "2gram",
    });

    // search_as_you_type automatically adds text type if not already present
    if (!antflyTypes.includes("text")) {
      searchableFields.push({
        displayName: `${field} (text)`,
        searchField: field,
        originalField: field,
        schemaTypes,
        antflyTypes,
        variation: "text",
      });
    }
  }

  return searchableFields;
}

export function generateBasicFields(field: string, schemaType: string): BasicField {
  return {
    displayName: `${field} (${schemaType})`,
    fieldName: field,
    schemaType: schemaType,
  };
}
