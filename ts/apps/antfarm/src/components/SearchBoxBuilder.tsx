import { Antfly, Autosuggest, Facet, QueryBox, Results } from "@antfly/components";
import type { IndexStatus, QueryHit } from "@antfly/sdk";
import { CopyIcon, PlusIcon, TrashIcon } from "@radix-ui/react-icons";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import { useTheme } from "@/components/theme-provider";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { TableSchema } from "../api";
import {
  type BasicField,
  generateBasicFields,
  generateSearchableFields,
  type SearchableField,
} from "../utils/fieldUtils";

interface SearchBoxBuilderProps {
  tableName: string;
  tableSchema?: TableSchema;
  indexes?: IndexStatus[];
}

interface FacetConfig {
  id: string;
  fields: string[];
  title: string;
}

export default function SearchBoxBuilder({
  tableName,
  tableSchema,
  indexes = [],
}: SearchBoxBuilderProps) {
  const { theme } = useTheme();
  const [searchFields, setSearchFields] = useState<string[]>([]);
  const [semanticIndexes, setSemanticIndexes] = useState<string[]>([]);
  const [facets, setFacets] = useState<FacetConfig[]>([]);
  const [newFacetField, setNewFacetField] = useState("");
  const [newFacetTitle, setNewFacetTitle] = useState("");
  const [newSearchField, setNewSearchField] = useState("");
  const [copiedCode, setCopiedCode] = useState(false);
  const [useSemanticSearch, setUseSemanticSearch] = useState(false);
  const [semanticLimit, setSemanticLimit] = useState<number>(5);
  const [useAutosuggest, setUseAutosuggest] = useState(false);
  const [useSemanticAutosuggest, setUseSemanticAutosuggest] = useState(false);
  const [autosuggestFields, setAutosuggestFields] = useState<string[]>([]);
  const [autosuggestSemanticIndexes, setAutosuggestSemanticIndexes] = useState<string[]>([]);
  const [autosuggestLimit, setAutosuggestLimit] = useState<number>(10);
  const [autosuggestMinChars, setAutosuggestMinChars] = useState<number>(2);
  const [itemsPerPage, setItemsPerPage] = useState<number>(10);
  const [resultFields, setResultFields] = useState<string[]>([]);
  const [showResults, setShowResults] = useState(true);
  const [usePagination, setUsePagination] = useState(true);
  const [showFacets, setShowFacets] = useState(true);
  const [useThumbnails, setUseThumbnails] = useState(false);
  const [thumbnailField, setThumbnailField] = useState<string>("");
  const [thumbnailSize, setThumbnailSize] = useState<"small" | "medium" | "large">("medium");
  const [displayTextField, setDisplayTextField] = useState<string>("");

  // Get baseUrl from environment
  const baseUrl =
    import.meta.env.MODE === "development" ? "http://localhost:8080/api/v1" : "/api/v1";
  const antflyUrl = `${baseUrl}/tables/${tableName}`;

  // Derive effective pagination: pagination is disabled when semantic search is enabled
  const effectivePagination = usePagination && !useSemanticSearch;

  // Extract available searchable field variations from table schema
  const availableFields = useMemo(() => {
    if (!tableSchema?.document_schemas) return [];

    const searchableFields: SearchableField[] = [];
    Object.values(tableSchema.document_schemas).forEach((docSchema) => {
      if (docSchema.schema?.properties) {
        Object.entries(docSchema.schema.properties).forEach(([field, property]) => {
          // Fields are indexed by default unless explicitly disabled or have non-indexed types
          const isExplicitlyNotIndexed = property["x-antfly-index"] === false;
          const types = property["x-antfly-types"] || [];
          const hasNonIndexedTypes = types.some((type) => type === "embedding" || type === "blob");

          if (!isExplicitlyNotIndexed && !hasNonIndexedTypes) {
            const schemaTypes = property.type ? [property.type] : [];
            const fieldVariations = generateSearchableFields(field, schemaTypes, types);
            searchableFields.push(...fieldVariations);
          }
        });
      }
    });

    return searchableFields.sort((a, b) => {
      // Sort by original field name first, then by variation type
      const fieldCompare = a.originalField.localeCompare(b.originalField);
      if (fieldCompare !== 0) return fieldCompare;

      // Define sort order for variations
      const variationOrder = { text: 0, keyword: 1, "2gram": 2 };
      return (
        (variationOrder[a.variation as keyof typeof variationOrder] || 999) -
        (variationOrder[b.variation as keyof typeof variationOrder] || 999)
      );
    });
  }, [tableSchema]);

  // Extract basic fields for result display (no variations)
  const availableBasicFields = useMemo(() => {
    if (!tableSchema?.document_schemas) return [];

    const basicFields: BasicField[] = [];
    const processedFields = new Set<string>();

    Object.values(tableSchema.document_schemas).forEach((docSchema) => {
      if (docSchema.schema?.properties) {
        Object.entries(docSchema.schema.properties).forEach(([field, property]) => {
          // Skip if already processed or explicitly not indexed
          if (processedFields.has(field)) return;
          processedFields.add(field);

          const isExplicitlyNotIndexed = property["x-antfly-index"] === false;
          const antflyTypes = property["x-antfly-types"] || [];
          const hasNonIndexedTypes = antflyTypes.some(
            (type) => type === "embedding" || type === "blob"
          );

          if (!isExplicitlyNotIndexed && !hasNonIndexedTypes) {
            const schemaType = property.type || "unknown";
            const basicField = generateBasicFields(field, schemaType);
            basicFields.push(basicField);
          }
        });
      }
    });

    return basicFields.sort((a, b) => a.fieldName.localeCompare(b.fieldName));
  }, [tableSchema]);

  // Extract available semantic indexes grouped by type
  const availableSemanticIndexes = useMemo(() => {
    const grouped = indexes.reduce(
      (acc, index) => {
        const type = index.config.type;
        if (!acc[type]) {
          acc[type] = [];
        }
        acc[type].push(index.config.name);
        return acc;
      },
      {} as Record<string, string[]>
    );

    // Sort indexes within each group
    Object.keys(grouped).forEach((type) => {
      grouped[type].sort();
    });

    return grouped;
  }, [indexes]);

  // Auto-select first vector index when semantic search is enabled but none selected
  useEffect(() => {
    if (
      useSemanticSearch &&
      semanticIndexes.length === 0 &&
      availableSemanticIndexes.embeddings?.length > 0
    ) {
      setSemanticIndexes([availableSemanticIndexes.embeddings[0]]);
    }
  }, [useSemanticSearch, semanticIndexes.length, availableSemanticIndexes.embeddings]);

  const handleAddSearchField = (searchField: string) => {
    if (searchField?.trim() && !searchFields.includes(searchField)) {
      setSearchFields([...searchFields, searchField.trim()]);
    }
  };

  const handleRemoveSearchField = (searchField: string) => {
    setSearchFields(searchFields.filter((f) => f !== searchField));
  };

  const handleAddCustomSearchField = () => {
    if (newSearchField?.trim()) {
      handleAddSearchField(newSearchField.trim());
      setNewSearchField("");
    }
  };

  const handleAddSemanticIndex = (index: string) => {
    if (!semanticIndexes.includes(index)) {
      setSemanticIndexes([...semanticIndexes, index]);
    }
  };

  const handleRemoveSemanticIndex = (index: string) => {
    setSemanticIndexes(semanticIndexes.filter((i) => i !== index));
  };

  const handleAddAutosuggestField = (field: string) => {
    if (!autosuggestFields.includes(field)) {
      setAutosuggestFields([...autosuggestFields, field]);
    }
  };

  const handleRemoveAutosuggestField = (field: string) => {
    setAutosuggestFields(autosuggestFields.filter((f) => f !== field));
  };

  const handleAddAutosuggestSemanticIndex = (index: string) => {
    if (!autosuggestSemanticIndexes.includes(index)) {
      setAutosuggestSemanticIndexes([...autosuggestSemanticIndexes, index]);
    }
  };

  const handleRemoveAutosuggestSemanticIndex = (index: string) => {
    setAutosuggestSemanticIndexes(autosuggestSemanticIndexes.filter((i) => i !== index));
  };

  const handleAddResultField = (field: string) => {
    if (!resultFields.includes(field)) {
      setResultFields([...resultFields, field]);
    }
  };

  const handleRemoveResultField = (field: string) => {
    setResultFields(resultFields.filter((f) => f !== field));
  };

  const handleSetThumbnailField = (field: string) => {
    setThumbnailField(field);
  };

  const handleClearThumbnailField = () => {
    setThumbnailField("");
  };

  const handleSetDisplayTextField = (field: string) => {
    setDisplayTextField(field);
  };

  const handleClearDisplayTextField = () => {
    setDisplayTextField("");
  };

  const handleAddFacet = () => {
    if (newFacetField && newFacetTitle) {
      const newFacet: FacetConfig = {
        id: `facet${Date.now()}`,
        fields: [newFacetField],
        title: newFacetTitle,
      };
      setFacets([...facets, newFacet]);
      setNewFacetField("");
      setNewFacetTitle("");
    }
  };

  const handleRemoveFacet = (facetId: string) => {
    setFacets(facets.filter((f) => f.id !== facetId));
  };

  // Memoize Results props to prevent infinite re-renders and add validation
  const resultsProps = useMemo(() => {
    if (useSemanticSearch) {
      // Only include semanticIndexes if they exist and are valid
      if (semanticIndexes.length > 0 && semanticIndexes.every((index) => index?.trim())) {
        return {
          semanticIndexes,
          limit: Math.max(1, Math.min(100, semanticLimit)),
        };
      } else {
        // Don't render without valid semantic indexes
        return null;
      }
    } else {
      // Only include fields if they exist and are valid
      if (searchFields.length > 0 && searchFields.every((field) => field?.trim())) {
        return { fields: searchFields };
      } else {
        // Don't render without valid search fields
        return null;
      }
    }
  }, [useSemanticSearch, semanticIndexes, semanticLimit, searchFields]);

  // Get thumbnail size classes
  const getThumbnailSizeClass = useCallback(() => {
    switch (thumbnailSize) {
      case "small":
        return "w-16 h-16";
      case "large":
        return "w-40 h-40";
      default:
        return "w-24 h-24";
    }
  }, [thumbnailSize]);

  // Memoize Results items function to prevent recreation
  const renderResultItems = useCallback(
    (data: QueryHit[]) => {
      return Array.isArray(data)
        ? data.map(({ _source, _id }, index: number) => {
            const thumbnailUrl =
              useThumbnails && thumbnailField && _source ? _source[thumbnailField] : null;
            const displayTextValue = displayTextField && _source ? _source[displayTextField] : null;

            return (
              <div key={_id || index} className="p-2 border-b">
                <div className="flex gap-4">
                  {/* Thumbnail */}
                  {useThumbnails && thumbnailField && (
                    <div
                      className={`shrink-0 ${getThumbnailSizeClass()} bg-gray-100 rounded overflow-hidden flex items-center justify-center`}
                    >
                      {thumbnailUrl ? (
                        <img
                          src={String(thumbnailUrl)}
                          alt="Thumbnail"
                          className="w-full h-full object-cover"
                          onError={(e) => {
                            const target = e.target as HTMLImageElement;
                            target.style.display = "none";
                            const parent = target.parentElement;
                            if (parent && !parent.querySelector(".placeholder-text")) {
                              const placeholder = document.createElement("div");
                              placeholder.className =
                                "placeholder-text text-gray-400 text-xs text-center p-2";
                              placeholder.textContent = "No image";
                              parent.appendChild(placeholder);
                            }
                          }}
                        />
                      ) : (
                        <div className="text-gray-400 text-xs text-center p-2">No image</div>
                      )}
                    </div>
                  )}

                  {/* Content */}
                  <div className="flex-1 min-w-0">
                    {/* Display Text Field */}
                    {displayTextField &&
                      displayTextValue !== undefined &&
                      displayTextValue !== null && (
                        <div className="text-lg font-semibold mb-2">{String(displayTextValue)}</div>
                      )}

                    {/* Other Result Fields */}
                    {_source && resultFields.length > 0
                      ? resultFields.map((field) => {
                          const value = _source[field];
                          return value !== undefined ? (
                            <div key={field} className="text-sm">
                              <strong>{field}:</strong> {String(value)}
                            </div>
                          ) : null;
                        })
                      : _source &&
                        Object.entries(_source).map(([key, value]) => (
                          <div key={key} className="text-sm">
                            <strong>{key}:</strong> {String(value)}
                          </div>
                        ))}
                  </div>
                </div>
              </div>
            );
          })
        : [];
    },
    [resultFields, useThumbnails, thumbnailField, displayTextField, getThumbnailSizeClass]
  );

  const generateReactCode = () => {
    const facetsCode =
      showFacets && facets.length > 0
        ? facets
            .map(
              (facet) =>
                `        <Facet
          id="${facet.id}"
          fields={[${facet.fields.map((f) => `"${f}"`).join(", ")}]}
          seeMore="See more"
          placeholder="Filter ${facet.title}..."
        />`
            )
            .join("\n")
        : "";

    const resultsSearchProps = useSemanticSearch
      ? semanticIndexes.length > 0
        ? `
        searchBoxId="main"
        semanticIndexes={[${semanticIndexes.map((i) => `"${i}"`).join(", ")}]}
        limit={${semanticLimit}}`
        : `
        searchBoxId="main"
        limit={${semanticLimit}}`
      : searchFields.length > 0
        ? `
        searchBoxId="main"
        fields={[${searchFields.map((f) => `"${f}"`).join(", ")}]}`
        : `
        searchBoxId="main"`;

    const autosuggestCode = useAutosuggest
      ? useSemanticAutosuggest && autosuggestSemanticIndexes.length > 0
        ? `
        <Autosuggest
          semanticIndexes={[${autosuggestSemanticIndexes.map((i) => `"${i}"`).join(", ")}]}
          fields={${autosuggestFields.length > 0 ? `[${autosuggestFields.map((f) => `"${f}"`).join(", ")}]` : "[]"}}
          limit={${autosuggestLimit}}
          minChars={${autosuggestMinChars}}
        />`
        : !useSemanticAutosuggest && autosuggestFields.length > 0
          ? `
        <Autosuggest
          fields={[${autosuggestFields.map((f) => `"${f}"`).join(", ")}]}
          limit={${autosuggestLimit}}
          minChars={${autosuggestMinChars}}
        />`
          : ""
      : "";

    const thumbnailSizeClass =
      thumbnailSize === "small"
        ? "w-16 h-16"
        : thumbnailSize === "large"
          ? "w-40 h-40"
          : "w-24 h-24";

    const resultsCode = showResults
      ? `
      <Results${resultsSearchProps}
        id="result"
        items={(data) =>
          Array.isArray(data)
            ? data.map(({ _source, _id }) => {${
              useThumbnails && thumbnailField
                ? `
                const thumbnailUrl = _source ? _source["${thumbnailField}"] : null;`
                : ""
            }${
              displayTextField
                ? `
                const displayTextValue = _source ? _source["${displayTextField}"] : null;`
                : ""
            }
                return (
                  <div key={_id} className="p-2 border-b">
                    <div className="flex gap-4">${
                      useThumbnails && thumbnailField
                        ? `
                      {/* Thumbnail */}
                      <div className="shrink-0 ${thumbnailSizeClass} bg-gray-100 rounded overflow-hidden flex items-center justify-center">
                        {thumbnailUrl ? (
                          <img
                            src={String(thumbnailUrl)}
                            alt="Thumbnail"
                            className="w-full h-full object-cover"
                            onError={(e) => {
                              const target = e.target;
                              target.style.display = 'none';
                              const parent = target.parentElement;
                              if (parent && !parent.querySelector('.placeholder-text')) {
                                const placeholder = document.createElement('div');
                                placeholder.className = 'placeholder-text text-gray-400 text-xs text-center p-2';
                                placeholder.textContent = 'No image';
                                parent.appendChild(placeholder);
                              }
                            }}
                          />
                        ) : (
                          <div className="text-gray-400 text-xs text-center p-2">No image</div>
                        )}
                      </div>`
                        : ""
                    }
                      {/* Content */}
                      <div className="flex-1 min-w-0">${
                        displayTextField
                          ? `
                        {/* Display Text Field */}
                        {displayTextValue !== undefined && displayTextValue !== null && (
                          <div className="text-lg font-semibold mb-2">{String(displayTextValue)}</div>
                        )}`
                          : ""
                      }
                        {/* Result Fields */}
                        {_source &&${
                          resultFields.length > 0
                            ? `
                        [${resultFields.map((f) => `"${f}"`).join(", ")}].map((field) => {
                          const value = _source[field];
                          return value !== undefined ? (
                            <div key={field} className="text-sm">
                              <strong>{field}:</strong> {String(value)}
                            </div>
                          ) : null;
                        })`
                            : `
                        Object.entries(_source).map(([key, value]) => (
                          <div key={key} className="text-sm">
                            <strong>{key}:</strong> {String(value)}
                          </div>
                        ))`
                        }}
                      </div>
                    </div>
                  </div>
                );
              })
            : []
        }${
          effectivePagination
            ? `
        itemsPerPage={${itemsPerPage}}`
            : ""
        }
      />`
      : "";

    // Build component list based on what's enabled
    const components = ["Antfly", "QueryBox"];
    if (showFacets && facets.length > 0) components.push("Facet");
    if (showResults) components.push("Results");
    if (
      useAutosuggest &&
      ((useSemanticAutosuggest && autosuggestSemanticIndexes.length > 0) ||
        (!useSemanticAutosuggest && autosuggestFields.length > 0))
    )
      components.push("Autosuggest");

    const imports = `import { ${components.join(", ")} } from "@antfly/components";`;

    return `${imports}

export default function MySearchComponent() {
  return (
    <Antfly url="${antflyUrl}" table="${tableName}">
      <QueryBox id="main" mode="live">${autosuggestCode}
      </QueryBox>
${facetsCode}${resultsCode}
    </Antfly>
  );
}`;
  };

  const handleCopyCode = async () => {
    try {
      await navigator.clipboard.writeText(generateReactCode());
      setCopiedCode(true);
      setTimeout(() => setCopiedCode(false), 2000);
    } catch (err) {
      console.error("Failed to copy code:", err);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">SearchBox Builder</h2>
      </div>

      <Tabs defaultValue="config">
        <TabsList>
          <TabsTrigger value="config">Configuration</TabsTrigger>
          <TabsTrigger value="preview">Preview</TabsTrigger>
          <TabsTrigger value="code">Code</TabsTrigger>
        </TabsList>

        <TabsContent value="preview" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Search Interface Preview</CardTitle>
            </CardHeader>
            <CardContent>
              {resultsProps ? (
                <Antfly url={antflyUrl} table={tableName}>
                  <div className="flex gap-6">
                    {/* Sidebar for facets */}
                    {showFacets && facets.length > 0 && (
                      <aside className="w-64 shrink-0 space-y-4">
                        {facets.map((facet) => (
                          <div key={facet.id}>
                            <h3 className="text-sm font-semibold mb-2 px-1">{facet.title}</h3>
                            <Facet
                              id={facet.id}
                              fields={facet.fields}
                              seeMore="See more"
                              placeholder={`Filter ${facet.title}...`}
                            />
                          </div>
                        ))}
                      </aside>
                    )}

                    {/* Main content area */}
                    <div className="flex-1 space-y-6">
                      <QueryBox id="querybox" mode="live">
                        {useAutosuggest &&
                          (useSemanticAutosuggest
                            ? autosuggestSemanticIndexes.length > 0 && (
                                <Autosuggest
                                  semanticIndexes={autosuggestSemanticIndexes}
                                  fields={autosuggestFields.length > 0 ? autosuggestFields : []}
                                  limit={autosuggestLimit}
                                  minChars={autosuggestMinChars}
                                />
                              )
                            : autosuggestFields.length > 0 && (
                                <Autosuggest
                                  fields={autosuggestFields}
                                  limit={autosuggestLimit}
                                  minChars={autosuggestMinChars}
                                />
                              ))}
                      </QueryBox>
                      {showResults && (
                        <Results
                          searchBoxId="querybox"
                          id="results"
                          items={renderResultItems}
                          itemsPerPage={effectivePagination ? itemsPerPage : undefined}
                          {...resultsProps}
                        />
                      )}
                    </div>
                  </div>
                </Antfly>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <p>
                    {useSemanticSearch
                      ? "Select semantic indexes to enable the search preview"
                      : "Select search fields to enable the search preview"}
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="code" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>React Component Code</CardTitle>
                <Button onClick={handleCopyCode} variant="outline" size="sm" disabled={copiedCode}>
                  <CopyIcon className="h-4 w-4 mr-2" />
                  {copiedCode ? "Copied!" : "Copy Code"}
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <SyntaxHighlighter
                language="jsx"
                style={theme === "dark" ? oneDark : oneLight}
                customStyle={{
                  margin: 0,
                  fontSize: "0.875rem",
                  lineHeight: "1.25rem",
                  maxHeight: "32rem",
                  borderRadius: "0.375rem",
                }}
                wrapLongLines={false}
              >
                {generateReactCode()}
              </SyntaxHighlighter>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="config" className="space-y-4">
          {/* Search Configuration Card */}
          <Card>
            <CardHeader>
              <CardTitle>Search Configuration</CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Search Mode Toggle */}
              <div>
                <div className="flex items-center space-x-2 mb-3">
                  <h3 className="text-lg font-semibold">Search Mode</h3>
                  <Switch
                    id="search-mode"
                    checked={useSemanticSearch}
                    onCheckedChange={setUseSemanticSearch}
                  />
                  <Label htmlFor="search-mode">{useSemanticSearch ? "Semantic" : "Text"}</Label>
                </div>
                <p className="text-sm text-muted-foreground">
                  {useSemanticSearch
                    ? "Use vector embeddings for semantic similarity search"
                    : "Use traditional text-based search across specified fields"}
                </p>
              </div>

              {/* Search Fields Configuration */}
              {!useSemanticSearch && (
                <div>
                  <h3 className="text-lg font-semibold mb-3">Search Fields</h3>
                  <div className="space-y-4">
                    <div>
                      <Label>Selected Fields</Label>
                      <div className="flex flex-wrap gap-2 mt-2">
                        {searchFields.map((searchField) => {
                          const fieldInfo = availableFields.find(
                            (f) => f.searchField === searchField
                          );
                          const displayName = fieldInfo ? fieldInfo.displayName : searchField;
                          return (
                            <Badge
                              key={searchField}
                              variant="secondary"
                              className="cursor-pointer"
                              onClick={() => handleRemoveSearchField(searchField)}
                              title={`Search field: ${searchField}\nSchema types: ${fieldInfo?.schemaTypes.join(", ") || "unknown"}\nAntfly types: ${fieldInfo?.antflyTypes.join(", ") || "unknown"}`}
                            >
                              {displayName} ×
                            </Badge>
                          );
                        })}
                      </div>
                    </div>

                    <div>
                      <Label>Add Custom Field</Label>
                      <div className="flex gap-2 mt-2">
                        <Input
                          placeholder="Enter field name"
                          value={newSearchField}
                          onChange={(e) => setNewSearchField(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") {
                              handleAddCustomSearchField();
                            }
                          }}
                          className="flex-1"
                        />
                        <Button
                          onClick={handleAddCustomSearchField}
                          size="icon"
                          className="h-10 w-10 shrink-0"
                        >
                          <PlusIcon className="h-4 w-4" />
                        </Button>
                      </div>
                      <p className="text-sm text-muted-foreground mt-1">
                        Type a custom field name or select from available fields below
                      </p>
                    </div>

                    <div>
                      <Label>Available Fields</Label>
                      <div className="flex flex-wrap gap-2 mt-2">
                        {availableFields
                          .filter(({ searchField }) => !searchFields.includes(searchField))
                          .map((fieldInfo) => (
                            <Badge
                              key={fieldInfo.searchField}
                              variant="outline"
                              className="cursor-pointer hover:bg-muted"
                              onClick={() => handleAddSearchField(fieldInfo.searchField)}
                              title={`Search field: ${fieldInfo.searchField}\nSchema types: ${fieldInfo.schemaTypes.join(", ")}\nAntfly types: ${fieldInfo.antflyTypes.join(", ")}`}
                            >
                              + {fieldInfo.displayName}
                            </Badge>
                          ))}
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Semantic Indexes Configuration */}
              {useSemanticSearch && (
                <div>
                  <h3 className="text-lg font-semibold mb-3">Semantic Indexes</h3>
                  <div className="space-y-4">
                    <div>
                      <Label>Vector Indexes</Label>
                      {!availableSemanticIndexes.embeddings ||
                      availableSemanticIndexes.embeddings.length === 0 ? (
                        <p className="text-sm text-destructive mt-2">
                          No vector indexes available. Create a vector index to enable semantic
                          search.
                        </p>
                      ) : (
                        <div className="mt-2">
                          <div className="flex flex-wrap gap-2">
                            {availableSemanticIndexes.embeddings?.map((index) => {
                              const isSelected = semanticIndexes.includes(index);
                              return (
                                <Badge
                                  key={index}
                                  variant={isSelected ? "default" : "outline"}
                                  className="cursor-pointer hover:bg-muted"
                                  onClick={() =>
                                    isSelected
                                      ? handleRemoveSemanticIndex(index)
                                      : handleAddSemanticIndex(index)
                                  }
                                >
                                  {isSelected ? `${index} ×` : `+ ${index}`}
                                </Badge>
                              );
                            })}
                          </div>
                          <p className="text-sm text-muted-foreground mt-2">
                            First index is auto-selected by default. Click to add/remove indexes.
                          </p>
                        </div>
                      )}
                    </div>

                    <div>
                      <Label htmlFor="semantic-limit">Result Limit</Label>
                      <Input
                        id="semantic-limit"
                        type="number"
                        min="1"
                        max="100"
                        value={semanticLimit}
                        onChange={(e) => setSemanticLimit(parseInt(e.target.value, 10) || 5)}
                        placeholder="Number of results"
                        className="mt-2 w-32"
                      />
                      <p className="text-sm text-muted-foreground mt-1">
                        Maximum number of results to return from semantic search (1-100). Default: 5
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Autosuggest Configuration Card */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Autosuggest</CardTitle>
                <div className="flex items-center space-x-2">
                  <Switch
                    id="autosuggest-enabled"
                    checked={useAutosuggest}
                    onCheckedChange={setUseAutosuggest}
                  />
                  <Label htmlFor="autosuggest-enabled">Enable</Label>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground">
                    Show search suggestions as the user types
                  </p>

                  {useAutosuggest && (
                    <>
                      {/* Autosuggest Mode Toggle */}
                      <div>
                        <div className="flex items-center space-x-2 mb-3">
                          <h3 className="text-lg font-semibold">Autosuggest Mode</h3>
                          <Switch
                            id="autosuggest-mode"
                            checked={useSemanticAutosuggest}
                            onCheckedChange={setUseSemanticAutosuggest}
                          />
                          <Label htmlFor="autosuggest-mode">
                            {useSemanticAutosuggest ? "Semantic" : "Text"}
                          </Label>
                        </div>
                        <p className="text-sm text-muted-foreground">
                          {useSemanticAutosuggest
                            ? "Use vector embeddings for semantic suggestions"
                            : "Use traditional text-based suggestions"}
                        </p>
                      </div>

                      {/* Text Mode - Fields Selection */}
                      {!useSemanticAutosuggest && (
                        <>
                          <div>
                            <Label>Selected Autosuggest Fields</Label>
                            <div className="flex flex-wrap gap-2 mt-2">
                              {autosuggestFields.map((field) => {
                                const fieldInfo = availableFields.find(
                                  (f) => f.searchField === field
                                );
                                const displayName = fieldInfo ? fieldInfo.displayName : field;
                                return (
                                  <Badge
                                    key={field}
                                    variant="secondary"
                                    className="cursor-pointer"
                                    onClick={() => handleRemoveAutosuggestField(field)}
                                    title={`Search field: ${field}\\nSchema types: ${fieldInfo?.schemaTypes.join(", ") || "unknown"}\\nAntfly types: ${fieldInfo?.antflyTypes.join(", ") || "unknown"}`}
                                  >
                                    {displayName} ×
                                  </Badge>
                                );
                              })}
                            </div>
                          </div>

                          <div>
                            <Label>Available Fields for Autosuggest</Label>
                            <div className="flex flex-wrap gap-2 mt-2">
                              {availableFields
                                .filter(
                                  ({ searchField }) => !autosuggestFields.includes(searchField)
                                )
                                .map((fieldInfo) => (
                                  <Badge
                                    key={fieldInfo.searchField}
                                    variant="outline"
                                    className="cursor-pointer hover:bg-muted"
                                    onClick={() => handleAddAutosuggestField(fieldInfo.searchField)}
                                    title={`Search field: ${fieldInfo.searchField}\\nSchema types: ${fieldInfo.schemaTypes.join(", ")}\\nAntfly types: ${fieldInfo.antflyTypes.join(", ")}`}
                                  >
                                    + {fieldInfo.displayName}
                                  </Badge>
                                ))}
                            </div>
                          </div>
                        </>
                      )}

                      {/* Semantic Mode - Indexes Selection */}
                      {useSemanticAutosuggest && (
                        <div>
                          <Label>Vector Indexes</Label>
                          {!availableSemanticIndexes.embeddings ||
                          availableSemanticIndexes.embeddings.length === 0 ? (
                            <p className="text-sm text-muted-foreground mt-2">
                              No vector indexes available. Create a vector index to enable semantic
                              autosuggest.
                            </p>
                          ) : (
                            <div className="mt-2">
                              <div className="flex flex-wrap gap-2">
                                {availableSemanticIndexes.embeddings?.map((index) => {
                                  const isSelected = autosuggestSemanticIndexes.includes(index);
                                  return (
                                    <Badge
                                      key={index}
                                      variant={isSelected ? "default" : "outline"}
                                      className="cursor-pointer hover:bg-muted"
                                      onClick={() =>
                                        isSelected
                                          ? handleRemoveAutosuggestSemanticIndex(index)
                                          : handleAddAutosuggestSemanticIndex(index)
                                      }
                                    >
                                      {isSelected ? `${index} ×` : `+ ${index}`}
                                    </Badge>
                                  );
                                })}
                              </div>
                            </div>
                          )}
                        </div>
                      )}

                      {/* Common Configuration */}
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <Label htmlFor="autosuggest-limit">Suggestion Limit</Label>
                          <Input
                            id="autosuggest-limit"
                            type="number"
                            min="1"
                            max="50"
                            value={autosuggestLimit}
                            onChange={(e) =>
                              setAutosuggestLimit(parseInt(e.target.value, 10) || 10)
                            }
                            placeholder="Number of suggestions"
                            className="mt-2"
                          />
                          <p className="text-sm text-muted-foreground mt-1">
                            Maximum number of suggestions to show (1-50)
                          </p>
                        </div>

                        <div>
                          <Label htmlFor="autosuggest-min-chars">Minimum Characters</Label>
                          <Input
                            id="autosuggest-min-chars"
                            type="number"
                            min="1"
                            max="10"
                            value={autosuggestMinChars}
                            onChange={(e) =>
                              setAutosuggestMinChars(parseInt(e.target.value, 10) || 2)
                            }
                            placeholder="Minimum characters"
                            className="mt-2"
                          />
                          <p className="text-sm text-muted-foreground mt-1">
                            Minimum characters before showing suggestions (1-10)
                          </p>
                        </div>
                      </div>
                    </>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Results Display Configuration Card */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Results Display</CardTitle>
                <div className="flex items-center space-x-2">
                  <Switch
                    id="show-results"
                    checked={showResults}
                    onCheckedChange={setShowResults}
                  />
                  <Label htmlFor="show-results">Enable</Label>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground">
                    Toggle to show or hide the results component
                  </p>

                  {showResults && (
                    <>
                      <div className="flex items-center space-x-2">
                        <Switch
                          id="use-pagination"
                          checked={usePagination}
                          onCheckedChange={setUsePagination}
                          disabled={useSemanticSearch}
                        />
                        <Label htmlFor="use-pagination">Enable Pagination</Label>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        {useSemanticSearch
                          ? "Pagination is disabled for semantic search"
                          : "Toggle to enable or disable pagination"}
                      </p>

                      {effectivePagination && (
                        <div>
                          <Label htmlFor="items-per-page">Items Per Page</Label>
                          <Input
                            id="items-per-page"
                            type="number"
                            min="1"
                            max="100"
                            value={itemsPerPage}
                            onChange={(e) => setItemsPerPage(parseInt(e.target.value, 10) || 10)}
                            placeholder="Number of items per page"
                            className="mt-2 w-32"
                          />
                          <p className="text-sm text-muted-foreground mt-1">
                            Number of results to display per page (1-100)
                          </p>
                        </div>
                      )}
                    </>
                  )}

                  {showResults && (
                    <div>
                      <Label>Result Display Fields</Label>
                      <p className="text-sm text-muted-foreground mt-1 mb-3">
                        Choose which fields to display in search results. If no fields are selected,
                        all fields will be shown.
                      </p>

                      <div className="space-y-4">
                        <div>
                          <Label>Selected Result Fields</Label>
                          <div className="flex flex-wrap gap-2 mt-2">
                            {resultFields.map((field) => {
                              const fieldInfo = availableBasicFields.find(
                                (f) => f.fieldName === field
                              );
                              const displayName = fieldInfo ? fieldInfo.displayName : field;
                              return (
                                <Badge
                                  key={field}
                                  variant="secondary"
                                  className="cursor-pointer"
                                  onClick={() => handleRemoveResultField(field)}
                                  title={`Field: ${field}\\nSchema type: ${fieldInfo?.schemaType || "unknown"}`}
                                >
                                  {displayName} ×
                                </Badge>
                              );
                            })}
                          </div>
                        </div>

                        <div>
                          <Label>Available Fields for Results</Label>
                          <div className="flex flex-wrap gap-2 mt-2">
                            {availableBasicFields
                              .filter(({ fieldName }) => !resultFields.includes(fieldName))
                              .map((fieldInfo) => (
                                <Badge
                                  key={fieldInfo.fieldName}
                                  variant="outline"
                                  className="cursor-pointer hover:bg-muted"
                                  onClick={() => handleAddResultField(fieldInfo.fieldName)}
                                  title={`Field: ${fieldInfo.fieldName}\\nSchema type: ${fieldInfo.schemaType}`}
                                >
                                  + {fieldInfo.displayName}
                                </Badge>
                              ))}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Thumbnails Configuration */}
                  {showResults && (
                    <Card className="border-muted">
                      <CardHeader>
                        <div className="flex items-center justify-between">
                          <CardTitle className="text-base">Thumbnails</CardTitle>
                          <div className="flex items-center space-x-2">
                            <Switch
                              id="use-thumbnails"
                              checked={useThumbnails}
                              onCheckedChange={setUseThumbnails}
                            />
                            <Label htmlFor="use-thumbnails">Enable</Label>
                          </div>
                        </div>
                      </CardHeader>
                      <CardContent>
                        <div className="space-y-4">
                          <p className="text-sm text-muted-foreground">
                            Display thumbnail images on the left side of search results
                          </p>

                          {useThumbnails && (
                            <>
                              <div>
                                <Label>Thumbnail Field</Label>
                                <p className="text-sm text-muted-foreground mt-1 mb-3">
                                  Select a field containing the image URL for thumbnails
                                </p>

                                {thumbnailField ? (
                                  <div className="mb-3">
                                    <Badge
                                      variant="secondary"
                                      className="cursor-pointer"
                                      onClick={handleClearThumbnailField}
                                    >
                                      {thumbnailField} ×
                                    </Badge>
                                  </div>
                                ) : (
                                  <p className="text-sm text-muted-foreground mb-3">
                                    No thumbnail field selected
                                  </p>
                                )}

                                <div className="flex flex-wrap gap-2">
                                  {availableBasicFields.map((fieldInfo) => (
                                    <Badge
                                      key={fieldInfo.fieldName}
                                      variant={
                                        thumbnailField === fieldInfo.fieldName
                                          ? "default"
                                          : "outline"
                                      }
                                      className="cursor-pointer hover:bg-muted"
                                      onClick={() => handleSetThumbnailField(fieldInfo.fieldName)}
                                      title={`Field: ${fieldInfo.fieldName}\\nSchema type: ${fieldInfo.schemaType}`}
                                    >
                                      {thumbnailField === fieldInfo.fieldName
                                        ? fieldInfo.displayName
                                        : `+ ${fieldInfo.displayName}`}
                                    </Badge>
                                  ))}
                                </div>
                              </div>

                              <div>
                                <Label htmlFor="thumbnail-size">Thumbnail Size</Label>
                                <div className="flex gap-2 mt-2">
                                  {(["small", "medium", "large"] as const).map((size) => (
                                    <Badge
                                      key={size}
                                      variant={thumbnailSize === size ? "default" : "outline"}
                                      className="cursor-pointer hover:bg-muted capitalize"
                                      onClick={() => setThumbnailSize(size)}
                                    >
                                      {size} (
                                      {size === "small"
                                        ? "64px"
                                        : size === "large"
                                          ? "160px"
                                          : "96px"}
                                      )
                                    </Badge>
                                  ))}
                                </div>
                                <p className="text-sm text-muted-foreground mt-1">
                                  Choose the size for thumbnail images
                                </p>
                              </div>
                            </>
                          )}
                        </div>
                      </CardContent>
                    </Card>
                  )}

                  {/* Display Text Configuration */}
                  {showResults && (
                    <Card className="border-muted">
                      <CardHeader>
                        <CardTitle className="text-base">Display Text</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="space-y-4">
                          <p className="text-sm text-muted-foreground">
                            Select a field to display prominently as the title/primary text in
                            search results
                          </p>

                          <div>
                            <Label>Display Text Field</Label>
                            <p className="text-sm text-muted-foreground mt-1 mb-3">
                              This field will appear larger and bold above other result fields
                            </p>

                            {displayTextField ? (
                              <div className="mb-3">
                                <Badge
                                  variant="secondary"
                                  className="cursor-pointer"
                                  onClick={handleClearDisplayTextField}
                                >
                                  {displayTextField} ×
                                </Badge>
                              </div>
                            ) : (
                              <p className="text-sm text-muted-foreground mb-3">
                                No display text field selected
                              </p>
                            )}

                            <div className="flex flex-wrap gap-2">
                              {availableBasicFields.map((fieldInfo) => (
                                <Badge
                                  key={fieldInfo.fieldName}
                                  variant={
                                    displayTextField === fieldInfo.fieldName ? "default" : "outline"
                                  }
                                  className="cursor-pointer hover:bg-muted"
                                  onClick={() => handleSetDisplayTextField(fieldInfo.fieldName)}
                                  title={`Field: ${fieldInfo.fieldName}\\nSchema type: ${fieldInfo.schemaType}`}
                                >
                                  {displayTextField === fieldInfo.fieldName
                                    ? fieldInfo.displayName
                                    : `+ ${fieldInfo.displayName}`}
                                </Badge>
                              ))}
                            </div>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Facets Configuration Card */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Facets</CardTitle>
                <div className="flex items-center space-x-2">
                  <Switch id="show-facets" checked={showFacets} onCheckedChange={setShowFacets} />
                  <Label htmlFor="show-facets">Enable</Label>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground">
                    Toggle to show or hide the facets component
                  </p>

                  {showFacets && (
                    <>
                      <div className="space-y-2">
                        {facets.map((facet) => (
                          <div
                            key={facet.id}
                            className="flex items-center justify-between p-2 border rounded"
                          >
                            <div>
                              <strong>{facet.title}</strong>
                              <div className="text-sm text-muted-foreground">
                                Fields: {facet.fields.join(", ")}
                              </div>
                            </div>
                            <Button
                              variant="destructive"
                              size="icon"
                              className="h-8 w-8"
                              onClick={() => handleRemoveFacet(facet.id)}
                            >
                              <TrashIcon className="h-4 w-4" />
                            </Button>
                          </div>
                        ))}
                      </div>

                      <div className="flex gap-2">
                        <Input
                          placeholder="Facet title"
                          value={newFacetTitle}
                          onChange={(e) => setNewFacetTitle(e.target.value)}
                          className="flex-1"
                        />
                        <Input
                          placeholder="Field name"
                          value={newFacetField}
                          onChange={(e) => setNewFacetField(e.target.value)}
                          className="flex-1"
                        />
                        <Button onClick={handleAddFacet} size="icon" className="h-10 w-10 shrink-0">
                          <PlusIcon className="h-4 w-4" />
                        </Button>
                      </div>

                      {availableFields.length > 0 && (
                        <div className="flex flex-col gap-2 mt-3">
                          <Label className="text-sm text-muted-foreground">
                            Available fields from schema:
                          </Label>
                          <div className="flex flex-wrap gap-2">
                            {availableFields.map((fieldInfo) => (
                              <Badge
                                key={fieldInfo.searchField}
                                variant="outline"
                                className="cursor-pointer hover:bg-muted"
                                onClick={() => setNewFacetField(fieldInfo.searchField)}
                                title={`Search field: ${fieldInfo.searchField}\nSchema types: ${fieldInfo.schemaTypes.join(", ")}\nAntfly types: ${fieldInfo.antflyTypes.join(", ")}`}
                              >
                                {fieldInfo.displayName}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
