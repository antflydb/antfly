import type { IndexStatus, QueryRequest, QueryResult } from "@antfly/sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import type React from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { api, type TableSchema } from "../api";
import AggregationResults from "../components/AggregationResults";
import AIQueryAssistant from "../components/AIQueryAssistant";
import CreateIndexDialog from "../components/CreateIndexDialog";
import DocumentBuilder from "../components/DocumentBuilder";

import BulkInsert from "../components/Insert";
import JsonViewer from "../components/JsonViewer";
import MultiSelect from "../components/MultiSelect";
import FieldSelector from "../components/querybuilder/FieldSelector";
import QueryBuilder from "../components/querybuilder/QueryBuilder";
import { QueryResultsList } from "../components/results";
import SearchBoxBuilder from "../components/SearchBoxBuilder";
import DocumentSchemasForm from "../components/schema-builder/DocumentSchemasForm";
import {
  type BasicField,
  generateBasicFields,
  generateSearchableFields,
  type SearchableField,
} from "../utils/fieldUtils";

const formatBytes = (bytes: number, decimals = 2) => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / k ** i).toFixed(dm))} ${sizes[i]}`;
};

function Row(props: {
  index: IndexStatus;
  handleOpenDropDialog: (index: IndexStatus) => void;
  theme: string;
}) {
  const { index, handleOpenDropDialog } = props;

  // Extract version from bleve_v2 index names (e.g., "full_text_index_v0" -> "0")
  const getVersion = (name: string) => {
    const match = name.match(/_v(\d+)$/);
    return match ? match[1] : null;
  };

  const version = index.config.type === "full_text" ? getVersion(index.config.name) : null;

  // Extract model and provider for vector indexes
  const getModelInfo = () => {
    if (index.config.type === "embeddings") {
      const embedderConfig = (index.config as { embedder?: { model?: string; provider?: string } })
        .embedder;
      return {
        model: embedderConfig?.model || "N/A",
        provider: embedderConfig?.provider || "N/A",
      };
    }
    return null;
  };

  const modelInfo = getModelInfo();

  return (
    <TableRow>
      {index.config.type !== "full_text" && <TableCell>{index.config.name}</TableCell>}
      {index.config.type === "full_text" && version && <TableCell>{version}</TableCell>}
      {index.config.type === "embeddings" && modelInfo && (
        <>
          <TableCell>{modelInfo.provider}</TableCell>
          <TableCell>{modelInfo.model}</TableCell>
        </>
      )}
      {(index.config.type === "embeddings" || index.config.type === "full_text") && (
        <TableCell>
          {"total_indexed" in (index.status || {})
            ? (index.status as { total_indexed?: number }).total_indexed
            : "N/A"}
        </TableCell>
      )}
      {index.config.type === "full_text" && (
        <TableCell>
          {"disk_usage" in (index.status || {}) &&
          (index.status as { disk_usage?: number }).disk_usage !== undefined
            ? formatBytes((index.status as { disk_usage: number }).disk_usage)
            : "N/A"}
        </TableCell>
      )}
      <TableCell>
        <Sheet>
          <SheetTrigger asChild>
            <Button variant="outline">Details</Button>
          </SheetTrigger>
          <SheetContent>
            <SheetHeader>
              <SheetTitle>Index Details</SheetTitle>
            </SheetHeader>
            <div className="space-y-4">
              <div>
                <h3 className="text-lg font-semibold mb-2">Status</h3>
                <JsonViewer json={index.status} />
              </div>
              <div>
                <h3 className="text-lg font-semibold mb-2">Config</h3>
                <JsonViewer json={index.config} />
              </div>
            </div>
          </SheetContent>
        </Sheet>
        <Button
          color="red"
          onClick={() => handleOpenDropDialog(index)}
          disabled={index.config.name.startsWith("full_text_index")}
          className="ml-2"
        >
          Drop
        </Button>
      </TableCell>
    </TableRow>
  );
}

interface TableDetailsPageProps {
  currentSection?: string;
  onSectionChange?: (section: string) => void;
}

const TableDetailsPage: React.FC<TableDetailsPageProps> = ({ currentSection = "indexes" }) => {
  const theme = localStorage.getItem("theme") || "light";
  const { tableName } = useParams<{ tableName: string }>();
  const [indexes, setIndexes] = useState<IndexStatus[]>([]);
  const [tableSchema, setTableSchema] = useState<TableSchema | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openCreateDialog, setOpenCreateDialog] = useState(false);
  const [openDropDialog, setOpenDropDialog] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState<IndexStatus | null>(null);
  const [query, setQuery] = useState("");
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [queryIndexes, setQueryIndexes] = useState<string[]>([]);
  const [filterQuery, setFilterQuery] = useState(JSON.stringify({}, null, 2));
  const [semanticQuery, setSemanticQuery] = useState(JSON.stringify({}, null, 2));
  const [selectedFields, setSelectedFields] = useState<string[]>([]);

  // Derive search modes from input content instead of toggles
  const hasSemanticQuery = query.trim().length > 0 && queryIndexes.length > 0;
  const hasFilterQuery = useMemo(() => {
    try {
      const parsed = JSON.parse(filterQuery);
      return Object.keys(parsed).length > 0;
    } catch {
      return false;
    }
  }, [filterQuery]);
  const [fieldInput, setFieldInput] = useState("");
  const [isEditingSchema, setIsEditingSchema] = useState(false);

  const [queryMode, setQueryMode] = useState<"builder" | "json">("builder");

  // Auto-select first vector index when indexes load
  useEffect(() => {
    if (queryIndexes.length === 0) {
      const vectorIndexes = indexes.filter((idx) => idx.config.type === "embeddings");
      if (vectorIndexes.length > 0) {
        setQueryIndexes([vectorIndexes[0].config.name]);
      }
    }
  }, [indexes, queryIndexes.length]);

  const semanticQueryRequestString = useMemo(() => {
    const queryRequest: QueryRequest = {};
    if (hasSemanticQuery) {
      queryRequest.indexes = queryIndexes;
      queryRequest.semantic_search = query || "";
    }
    if (selectedFields.length > 0) {
      queryRequest.fields = selectedFields;
    }
    try {
      const semanticQueryObject = JSON.parse(semanticQuery);
      queryRequest.aggregations = semanticQueryObject.aggregations;
      // Use explicit limit if set, otherwise default to 10
      queryRequest.limit = semanticQueryObject.limit ?? 10;
      // Only include offset if semantic search is disabled
      if (!hasSemanticQuery && semanticQueryObject.offset !== undefined) {
        queryRequest.offset = semanticQueryObject.offset;
      }
    } catch (e) {
      // ignore invalid json - still use default limit
      queryRequest.limit = 10;
      console.error("Invalid semantic query JSON:", e);
    }
    if (hasFilterQuery) {
      try {
        queryRequest.filter_query = JSON.parse(filterQuery);
      } catch (e) {
        // ignore invalid json
        console.error("Invalid filter query JSON:", e);
      }
    }
    return JSON.stringify(queryRequest, null, 2);
  }, [
    query,
    queryIndexes,
    filterQuery,
    semanticQuery,
    hasSemanticQuery,
    hasFilterQuery,
    selectedFields,
  ]);

  const [queryJsonString, setQueryJsonString] = useState(semanticQueryRequestString);

  const semanticQueryRequest = useMemo(() => {
    try {
      return JSON.parse(semanticQueryRequestString);
    } catch {
      return {};
    }
  }, [semanticQueryRequestString]);

  const handleQueryModeChange = (v: string) => {
    const mode = v as "builder" | "json";
    if (mode === "json") {
      setQueryJsonString(semanticQueryRequestString);
    } else if (mode === "builder") {
      try {
        const queryRequest = JSON.parse(queryJsonString);
        setQueryIndexes(queryRequest.indexes || []);
        setSelectedFields(queryRequest.fields || []);
        setFieldInput(""); // Clear field input when switching from JSON mode

        // Set query content (search mode is auto-detected from content)
        setQuery(queryRequest.semantic_search || "");

        // Set filter query content
        if (queryRequest.filter_query) {
          setFilterQuery(JSON.stringify(queryRequest.filter_query, null, 2));
        } else {
          setFilterQuery(JSON.stringify({}, null, 2));
        }
        const { aggregations, limit, offset } = queryRequest;
        const semanticPart: {
          aggregations?: unknown;
          limit?: unknown;
          offset?: unknown;
        } = {};
        if (aggregations) semanticPart.aggregations = aggregations;
        if (limit !== undefined) semanticPart.limit = limit;
        if (offset !== undefined) semanticPart.offset = offset;
        setSemanticQuery(JSON.stringify(semanticPart, null, 2));
        setError(null);
      } catch (e) {
        setError("Invalid JSON in query editor. Please fix it before switching to builder mode.");
        console.error("Invalid JSON in full query editor:", e);
        return;
      }
    }
    setQueryMode(mode);
  };

  const fetchIndexes = useCallback(async () => {
    if (!tableName) return;
    try {
      const response = await api.indexes.list(tableName);
      setIndexes(response as IndexStatus[]);
    } catch (e) {
      setError(`Failed to fetch indexes for table ${tableName}.`);
      console.error(e);
    }
  }, [tableName]);

  const fetchTableSchema = useCallback(async () => {
    if (!tableName) return;
    try {
      const response = (await api.tables.get(tableName)) as {
        schema: TableSchema;
      };
      if (response.schema && Object.keys(response.schema).length > 0) {
        setTableSchema(response.schema);
      } else {
        setTableSchema(null);
      }
    } catch {
      // This is a 404, so we can ignore it.
      setTableSchema(null);
    }
  }, [tableName]);

  useEffect(() => {
    fetchIndexes();
    fetchTableSchema();
  }, [fetchIndexes, fetchTableSchema]);

  // Reset editing state when switching tables
  useEffect(() => {
    setIsEditingSchema(false);
  }, []);

  const handleOpenCreateDialog = () => {
    setOpenCreateDialog(true);
  };

  const handleCloseCreateDialog = () => {
    setOpenCreateDialog(false);
  };

  const handleIndexCreated = () => {
    fetchIndexes();
  };

  const handleOpenDropDialog = (index: IndexStatus) => {
    setSelectedIndex(index);
    setOpenDropDialog(true);
  };

  const handleCloseDropDialog = () => {
    setSelectedIndex(null);
    setOpenDropDialog(false);
  };
  const handleDropIndex = async () => {
    if (!tableName || !selectedIndex) return;
    try {
      await api.indexes.drop(tableName, selectedIndex.config.name);
      fetchIndexes();
      handleCloseDropDialog();
    } catch (e) {
      setError(`Failed to drop index ${selectedIndex.config.name}.`);
      console.error(e);
    }
  };

  const handleQueryChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setQuery(event.target.value);
  };

  const handleQueryIndexChange = (value: string[]) => {
    setQueryIndexes(value);
  };

  const handleRunQuery = useCallback(async () => {
    if (!tableName) return;
    try {
      const queryRequest =
        queryMode === "json" ? JSON.parse(queryJsonString) : semanticQueryRequest;
      const response = await api.tables.query(tableName, queryRequest);
      setQueryResult(response?.responses?.[0] || null);
    } catch (e) {
      setError(`Failed to run query on table ${tableName}.`);
      console.error(e);
    }
  }, [tableName, queryMode, queryJsonString, semanticQueryRequest]);

  // Global Ctrl+Enter handler for search section
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (currentSection !== "semantic") return;
      if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        handleRunQuery();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [currentSection, handleRunQuery]);

  const groupedIndexes = indexes.reduce(
    (acc, index) => {
      const type = index.config.type;
      if (!acc[type]) {
        acc[type] = [];
      }
      acc[type].push(index);
      return acc;
    },
    {} as Record<string, IndexStatus[]>
  );

  const sortedIndexTypes = Object.keys(groupedIndexes).sort();
  const indexTypeDisplayNames: Record<string, string> = {
    embeddings: "Vector Indexes",
    full_text: "Full Text Index",
  };

  const handleFieldInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setFieldInput(event.target.value);
  };

  const handleFieldInputKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter" && fieldInput.trim()) {
      event.preventDefault();
      const newField = fieldInput.trim();
      if (!selectedFields.includes(newField)) {
        setSelectedFields([...selectedFields, newField]);
      }
      setFieldInput("");
    }
  };

  const handleRemoveField = (fieldToRemove: string) => {
    setSelectedFields(selectedFields.filter((field) => field !== fieldToRemove));
  };

  const handleAddAvailableField = (field: string) => {
    if (!selectedFields.includes(field)) {
      setSelectedFields([...selectedFields, field]);
    }
  };

  const handleUpdateSchema = async (schema: Omit<TableSchema, "key"> & { key?: string }) => {
    if (!tableName) return;
    try {
      const schemaWithVersion = {
        version: 0, // Default version to 0 if not specified
        ...schema,
      };
      await api.tables.updateSchema(tableName, schemaWithVersion);
      fetchTableSchema();
      setIsEditingSchema(false);
    } catch (error) {
      setError(`Failed to update schema for table ${tableName}.`);
      console.error(error);
    }
  };

  // Extract available searchable field variations for QueryBuilder
  const availableSearchableFields = useMemo(() => {
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

  // Extract basic fields for simple field selection (no variations)
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

  return (
    <div>
      {error && <p className="text-red-500">{error}</p>}
      <div className="space-y-6">
        {/* Indexes Section */}
        {currentSection === "indexes" && (
          <div className="flex flex-col gap-3">
            <div className="flex gap-2">
              <Button onClick={handleOpenCreateDialog}>Create Index</Button>
              <Button onClick={fetchIndexes} variant="outline" size="icon">
                <ReloadIcon />
              </Button>
            </div>
            {sortedIndexTypes.map((type) => (
              <div key={type}>
                <h3 className="text-xl font-semibold mt-4 mb-2">
                  {" "}
                  {indexTypeDisplayNames[type] || type}
                </h3>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{type === "full_text" ? "Version" : "Name"}</TableHead>
                      {type === "embeddings" && (
                        <>
                          <TableHead>Provider</TableHead>
                          <TableHead>Model</TableHead>
                        </>
                      )}
                      {(type === "embeddings" || type === "full_text") && (
                        <TableHead>Total Indexed</TableHead>
                      )}
                      {type === "full_text" && <TableHead>Disk Usage</TableHead>}
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {groupedIndexes[type].map((index) => (
                      <Row
                        key={index.config.name}
                        index={index}
                        handleOpenDropDialog={handleOpenDropDialog}
                        theme={theme}
                      />
                    ))}
                  </TableBody>
                </Table>
              </div>
            ))}
          </div>
        )}

        {/* Search Section */}
        {currentSection === "semantic" && (
          <div className="flex flex-col gap-6">
            <h2 className="text-2xl font-bold">Query Builder</h2>
            <Tabs value={queryMode} onValueChange={(v) => handleQueryModeChange(v)}>
              <TabsList>
                <TabsTrigger value="builder">Builder</TabsTrigger>
                <TabsTrigger value="json">JSON</TabsTrigger>
              </TabsList>
              <div className="pt-3">
                <TabsContent value="builder" className="space-y-3">
                  <AIQueryAssistant
                    tableName={tableName}
                    schemaFields={availableSearchableFields.map((f) => f.originalField)}
                    currentQuery={(() => {
                      try {
                        return JSON.parse(filterQuery);
                      } catch {
                        return {};
                      }
                    })()}
                    onQueryApplied={(query) => {
                      setFilterQuery(JSON.stringify(query, null, 2));
                    }}
                    onQueryAppliedAndRun={(query) => {
                      setFilterQuery(JSON.stringify(query, null, 2));
                      // Defer run to next tick so state is updated
                      setTimeout(() => handleRunQuery(), 0);
                    }}
                  />

                  <Accordion type="multiple" defaultValue={["semantic"]} className="space-y-2">
                    {/* Field Selection - Collapsible */}
                    <AccordionItem value="fields" className="border rounded-lg bg-card/50 px-3">
                      <AccordionTrigger className="py-2.5 hover:no-underline">
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-sm">Field Selection</span>
                          {selectedFields.length > 0 && (
                            <Badge variant="secondary" className="h-5 text-xs">
                              {selectedFields.length}
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="pb-3 pt-1 space-y-2.5">
                        <Input
                          id="fields-input"
                          placeholder="Type field name and press Enter"
                          value={fieldInput}
                          onChange={handleFieldInputChange}
                          onKeyDown={handleFieldInputKeyDown}
                          className="h-9"
                        />
                        {selectedFields.length > 0 && (
                          <div className="flex flex-wrap gap-1.5">
                            {selectedFields.map((field) => {
                              const fieldInfo = availableBasicFields.find(
                                (f) => f.fieldName === field
                              );
                              return (
                                <Badge
                                  key={field}
                                  variant="secondary"
                                  className="cursor-pointer hover:bg-destructive hover:text-destructive-foreground transition-colors h-6 text-xs"
                                  onClick={() => handleRemoveField(field)}
                                >
                                  {fieldInfo?.displayName || field} ×
                                </Badge>
                              );
                            })}
                          </div>
                        )}
                        <FieldSelector
                          availableFields={availableBasicFields.filter(
                            (f) => !selectedFields.includes(f.fieldName)
                          )}
                          onFieldSelect={handleAddAvailableField}
                        />
                      </AccordionContent>
                    </AccordionItem>

                    {/* Semantic Search */}
                    <AccordionItem value="semantic" className="border rounded-lg bg-card/50 px-3">
                      <AccordionTrigger className="py-2.5 hover:no-underline">
                        <span className="font-medium text-sm">Semantic Search</span>
                      </AccordionTrigger>
                      <AccordionContent className="pb-3 pt-1">
                        <div className="space-y-2.5">
                          <div>
                            <Label className="text-xs mb-1 block">Vector Index</Label>
                            {indexes.filter((idx) => idx.config.type === "embeddings").length ===
                            0 ? (
                              <p className="text-xs text-muted-foreground">
                                No vector indexes available. Create one to enable semantic search.
                              </p>
                            ) : (
                              <MultiSelect
                                options={indexes
                                  .filter((idx) => idx.config.type === "embeddings")
                                  .map((index) => ({
                                    label: index.config.name,
                                    value: index.config.name,
                                  }))}
                                value={queryIndexes}
                                onChange={handleQueryIndexChange}
                                placeholder="Select vector index(es)"
                              />
                            )}
                          </div>
                          {queryIndexes.length > 1 && (
                            <Alert className="py-1.5 px-3">
                              <AlertDescription className="text-xs">
                                RRF search with multiple indexes.{" "}
                                <a
                                  href="https://learn.microsoft.com/en-us/azure/search/hybrid-search-ranking"
                                  target="_blank"
                                  rel="noreferrer"
                                  className="underline"
                                >
                                  Learn more
                                </a>
                              </AlertDescription>
                            </Alert>
                          )}
                          <div>
                            <Label className="text-xs mb-1 block">Query</Label>
                            <Input
                              placeholder="Enter search query..."
                              value={query}
                              onChange={handleQueryChange}
                              onKeyDown={(e) => {
                                if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
                                  e.preventDefault();
                                  handleRunQuery();
                                }
                              }}
                              className="h-9"
                            />
                          </div>
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Full-Text Search */}
                    <AccordionItem value="filter" className="border rounded-lg bg-card/50 px-3">
                      <AccordionTrigger className="py-2.5 hover:no-underline">
                        <span className="font-medium text-sm">Full-Text Search</span>
                      </AccordionTrigger>
                      <AccordionContent className="pb-3 pt-1">
                        <QueryBuilder
                          value={filterQuery}
                          onChange={setFilterQuery}
                          showOrderByAndFacets={false}
                          availableFields={availableSearchableFields}
                          availableBasicFields={availableBasicFields}
                        />
                      </AccordionContent>
                    </AccordionItem>
                  </Accordion>

                  <QueryBuilder
                    value={semanticQuery}
                    onChange={setSemanticQuery}
                    showQueryNode={false}
                    showLimitAndOffset={true}
                    disableOffset={hasSemanticQuery}
                    availableFields={availableSearchableFields}
                    availableBasicFields={availableBasicFields}
                  />
                </TabsContent>
                <TabsContent value="json">
                  {(() => {
                    let jsonObject: unknown;
                    let parseError = false;
                    try {
                      jsonObject = JSON.parse(queryJsonString);
                    } catch {
                      parseError = true;
                    }

                    if (parseError) {
                      return (
                        <div className="flex flex-col gap-2">
                          <p className="text-red-500">
                            The current query is not valid JSON. Please correct it.
                          </p>
                          <Textarea
                            value={queryJsonString}
                            onChange={(event: React.ChangeEvent<HTMLTextAreaElement>) =>
                              setQueryJsonString(event.target.value)
                            }
                            rows={20}
                            className="font-mono"
                          />
                        </div>
                      );
                    }

                    return <JsonViewer json={jsonObject as object} />;
                  })()}
                </TabsContent>
              </div>
            </Tabs>

            <div className="flex gap-3 items-center mt-6">
              <Button
                onClick={handleRunQuery}
                disabled={!hasSemanticQuery && !hasFilterQuery}
                size="lg"
              >
                Run Query
              </Button>
              <span className="text-xs text-muted-foreground">
                {hasSemanticQuery && hasFilterQuery
                  ? "Running semantic + full-text search"
                  : hasSemanticQuery
                    ? "Running semantic search"
                    : hasFilterQuery
                      ? "Running full-text search"
                      : "Enter a query to search"}
              </span>
            </div>

            {queryResult?.aggregations && Object.keys(queryResult.aggregations).length > 0 && (
              <AggregationResults aggregations={queryResult.aggregations} className="mt-6" />
            )}

            {queryResult && (
              <Card className="mt-6 shadow-sm">
                <CardHeader>
                  <CardTitle>Query Results</CardTitle>
                </CardHeader>
                <CardContent>
                  <QueryResultsList result={queryResult} />
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* SearchBox Builder Section */}
        {currentSection === "faceted" && (
          <SearchBoxBuilder
            tableName={tableName || ""}
            tableSchema={tableSchema || undefined}
            indexes={indexes}
          />
        )}

        {/* Upload Section */}
        {currentSection === "bulk" && <BulkInsert tableName={tableName || ""} />}

        {/* Document Builder Section */}
        {currentSection === "document-builder" && (
          <DocumentBuilder tableName={tableName || ""} schema={tableSchema} />
        )}

        {/* Schema Section */}
        {currentSection === "schema" && (
          <div className="flex flex-col gap-4">
            <div className="flex justify-between items-center">
              <h3 className="text-lg font-semibold">Table Schema</h3>
              <Button
                onClick={() => setIsEditingSchema(!isEditingSchema)}
                variant={isEditingSchema ? "destructive" : "default"}
              >
                {isEditingSchema ? "Cancel" : "Edit Schema"}
              </Button>
            </div>

            {isEditingSchema ? (
              <div>
                <DocumentSchemasForm
                  onSubmit={handleUpdateSchema}
                  theme={theme}
                  initialSchema={tableSchema}
                  tableName={tableName}
                />
              </div>
            ) : tableSchema?.document_schemas &&
              Object.keys(tableSchema.document_schemas).length > 0 ? (
              <JsonViewer json={tableSchema} />
            ) : (
              <DocumentSchemasForm
                onSubmit={handleUpdateSchema}
                theme={theme}
                initialSchema={null}
                tableName={tableName}
              />
            )}
          </div>
        )}
      </div>

      <CreateIndexDialog
        open={openCreateDialog}
        onClose={handleCloseCreateDialog}
        tableName={tableName || ""}
        onIndexCreated={handleIndexCreated}
        schema={tableSchema}
      />
      <Dialog open={openDropDialog} onOpenChange={setOpenDropDialog}>
        <DialogContent className="max-w-[450px]">
          <DialogTitle>Drop Index</DialogTitle>
          <DialogDescription>
            Are you sure you want to drop the index "{selectedIndex?.config.name}"? This action
            cannot be undone.
          </DialogDescription>
          <div className="flex gap-3 mt-4 justify-end">
            <DialogTrigger>
              <Button variant="destructive" color="gray">
                Cancel
              </Button>
            </DialogTrigger>
            <DialogTrigger>
              <Button color="red" onClick={handleDropIndex}>
                Drop
              </Button>
            </DialogTrigger>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default TableDetailsPage;
