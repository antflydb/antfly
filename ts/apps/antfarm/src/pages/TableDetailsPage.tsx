import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
  Alert,
  AlertDescription,
  Badge,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Checkbox,
  type ColumnDef,
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DashboardToolbar,
  DataTable,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
  Input,
  Label,
  MultiSelect,
  MultiSelectContent,
  MultiSelectItem,
  MultiSelectTrigger,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  Textarea,
} from "@antfly/design-system";
import type {
  Table as AntflyTable,
  IndexStatus,
  QueryRequest,
  QueryResult,
  TableStatus,
} from "@antfly/sdk";
import { queryResultTotalHits } from "@antfly/sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import type { TableSchema } from "../api";
import AggregationResults from "../components/AggregationResults";
import AIQueryAssistant from "../components/AIQueryAssistant";
import CreateIndexDialog from "../components/CreateIndexDialog";
import DocumentBuilder from "../components/DocumentBuilder";
import { GraphIndexExplorer } from "../components/GraphIndexExplorer";
import BulkInsert from "../components/Insert";
import JsonViewer from "../components/JsonViewer";
import FieldSelector from "../components/querybuilder/FieldSelector";
import QueryBuilder from "../components/querybuilder/QueryBuilder";
import { QueryResultsList } from "../components/results";
import SearchBoxBuilder from "../components/SearchBoxBuilder";
import DocumentSchemasForm from "../components/schema-builder/DocumentSchemasForm";
import { DocumentArtifactsPanel } from "../components/table/DocumentArtifactsPanel";
import { TableReprocessPanel } from "../components/table/TableReprocessPanel";
import { useApi } from "../hooks/use-api-config";
import {
  type BasicField,
  generateBasicFields,
  generateSearchableFields,
  type SearchableField,
} from "../utils/fieldUtils";
import {
  builderArtifactRetrievalDefaults,
  buildTableQueryRequest,
  parseTableQueryRequest,
  requestArtifactRetrievalDefaults,
  type TableQueryMetadataState,
  tableQueryBuilderConversionBlocker,
  tableQueryErrorMessage,
  tableQueryInput,
  tableQueryJsonSafetyBlocker,
  tableQueryMetadataBlocker,
  tableRequiresSafeProjection,
} from "./table-query";

const formatBytes = (bytes: number, decimals = 2) => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / k ** i).toFixed(dm))} ${sizes[i]}`;
};

function ActionsCell({
  index,
  onDrop,
}: {
  index: IndexStatus;
  onDrop: (index: IndexStatus) => void;
}) {
  return (
    <div className="flex items-center gap-2">
      <Sheet>
        <SheetTrigger asChild>
          <Button variant="outline" size="sm">
            Details
          </Button>
        </SheetTrigger>
        <SheetContent>
          <SheetHeader>
            <SheetTitle>Index Details</SheetTitle>
          </SheetHeader>
          <div className="space-y-4">
            <div>
              <h3 className="mb-2 font-semibold">Status</h3>
              <JsonViewer json={index.status} />
            </div>
            <div>
              <h3 className="mb-2 font-semibold">Config</h3>
              <JsonViewer json={index.config} />
            </div>
          </div>
        </SheetContent>
      </Sheet>
      <Button
        variant="ghost"
        size="sm"
        onClick={() => onDrop(index)}
        disabled={index.config.name.startsWith("full_text_index")}
        className="text-muted-foreground hover:text-destructive"
      >
        Drop
      </Button>
    </div>
  );
}

function getIndexVersion(name: string) {
  const match = name.match(/_v(\d+)$/);
  return match ? match[1] : null;
}

function getModelInfo(index: IndexStatus) {
  if (index.config.type === "embeddings") {
    const embedderConfig = (index.config as { embedder?: { model?: string; provider?: string } })
      .embedder;
    return {
      model: embedderConfig?.model || "N/A",
      provider: embedderConfig?.provider || "N/A",
    };
  }
  return null;
}

function getTotalIndexed(index: IndexStatus) {
  if ("total_indexed" in (index.status || {})) {
    return (index.status as { total_indexed?: number }).total_indexed ?? "N/A";
  }
  return "N/A";
}

function buildIndexColumns(
  type: string,
  onDrop: (index: IndexStatus) => void
): ColumnDef<IndexStatus>[] {
  const cols: ColumnDef<IndexStatus>[] = [];

  if (type === "full_text") {
    cols.push({
      id: "version",
      header: "Version",
      cell: ({ row }) => getIndexVersion(row.original.config.name) ?? "-",
    });
    cols.push({
      id: "total_indexed",
      header: "Total Indexed",
      cell: ({ row }) => getTotalIndexed(row.original),
    });
    cols.push({
      id: "disk_usage",
      header: "Disk Usage",
      cell: ({ row }) => {
        const status = row.original.status as { disk_usage?: number } | undefined;
        return status?.disk_usage !== undefined ? formatBytes(status.disk_usage) : "N/A";
      },
    });
  } else if (type === "embeddings") {
    cols.push({
      accessorFn: (row) => row.config.name,
      id: "name",
      header: "Name",
    });
    cols.push({
      id: "provider",
      header: "Provider",
      cell: ({ row }) => getModelInfo(row.original)?.provider ?? "N/A",
    });
    cols.push({
      id: "model",
      header: "Model",
      cell: ({ row }) => getModelInfo(row.original)?.model ?? "N/A",
    });
    cols.push({
      id: "total_indexed",
      header: "Total Indexed",
      cell: ({ row }) => getTotalIndexed(row.original),
    });
  } else {
    cols.push({
      accessorFn: (row) => row.config.name,
      id: "name",
      header: "Name",
    });
  }

  cols.push({
    id: "actions",
    header: "",
    cell: ({ row }) => <ActionsCell index={row.original} onDrop={onDrop} />,
  });

  return cols;
}

interface TableDetailsPageProps {
  currentSection?: string;
}

const TableDetailsPage: React.FC<TableDetailsPageProps> = ({ currentSection = "overview" }) => {
  const theme = localStorage.getItem("theme") || "light";
  const client = useApi();
  const { tableName } = useParams<{ tableName: string }>();
  const activeTableName = useRef(tableName);
  activeTableName.current = tableName;
  const indexesRequestSequence = useRef(0);
  const tableMetadataRequestSequence = useRef(0);
  const artifactSourcesCapabilityRequestSequence = useRef(0);
  const queryAbortController = useRef<AbortController | null>(null);
  const navigate = useNavigate();
  const [indexes, setIndexes] = useState<IndexStatus[]>([]);
  const [indexesMetadataState, setIndexesMetadataState] =
    useState<TableQueryMetadataState>("loading");
  const [tableStatus, setTableStatus] = useState<TableStatus | null>(null);
  const [tableMetadataState, setTableMetadataState] = useState<TableQueryMetadataState>("loading");
  const [tableSchema, setTableSchema] = useState<TableSchema | null>(null);
  const [storageStatus, setStorageStatus] = useState<TableStatus["storage_status"] | null>(null);
  const [documentCount, setDocumentCount] = useState<number | null>(null);
  const [migration, setMigration] = useState<AntflyTable["migration"]>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [openCreateDialog, setOpenCreateDialog] = useState(false);
  const [artifactSourcesSupported, setArtifactSourcesSupported] = useState<boolean | undefined>(
    undefined
  );
  const [artifactSourcesCapabilityError, setArtifactSourcesCapabilityError] = useState(false);
  const [openDropDialog, setOpenDropDialog] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState<IndexStatus | null>(null);
  const [query, setQuery] = useState("");
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [queryIndexes, setQueryIndexes] = useState<string[]>([]);
  const [fullTextIndex, setFullTextIndex] = useState("");
  const [filterQuery, setFilterQuery] = useState(JSON.stringify({}, null, 2));
  const [semanticQuery, setSemanticQuery] = useState(JSON.stringify({}, null, 2));
  const [selectedFields, setSelectedFields] = useState<string[]>([]);
  const [includeProfile, setIncludeProfile] = useState(false);

  const refreshArtifactSourcesCapability = useCallback(() => {
    const requestSequence = ++artifactSourcesCapabilityRequestSequence.current;
    setArtifactSourcesSupported(undefined);
    setArtifactSourcesCapabilityError(false);
    void client
      .getStatus()
      .then((status) => {
        if (requestSequence === artifactSourcesCapabilityRequestSequence.current) {
          setArtifactSourcesSupported(status.index_capabilities?.artifact_sources === true);
          setArtifactSourcesCapabilityError(false);
        }
      })
      .catch(() => {
        if (requestSequence === artifactSourcesCapabilityRequestSequence.current) {
          setArtifactSourcesSupported(undefined);
          setArtifactSourcesCapabilityError(true);
        }
      });
  }, [client]);

  useEffect(() => {
    refreshArtifactSourcesCapability();
    return () => {
      artifactSourcesCapabilityRequestSequence.current++;
    };
  }, [refreshArtifactSourcesCapability]);

  // Derive search modes from input content instead of toggles
  const hasSemanticQuery = query.trim().length > 0 && queryIndexes.length > 0;
  const hasFullTextQuery = query.trim().length > 0 && !hasSemanticQuery;
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

  const artifactRetrieval = useMemo(
    () =>
      builderArtifactRetrievalDefaults(indexes, query, queryIndexes, tableStatus, fullTextIndex),
    [indexes, query, queryIndexes, tableStatus, fullTextIndex]
  );
  const artifactSelectionError = hasSemanticQuery ? artifactRetrieval?.selectionError : undefined;
  const queryMetadataBlocker = tableQueryMetadataBlocker(indexesMetadataState, tableMetadataState);
  const artifactPayloadProjectionRequired = useMemo(
    () => tableRequiresSafeProjection(indexes, tableStatus),
    [indexes, tableStatus]
  );

  const semanticQueryRequest = useMemo(() => {
    return buildTableQueryRequest({
      query,
      queryIndexes,
      fullTextIndex,
      selectedFields,
      semanticQuery,
      filterQuery,
      includeProfile,
      artifactSearchFields: artifactRetrieval?.searchFields,
      artifactProjectionFields: artifactRetrieval?.projectionFields,
      returnArtifactMatches: artifactRetrieval?.returnMatches,
      requireSafeProjection: artifactPayloadProjectionRequired,
    });
  }, [
    query,
    queryIndexes,
    fullTextIndex,
    filterQuery,
    semanticQuery,
    selectedFields,
    includeProfile,
    artifactRetrieval,
    artifactPayloadProjectionRequired,
  ]);
  const semanticQueryRequestString = useMemo(
    () => JSON.stringify(semanticQueryRequest, null, 2),
    [semanticQueryRequest]
  );

  const [queryJsonString, setQueryJsonString] = useState(semanticQueryRequestString);
  const parsedJsonQuery = useMemo(() => parseTableQueryRequest(queryJsonString), [queryJsonString]);
  const isJsonQueryValid = parsedJsonQuery !== null;
  const jsonArtifactRetrieval = useMemo(
    () => requestArtifactRetrievalDefaults(indexes, parsedJsonQuery, tableStatus),
    [indexes, parsedJsonQuery, tableStatus]
  );
  const jsonQuerySafetyBlocker = tableQueryJsonSafetyBlocker(
    queryMetadataBlocker,
    artifactPayloadProjectionRequired || jsonArtifactRetrieval !== null,
    parsedJsonQuery
  );

  const handleQueryModeChange = (v: string) => {
    const mode = v as "builder" | "json";
    if (mode === "json") {
      setQueryJsonString(semanticQueryRequestString);
    } else if (mode === "builder") {
      const queryRequest = parseTableQueryRequest(queryJsonString);
      if (queryRequest) {
        const nextQueryIndexes = Array.isArray(queryRequest.indexes)
          ? queryRequest.indexes.filter((index): index is string => typeof index === "string")
          : [];
        const nextFullTextIndex =
          typeof queryRequest.full_text_index === "string" ? queryRequest.full_text_index : "";
        const nextSelectedFields = Array.isArray(queryRequest.fields)
          ? queryRequest.fields.filter((field): field is string => typeof field === "string")
          : [];
        const nextArtifactRetrieval = requestArtifactRetrievalDefaults(
          indexes,
          queryRequest,
          tableStatus
        );
        const nextQuery = tableQueryInput(queryRequest, nextArtifactRetrieval?.searchFields);

        const nextFilterQuery = JSON.stringify(queryRequest.filter_query || {}, null, 2);
        const { aggregations, limit, offset } = queryRequest;
        const semanticPart: {
          aggregations?: unknown;
          limit?: unknown;
          offset?: unknown;
        } = {};
        if (aggregations) semanticPart.aggregations = aggregations;
        if (limit !== undefined) semanticPart.limit = limit;
        if (offset !== undefined) semanticPart.offset = offset;
        const nextSemanticQuery = JSON.stringify(semanticPart, null, 2);
        const rebuilt = buildTableQueryRequest({
          query: nextQuery,
          queryIndexes: nextQueryIndexes,
          fullTextIndex: nextFullTextIndex,
          selectedFields: nextSelectedFields,
          semanticQuery: nextSemanticQuery,
          filterQuery: nextFilterQuery,
          includeProfile: queryRequest.profile === true,
          artifactSearchFields: nextArtifactRetrieval?.searchFields,
          artifactProjectionFields: nextArtifactRetrieval?.projectionFields,
          returnArtifactMatches: nextArtifactRetrieval?.returnMatches,
          requireSafeProjection: artifactPayloadProjectionRequired,
        });
        const conversionBlocker = tableQueryBuilderConversionBlocker(queryRequest, rebuilt);
        if (conversionBlocker) {
          setError(conversionBlocker);
          return;
        }

        setQueryIndexes(nextQueryIndexes);
        setFullTextIndex(nextFullTextIndex);
        setSelectedFields(nextSelectedFields);
        setIncludeProfile(queryRequest.profile === true);
        setFieldInput("");
        setQuery(nextQuery);
        setFilterQuery(nextFilterQuery);
        setSemanticQuery(nextSemanticQuery);
        setError(null);
      } else {
        setError("The query editor must contain one JSON object.");
        return;
      }
    }
    setQueryMode(mode);
  };

  const fetchIndexes = useCallback(async () => {
    if (!tableName) return;
    if (activeTableName.current !== tableName) return;
    const requestSequence = ++indexesRequestSequence.current;
    setIndexesMetadataState("loading");
    try {
      const response = await client.indexes.list(tableName);
      if (
        activeTableName.current !== tableName ||
        requestSequence !== indexesRequestSequence.current
      )
        return;
      if (!Array.isArray(response)) throw new Error("Index metadata response was empty.");
      const nextIndexes = response as IndexStatus[];
      const liveVectorIndexes = new Set(
        nextIndexes
          .filter((index) => index.config.type === "embeddings")
          .map((index) => index.config.name)
      );
      const liveFullTextIndexes = new Set(
        nextIndexes
          .filter((index) => index.config.type === "full_text")
          .map((index) => index.config.name)
      );
      setIndexes(nextIndexes);
      setQueryIndexes((current) => current.filter((name) => liveVectorIndexes.has(name)));
      setFullTextIndex((current) => (current && liveFullTextIndexes.has(current) ? current : ""));
      setIndexesMetadataState("ready");
    } catch (e) {
      if (
        activeTableName.current !== tableName ||
        requestSequence !== indexesRequestSequence.current
      )
        return;
      setIndexesMetadataState("error");
      setError(`Failed to fetch indexes for table ${tableName}.`);
      console.error(e);
    }
  }, [client, tableName]);

  const fetchTableSchema = useCallback(async () => {
    if (!tableName) return;
    if (activeTableName.current !== tableName) return;
    const requestSequence = ++tableMetadataRequestSequence.current;
    setTableMetadataState("loading");
    try {
      const response = await client.tables.get(tableName);
      if (
        activeTableName.current !== tableName ||
        requestSequence !== tableMetadataRequestSequence.current
      )
        return;
      if (!response) throw new Error("Table metadata response was empty.");
      setTableStatus(response as TableStatus);
      if (response?.schema && Object.keys(response.schema).length > 0) {
        setTableSchema(response.schema as TableSchema);
      } else {
        setTableSchema(null);
      }
      setStorageStatus((response as TableStatus | undefined)?.storage_status ?? null);
      setMigration(response?.migration);
      setTableMetadataState("ready");
    } catch (e) {
      if (
        activeTableName.current !== tableName ||
        requestSequence !== tableMetadataRequestSequence.current
      )
        return;
      // Keep the last known-good metadata. Clearing it here can silently remove
      // artifact projections and turn a bounded query into a source rollup.
      setTableMetadataState("error");
      setError(tableQueryErrorMessage(e, `Failed to fetch table metadata for ${tableName}.`));
      console.error(e);
    }
  }, [client, tableName]);

  const fetchDocumentCount = useCallback(async () => {
    if (!tableName) return;
    if (storageStatus?.empty) {
      setDocumentCount(0);
      return;
    }
    try {
      const response = await client.tables.query(tableName, {
        filter_query: { match_all: {} },
        count: true,
        limit: 0,
      } as QueryRequest);
      if (activeTableName.current !== tableName) return;
      setDocumentCount(queryResultTotalHits(response?.responses?.[0]) ?? null);
    } catch {
      if (activeTableName.current !== tableName) return;
      setDocumentCount(null);
    }
  }, [client, storageStatus?.empty, tableName]);

  useEffect(() => {
    fetchIndexes();
    fetchTableSchema();
  }, [fetchIndexes, fetchTableSchema]);

  useEffect(() => {
    fetchDocumentCount();
  }, [fetchDocumentCount]);

  const refreshQueryMetadata = useCallback(async () => {
    setError(null);
    await Promise.all([fetchIndexes(), fetchTableSchema()]);
  }, [fetchIndexes, fetchTableSchema]);

  // Reset table-specific editor state when switching tables.
  useEffect(() => {
    if (!tableName) return;
    queryAbortController.current?.abort();
    queryAbortController.current = null;
    setIsEditingSchema(false);
    setIndexes([]);
    setIndexesMetadataState("loading");
    setTableStatus(null);
    setTableMetadataState("loading");
    setQuery("");
    setQueryResult(null);
    setQueryIndexes([]);
    setFullTextIndex("");
    setFilterQuery(JSON.stringify({}, null, 2));
    setSemanticQuery(JSON.stringify({}, null, 2));
    setSelectedFields([]);
    setIncludeProfile(false);
    setQueryMode("builder");
    setError(null);
    return () => queryAbortController.current?.abort();
  }, [tableName]);

  const handleOpenCreateDialog = () => {
    setOpenCreateDialog(true);
  };

  const handleCloseCreateDialog = () => {
    setOpenCreateDialog(false);
  };

  const handleIndexCreated = () => {
    void refreshQueryMetadata();
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
      const droppedIndexName = selectedIndex.config.name;
      await client.indexes.drop(tableName, droppedIndexName);
      setQueryIndexes((current) => current.filter((name) => name !== droppedIndexName));
      setFullTextIndex((current) => (current === droppedIndexName ? "" : current));
      await refreshQueryMetadata();
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

  const handleFullTextIndexChange = (value: string) => {
    setFullTextIndex(value === "__active_schema__" ? "" : value);
  };

  const handleRunQuery = useCallback(async () => {
    if (!tableName) return;
    queryAbortController.current?.abort();
    const controller = new AbortController();
    queryAbortController.current = controller;
    setError(null);
    try {
      if (queryMode === "builder" && queryMetadataBlocker) {
        setError(queryMetadataBlocker);
        return;
      }
      if (queryMode === "builder" && artifactSelectionError) {
        setError(artifactSelectionError);
        return;
      }
      if (queryMode === "json" && jsonQuerySafetyBlocker) {
        setError(jsonQuerySafetyBlocker);
        return;
      }
      const queryRequest = queryMode === "json" ? parsedJsonQuery : semanticQueryRequest;
      if (!queryRequest) {
        setError("The query editor must contain one JSON object.");
        return;
      }
      const response = await client.tables.query(tableName, queryRequest, {
        signal: controller.signal,
      });
      if (
        controller.signal.aborted ||
        queryAbortController.current !== controller ||
        activeTableName.current !== tableName
      )
        return;
      setQueryResult(response?.responses?.[0] || null);
    } catch (e) {
      if (
        controller.signal.aborted ||
        queryAbortController.current !== controller ||
        activeTableName.current !== tableName
      )
        return;
      setError(tableQueryErrorMessage(e, `Failed to run query on table ${tableName}.`));
      console.error(e);
    } finally {
      if (queryAbortController.current === controller) {
        queryAbortController.current = null;
      }
    }
  }, [
    client,
    tableName,
    queryMode,
    parsedJsonQuery,
    semanticQueryRequest,
    artifactSelectionError,
    jsonQuerySafetyBlocker,
    queryMetadataBlocker,
  ]);

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
  const vectorIndexCount = indexes.filter((idx) => idx.config.type === "embeddings").length;
  const fullTextIndexCount = indexes.filter((idx) => idx.config.type === "full_text").length;
  const graphIndexCount = indexes.filter((idx) => idx.config.type === "graph").length;
  const hasGraphCapability = graphIndexCount > 0;
  const hasData = documentCount !== 0 && storageStatus?.empty !== true;
  const isRetrievalReady = hasData && indexes.length > 0;
  const selectedVectorIndexes = indexes.filter((index) => queryIndexes.includes(index.config.name));
  const schemaCount = tableSchema?.document_schemas
    ? Object.keys(tableSchema.document_schemas).length
    : 0;
  const documentCountLabel =
    documentCount === null
      ? storageStatus?.empty
        ? "0"
        : "Unknown"
      : Intl.NumberFormat().format(documentCount);
  const tableSectionPath = (section: string) =>
    `/tables/${encodeURIComponent(tableName || "")}?section=${encodeURIComponent(section)}`;

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
      await client.tables.updateSchema(tableName, schemaWithVersion);
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

  const sectionLabels: Record<string, string> = {
    overview: "Overview",
    indexes: "Indexes",
    schema: "Schema",
    semantic: "Search",
    graph: "Graph",
    faceted: "Component Builder",
    bulk: "Upload",
    "document-builder": "Manual Entry",
    artifacts: "Artifacts",
    reprocess: "Reprocess",
  };

  return (
    <DashboardPage>
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink asChild>
              <Link to="/">Tables</Link>
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbLink asChild>
              <Link to={tableSectionPath("overview")}>{tableName}</Link>
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>{sectionLabels[currentSection] ?? currentSection}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>

      <DashboardPageHeader>
        <div>
          <DashboardPageTitle>{tableName}</DashboardPageTitle>
          <DashboardPageDescription>
            {currentSection === "overview"
              ? "Table health, setup status, and next actions."
              : `${sectionLabels[currentSection] ?? currentSection} for this table.`}
          </DashboardPageDescription>
        </div>
        {currentSection === "indexes" && (
          <DashboardPageActions>
            <Button onClick={handleOpenCreateDialog}>Create Index</Button>
            <Button onClick={fetchIndexes} variant="outline" size="icon">
              <ReloadIcon />
            </Button>
          </DashboardPageActions>
        )}
      </DashboardPageHeader>

      {migration && (
        <Alert className="af-status-badge-warning">
          <AlertDescription>
            <span className="font-medium">Schema migration in progress</span> — rebuilding full-text
            indexes. Reads are served from schema v{migration.read_schema.version} while v
            {tableSchema?.version ?? "?"} is being built.
          </AlertDescription>
        </Alert>
      )}
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      <div className="space-y-6">
        {/* Overview Section */}
        {currentSection === "overview" && (
          <div className="space-y-6">
            <div className="grid gap-3 md:grid-cols-5">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Documents</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-semibold">{documentCountLabel}</div>
                  <p className="text-xs text-muted-foreground">
                    {storageStatus?.disk_usage != null
                      ? `${formatBytes(storageStatus.disk_usage)} persisted in LSM runs`
                      : "data count from query API"}
                  </p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Schema</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-semibold">{schemaCount}</div>
                  <p className="text-xs text-muted-foreground">document schemas</p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Indexes</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-semibold">{indexes.length}</div>
                  <p className="text-xs text-muted-foreground">
                    {vectorIndexCount} vector, {fullTextIndexCount} full-text, {graphIndexCount}{" "}
                    graph
                  </p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Migration</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-semibold">{migration ? "Active" : "Idle"}</div>
                  <p className="text-xs text-muted-foreground">
                    {migration ? "schema rebuild in progress" : "no rebuild running"}
                  </p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Retrieval</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-semibold">
                    {isRetrievalReady ? "Ready" : "Setup"}
                  </div>
                  <p className="text-xs text-muted-foreground">
                    {isRetrievalReady
                      ? "search and ask are available"
                      : !hasData
                        ? "add data before retrieval"
                        : "create an index first"}
                  </p>
                </CardContent>
              </Card>
            </div>

            <DashboardToolbar className="flex-row flex-wrap items-center gap-2 md:items-center">
              <Button onClick={() => navigate(tableSectionPath("bulk"))}>Upload data</Button>
              <Button variant="outline" onClick={handleOpenCreateDialog}>
                Create index
              </Button>
              <Button variant="outline" onClick={() => navigate(tableSectionPath("semantic"))}>
                Search
              </Button>
              <Button
                variant="outline"
                onClick={() =>
                  navigate(`/data/playground/chat?table=${encodeURIComponent(tableName || "")}`)
                }
              >
                Ask
              </Button>
            </DashboardToolbar>

            <div className="grid gap-4 lg:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>Setup Path</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">1. Define the table shape</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => navigate(tableSectionPath("schema"))}
                    >
                      {schemaCount > 0 ? "View schema" : "Define schema"}
                    </Button>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">2. Add documents</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => navigate(tableSectionPath("bulk"))}
                    >
                      {hasData ? "Upload more" : "Upload data"}
                    </Button>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">3. Build retrieval indexes</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => navigate(tableSectionPath("indexes"))}
                    >
                      {indexes.length > 0 ? "Manage indexes" : "Create index"}
                    </Button>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">4. Search or ask</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => navigate(tableSectionPath("semantic"))}
                      disabled={!isRetrievalReady}
                    >
                      Search
                    </Button>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Retrieval Readiness</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3 text-sm">
                  {!hasData && (
                    <Alert>
                      <AlertDescription>
                        This table does not appear to have documents yet. Upload data or add a
                        document manually before evaluating retrieval.
                      </AlertDescription>
                    </Alert>
                  )}
                  {hasData && indexes.length === 0 && (
                    <Alert>
                      <AlertDescription>
                        This table has data but no indexes. Create a full-text or vector index to
                        unlock Search and Ask.
                      </AlertDescription>
                    </Alert>
                  )}
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Search ranked documents</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => navigate(tableSectionPath("semantic"))}
                      disabled={!isRetrievalReady}
                    >
                      Search
                    </Button>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Ask questions over this table</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        navigate(
                          `/data/playground/chat?table=${encodeURIComponent(tableName || "")}`
                        )
                      }
                      disabled={!isRetrievalReady}
                    >
                      Ask
                    </Button>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Measure retrieval quality</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        navigate(
                          `/data/playground/evals?table=${encodeURIComponent(tableName || "")}`
                        )
                      }
                      disabled={!isRetrievalReady}
                    >
                      Evaluate
                    </Button>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Explore graph relationships</span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => navigate(tableSectionPath("graph"))}
                      disabled={!hasGraphCapability}
                    >
                      {hasGraphCapability ? "Graph" : "Needs graph index"}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        )}

        {/* Indexes Section */}
        {currentSection === "indexes" && (
          <div className="flex flex-col gap-6">
            {sortedIndexTypes.map((type) => (
              <div key={type} className="space-y-2">
                <h3 className="font-semibold">{indexTypeDisplayNames[type] || type}</h3>
                <DataTable
                  columns={buildIndexColumns(type, handleOpenDropDialog)}
                  data={groupedIndexes[type]}
                  emptyMessage="No indexes."
                />
              </div>
            ))}
          </div>
        )}

        {/* Search Section */}
        {currentSection === "semantic" && (
          <div className="flex flex-col gap-6">
            <DashboardToolbar>
              <h2>Query Builder</h2>
            </DashboardToolbar>
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
                    <AccordionItem value="fields" className="border rounded-none bg-card/50 px-3">
                      <AccordionTrigger className="py-2.5 hover:no-underline">
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-sm">Field Selection</span>
                          {selectedFields.length > 0 && (
                            <Badge className="h-5 text-xs">{selectedFields.length}</Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="pb-3 pt-1 space-y-2.5">
                        {artifactPayloadProjectionRequired && selectedFields.length === 0 && (
                          <p className="text-xs text-muted-foreground">
                            {artifactRetrieval && artifactRetrieval.projectionFields.length > 0
                              ? `Artifact retrieval defaults to the ${artifactRetrieval.projectionFields.join(", ")} ${artifactRetrieval.projectionFields.length === 1 ? "field" : "fields"} so source files and inline URLs are not returned.`
                              : artifactRetrieval
                                ? "Generated asset output fields are not declared, so artifact retrieval defaults to identity-only results."
                                : "Artifact-backed source browsing defaults to identity-only results so large source fields are not returned."}{" "}
                            Select fields here to override that projection.
                          </p>
                        )}
                        <Input
                          id="fields-input"
                          placeholder="Type field name and press Enter"
                          value={fieldInput}
                          onChange={handleFieldInputChange}
                          onKeyDown={handleFieldInputKeyDown}
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

                    {/* Search query and optional semantic index */}
                    <AccordionItem value="semantic" className="border rounded-none bg-card/50 px-3">
                      <AccordionTrigger className="py-2.5 hover:no-underline">
                        <span className="font-medium text-sm">Search Query</span>
                      </AccordionTrigger>
                      <AccordionContent className="pb-3 pt-1">
                        <div className="space-y-2.5">
                          {queryMetadataBlocker && (
                            <Alert
                              variant={
                                indexesMetadataState === "error" || tableMetadataState === "error"
                                  ? "destructive"
                                  : "default"
                              }
                              className="py-2 px-3"
                            >
                              <AlertDescription className="flex items-center justify-between gap-3 text-xs">
                                <span>{queryMetadataBlocker}</span>
                                {(indexesMetadataState === "error" ||
                                  tableMetadataState === "error") && (
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => void refreshQueryMetadata()}
                                  >
                                    Retry
                                  </Button>
                                )}
                              </AlertDescription>
                            </Alert>
                          )}
                          <div>
                            <Label className="text-xs mb-1 block">Vector Index (optional)</Label>
                            {indexes.filter((idx) => idx.config.type === "embeddings").length ===
                            0 ? (
                              <p className="text-xs text-muted-foreground">
                                No vector indexes available. Entered text will use full-text search.
                              </p>
                            ) : (
                              <MultiSelect
                                value={queryIndexes}
                                onValueChange={handleQueryIndexChange}
                              >
                                <MultiSelectTrigger placeholder="Select vector index(es)" />
                                <MultiSelectContent searchPlaceholder="Search indexes…">
                                  {indexes
                                    .filter((idx) => idx.config.type === "embeddings")
                                    .map((index) => (
                                      <MultiSelectItem
                                        key={index.config.name}
                                        value={index.config.name}
                                      >
                                        {index.config.name}
                                      </MultiSelectItem>
                                    ))}
                                </MultiSelectContent>
                              </MultiSelect>
                            )}
                            {indexes.some((idx) => idx.config.type === "embeddings") && (
                              <p className="mt-1 text-xs text-muted-foreground">
                                Select a vector index for semantic search, or clear the selection to
                                use full-text search.
                              </p>
                            )}
                          </div>
                          {indexes.some((idx) => idx.config.type === "full_text") && (
                            <div>
                              <Label className="text-xs mb-1 block">Full-text Index</Label>
                              <Select
                                value={fullTextIndex || "__active_schema__"}
                                onValueChange={handleFullTextIndexChange}
                                disabled={queryIndexes.length > 0}
                              >
                                <SelectTrigger>
                                  <SelectValue placeholder="Use active schema index" />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="__active_schema__">
                                    Active schema index (automatic)
                                  </SelectItem>
                                  {indexes
                                    .filter((idx) => idx.config.type === "full_text")
                                    .map((index) => (
                                      <SelectItem key={index.config.name} value={index.config.name}>
                                        {index.config.name}
                                      </SelectItem>
                                    ))}
                                </SelectContent>
                              </Select>
                              <p className="mt-1 text-xs text-muted-foreground">
                                {queryIndexes.length > 0
                                  ? "Clear vector indexes to run full-text search."
                                  : fullTextIndex
                                    ? "Queries the selected named full-text index."
                                    : "Uses the full-text index for the table's active read schema."}
                              </p>
                            </div>
                          )}
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
                                  Learn more about RRF ranking
                                </a>
                              </AlertDescription>
                            </Alert>
                          )}
                          {artifactSelectionError && (
                            <Alert variant="destructive" className="py-1.5 px-3">
                              <AlertDescription className="text-xs">
                                {artifactSelectionError}
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
                            />
                          </div>
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Structured filters */}
                    <AccordionItem value="filter" className="border rounded-none bg-card/50 px-3">
                      <AccordionTrigger className="py-2.5 hover:no-underline">
                        <span className="font-medium text-sm">Filters</span>
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
                  <Card>
                    <CardHeader>
                      <CardTitle className="text-base">Retrieval Settings & Trace</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-3 text-sm">
                      <div className="grid gap-3 md:grid-cols-3">
                        <div>
                          <p className="text-xs text-muted-foreground">Embedding</p>
                          <p className="font-medium">
                            {selectedVectorIndexes.length > 0
                              ? selectedVectorIndexes
                                  .map((index) => getModelInfo(index)?.model ?? index.config.name)
                                  .join(", ")
                              : "No vector index selected"}
                          </p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Chunking</p>
                          <p className="font-medium">Configured by ingest/index pipeline</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Reranking</p>
                          <p className="font-medium">Not enabled for this query</p>
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <Checkbox
                          id="include-retrieval-profile"
                          checked={includeProfile}
                          onCheckedChange={(checked) => setIncludeProfile(checked === true)}
                        />
                        <Label htmlFor="include-retrieval-profile" className="font-normal">
                          Request execution profile and show it as the retrieval trace
                        </Label>
                      </div>
                    </CardContent>
                  </Card>
                </TabsContent>
                <TabsContent value="json" className="space-y-2">
                  {!isJsonQueryValid && (
                    <Alert variant="destructive">
                      <AlertDescription>
                        The current query must be one valid JSON object.
                      </AlertDescription>
                    </Alert>
                  )}
                  {jsonQuerySafetyBlocker && (
                    <Alert
                      variant={
                        indexesMetadataState === "error" || tableMetadataState === "error"
                          ? "destructive"
                          : "default"
                      }
                    >
                      <AlertDescription>{jsonQuerySafetyBlocker}</AlertDescription>
                    </Alert>
                  )}
                  <Textarea
                    aria-invalid={!isJsonQueryValid || Boolean(jsonQuerySafetyBlocker)}
                    value={queryJsonString}
                    onChange={(event: React.ChangeEvent<HTMLTextAreaElement>) =>
                      setQueryJsonString(event.target.value)
                    }
                    rows={20}
                    className="font-mono"
                    spellCheck={false}
                  />
                </TabsContent>
              </div>
            </Tabs>

            <DashboardToolbar className="flex-row items-center gap-3 md:items-center">
              <Button
                onClick={handleRunQuery}
                disabled={
                  (queryMode === "json" &&
                    (!isJsonQueryValid || Boolean(jsonQuerySafetyBlocker))) ||
                  (queryMode === "builder" && Boolean(queryMetadataBlocker)) ||
                  (queryMode === "builder" && Boolean(artifactSelectionError))
                }
                size="lg"
              >
                Run Query
              </Button>
              <span className="text-xs text-muted-foreground">
                {hasSemanticQuery && hasFilterQuery
                  ? "Running semantic search with filters"
                  : hasSemanticQuery
                    ? "Running semantic search"
                    : hasFullTextQuery && hasFilterQuery
                      ? "Running full-text search with filters"
                      : hasFullTextQuery
                        ? "Running full-text search"
                        : hasFilterQuery
                          ? "Filtering documents"
                          : "Browsing all documents"}
              </span>
            </DashboardToolbar>

            {queryResult?.aggregations && Object.keys(queryResult.aggregations).length > 0 && (
              <AggregationResults aggregations={queryResult.aggregations} className="mt-6" />
            )}

            {queryResult && (
              <Card>
                <CardHeader>
                  <CardTitle>Query Results</CardTitle>
                </CardHeader>
                <CardContent>
                  <QueryResultsList result={queryResult} />
                </CardContent>
              </Card>
            )}

            {queryResult?.profile && (
              <Card>
                <CardHeader>
                  <CardTitle>Retrieval Trace</CardTitle>
                </CardHeader>
                <CardContent>
                  <JsonViewer json={queryResult.profile} />
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Graph Explorer Section */}
        {currentSection === "graph" &&
          tableName &&
          (hasGraphCapability ? (
            <GraphIndexExplorer
              tableName={tableName}
              indexes={indexes}
              onRefreshIndexes={fetchIndexes}
            />
          ) : (
            <Card>
              <CardHeader>
                <CardTitle>Graph is not configured</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm text-muted-foreground">
                <p>
                  This table does not have a graph index yet. Add graph-capable schema or create a
                  graph index before exploring relationships.
                </p>
                <Button variant="outline" onClick={() => navigate(tableSectionPath("indexes"))}>
                  Create graph index
                </Button>
              </CardContent>
            </Card>
          ))}

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

        {/* Manual Entry Section */}
        {currentSection === "document-builder" && (
          <DocumentBuilder tableName={tableName || ""} schema={tableSchema} />
        )}

        {/* Artifacts Section */}
        {currentSection === "artifacts" && (
          <DocumentArtifactsPanel key={tableName} tableName={tableName || ""} />
        )}

        {/* Reprocess Section */}
        {currentSection === "reprocess" && (
          <TableReprocessPanel key={tableName} tableName={tableName || ""} />
        )}

        {/* Schema Section */}
        {currentSection === "schema" && (
          <div className="flex flex-col gap-4">
            <DashboardToolbar className="justify-between">
              <h3>Table Schema</h3>
              <Button
                onClick={() => setIsEditingSchema(!isEditingSchema)}
                variant={isEditingSchema ? "destructive" : "default"}
              >
                {isEditingSchema ? "Cancel" : "Edit Schema"}
              </Button>
            </DashboardToolbar>

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
        artifactSourcesSupported={artifactSourcesSupported}
        artifactSourcesCapabilityError={artifactSourcesCapabilityError}
        onRetryArtifactSourcesCapability={refreshArtifactSourcesCapability}
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
    </DashboardPage>
  );
};

export default TableDetailsPage;
