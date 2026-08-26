import {
  Alert,
  AlertDescription,
  Button,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
  Form,
  FormActions,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
  Input,
  RadioGroup,
  RadioGroupItem,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Switch,
  Textarea,
} from "@antfly/design-system";
import {
  artifactEmbeddingIndexConfig,
  artifactFullTextIndexConfig,
  type EmbedderConfig,
  type GeneratorConfig,
  graphIndexSources,
  type IndexConfig,
} from "@antfly/sdk";
import { zodResolver } from "@hookform/resolvers/zod";
import type React from "react";
import { useEffect, useState } from "react";
import { useFieldArray, useForm, useFormContext } from "react-hook-form";
import { z } from "zod";
import { api, type TableSchema } from "../api";
import { createIndexArguments } from "../lib/create-index";
import { Combobox } from "./Combobox";
import IndexForm from "./IndexForm";

interface CreateIndexDialogProps {
  open: boolean;
  onClose: () => void;
  tableName: string;
  onIndexCreated: () => void;
  schema: TableSchema | null;
}

export function getSchemaFieldNames(schema: TableSchema | null): string[] {
  if (!schema?.document_schemas || typeof schema.document_schemas !== "object") {
    return [];
  }

  const fields = Object.values(schema.document_schemas).flatMap((documentSchema) => {
    const properties = documentSchema?.schema?.properties;
    if (!properties || typeof properties !== "object") {
      return [];
    }
    return Object.keys(properties);
  });

  return [...new Set(fields)].sort((a, b) => a.localeCompare(b));
}

export function parseAdvancedIndexConfig(source: string): IndexConfig {
  let value: unknown;
  try {
    value = JSON.parse(source);
  } catch (error) {
    throw new Error(error instanceof Error ? `Invalid JSON: ${error.message}` : "Invalid JSON.");
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Index configuration must be a JSON object.");
  }
  const config = value as Record<string, unknown>;
  if (typeof config.name !== "string" || !config.name.trim()) {
    throw new Error("Index configuration requires a non-empty name.");
  }
  if (config.type !== "embeddings" && config.type !== "full_text" && config.type !== "graph") {
    throw new Error('Index configuration type must be "embeddings", "full_text", or "graph".');
  }
  if (config.sources !== undefined) {
    if (!Array.isArray(config.sources)) {
      throw new Error("Index sources must be an array.");
    }
    if (config.sources.length === 0 || config.sources.length > 64) {
      throw new Error("Index sources must contain between 1 and 64 items.");
    }
    if (config.type === "graph") {
      graphIndexSources(...(config.sources as Parameters<typeof graphIndexSources>));
    } else {
      const seen = new Set<string>();
      config.sources.forEach((source, index) => {
        if (!source || typeof source !== "object" || Array.isArray(source)) {
          throw new Error(`Index sources[${index}] must be an object.`);
        }
        const keys = Object.keys(source);
        if (keys.some((key) => key !== "artifact")) {
          throw new Error(`Index sources[${index}] only supports artifact.`);
        }
        const artifact = (source as Record<string, unknown>).artifact;
        if (typeof artifact !== "string" || !artifact.trim()) {
          throw new Error(`Index sources[${index}].artifact must be a non-empty string.`);
        }
        if (seen.has(artifact)) {
          throw new Error(`Index sources contains duplicate artifact ${JSON.stringify(artifact)}.`);
        }
        seen.add(artifact);
      });
    }
  }
  const mutuallyExclusiveWithSources =
    config.type === "full_text"
      ? ["artifact_name"]
      : config.type === "graph"
        ? ["source"]
        : ["external", "field", "template", "chunker", "embedding_name", "source_artifact_name"];
  if (config.sources !== undefined) {
    const conflict = mutuallyExclusiveWithSources.find((field) => config[field] !== undefined);
    if (conflict) {
      throw new Error(`Index sources cannot be combined with ${conflict}.`);
    }
  }
  return value as IndexConfig;
}

const indexFormSchema = z
  .object({
    name: z.string().trim().min(1, "Index name is required."),
    indexType: z.enum(["embeddings", "full_text", "graph"]),
    dimension: z.number().optional(),
    field: z.string().optional(),
    template: z.string().optional(),
    sourceType: z.enum(["field", "template", "artifacts"]),
    artifactSources: z
      .array(
        z.object({
          artifact: z.string(),
          sourceArtifact: z.string().optional(),
          field: z.string().optional(),
        })
      )
      .max(64, "At most 64 artifact sources are allowed."),
    fullTextSourceType: z.enum(["document", "field", "artifacts"]),
    fullTextField: z.string().optional(),
    fullTextArtifacts: z.array(z.object({ artifact: z.string() })).max(64),
    graphSources: z
      .array(
        z.object({
          artifact: z.string(),
          path: z.string().optional(),
          format: z.enum(["extraction_relation", "extraction_graph"]),
          mentionEdgeType: z.string().optional(),
          nodeModel: z.enum(["document", "external"]),
          sourceNode: z.string().optional(),
          targetNode: z.string().optional(),
          edgeType: z.string().optional(),
          edgeWeight: z.string().optional(),
          edgeMetadata: z.string().optional(),
          contextFields: z.string().optional(),
        })
      )
      .max(64),
    embedder: z.object({
      provider: z.enum([
        "antfly",
        "ollama",
        "gemini",
        "vertex",
        "openai",
        "openrouter",
        "bedrock",
        "cohere",
        "mock",
      ]),
      model: z.string().trim(),
      api_key: z.string().optional(),
      url: z.string().optional(),
      aws_access_key_id: z.string().optional(),
      aws_secret_access_key: z.string().optional(),
      region: z.string().optional(),
    }),
    chunker: z
      .object({
        provider: z.enum(["antfly", "mock"]),
        strategy: z.enum(["hugot", "fixed"]),
        api_url: z.string().optional(),
        target_tokens: z.number().optional(),
        overlap_tokens: z.number().optional(),
        separator: z.string().optional(),
        max_chunks: z.number().optional(),
        threshold: z.number().optional(),
      })
      .optional(),
  })
  .superRefine((data, context) => {
    if (data.indexType === "full_text") {
      if (data.fullTextSourceType === "field" && !data.fullTextField?.trim()) {
        context.addIssue({
          code: "custom",
          path: ["fullTextField"],
          message: "Field is required.",
        });
      }
      if (data.fullTextSourceType === "artifacts") {
        validateNamedSources(data.fullTextArtifacts, "fullTextArtifacts", context);
      }
      return;
    }
    if (data.indexType === "graph") {
      validateNamedSources(data.graphSources, "graphSources", context);
      data.graphSources.forEach((source, index) => {
        const fields = splitContextFields(source.contextFields);
        if (new Set(fields).size !== fields.length) {
          context.addIssue({
            code: "custom",
            path: ["graphSources", index, "contextFields"],
            message: "Context fields must be unique.",
          });
        }
        if (source.edgeMetadata?.trim()) {
          try {
            const value: unknown = JSON.parse(source.edgeMetadata);
            if (!value || typeof value !== "object" || Array.isArray(value)) {
              throw new Error("not an object");
            }
          } catch {
            context.addIssue({
              code: "custom",
              path: ["graphSources", index, "edgeMetadata"],
              message: "Edge metadata must be a JSON object.",
            });
          }
        }
      });
      return;
    }
    if (!data.embedder.model.trim()) {
      context.addIssue({
        code: "custom",
        path: ["embedder", "model"],
        message: "Model is required.",
      });
    }
    if (data.sourceType === "field" && !data.field?.trim()) {
      context.addIssue({ code: "custom", path: ["field"], message: "Field is required." });
    }
    if (data.sourceType === "template" && !data.template?.trim()) {
      context.addIssue({ code: "custom", path: ["template"], message: "Template is required." });
    }
    if (data.sourceType !== "artifacts") return;
    if (data.artifactSources.length === 0) {
      context.addIssue({
        code: "custom",
        path: ["artifactSources"],
        message: "At least one artifact source is required.",
      });
    }
    const seen = new Set<string>();
    data.artifactSources.forEach((source, index) => {
      const artifact = source.artifact.trim();
      if (!artifact) {
        context.addIssue({
          code: "custom",
          path: ["artifactSources", index, "artifact"],
          message: "Artifact name is required.",
        });
      } else if (seen.has(artifact)) {
        context.addIssue({
          code: "custom",
          path: ["artifactSources", index, "artifact"],
          message: "Artifact names must be unique.",
        });
      }
      seen.add(artifact);
      if (!source.field?.trim()) {
        context.addIssue({
          code: "custom",
          path: ["artifactSources", index, "field"],
          message: "Embedding input field is required.",
        });
      }
    });
  });

type IndexFormData = z.infer<typeof indexFormSchema>;
type GraphSourceFormData = IndexFormData["graphSources"][number];

function splitContextFields(value: string | undefined): string[] {
  return (value ?? "")
    .split(",")
    .map((field) => field.trim())
    .filter(Boolean);
}

function validateNamedSources(
  sources: Array<{ artifact: string }>,
  path: "fullTextArtifacts" | "graphSources",
  context: z.RefinementCtx
): void {
  if (sources.length === 0) {
    context.addIssue({
      code: "custom",
      path: [path],
      message: "At least one artifact source is required.",
    });
    return;
  }
  const seen = new Set<string>();
  sources.forEach((source, index) => {
    const artifact = source.artifact.trim();
    if (!artifact) {
      context.addIssue({
        code: "custom",
        path: [path, index, "artifact"],
        message: "Artifact name is required.",
      });
    } else if (seen.has(artifact)) {
      context.addIssue({
        code: "custom",
        path: [path, index, "artifact"],
        message: "Artifact names must be unique.",
      });
    }
    seen.add(artifact);
  });
}

export function buildGraphSourceConfig(source: GraphSourceFormData) {
  const sourceNode = source.sourceNode?.trim();
  const targetNode = source.targetNode?.trim();
  const edgeType = source.edgeType?.trim();
  const edgeWeight = source.edgeWeight?.trim();
  const numericWeight = edgeWeight ? Number(edgeWeight) : Number.NaN;
  let edgeMetadata: Record<string, unknown> | undefined;
  if (source.edgeMetadata?.trim()) {
    try {
      const parsed: unknown = JSON.parse(source.edgeMetadata);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        edgeMetadata = parsed as Record<string, unknown>;
      }
    } catch {
      // Live form previews must remain renderable while the user is typing;
      // schema validation blocks submission until the JSON object is valid.
    }
  }
  const contextFields = splitContextFields(source.contextFields);
  return {
    artifact: source.artifact.trim(),
    ...(source.path?.trim() ? { path: source.path.trim() } : {}),
    format: source.format,
    ...(source.mentionEdgeType?.trim() ? { mention_edge_type: source.mentionEdgeType.trim() } : {}),
    ...(source.nodeModel === "external" || sourceNode || targetNode
      ? {
          nodes: {
            model: source.nodeModel,
            ...(sourceNode ? { source: sourceNode } : {}),
            ...(targetNode ? { target: targetNode } : {}),
          },
        }
      : {}),
    ...(edgeType || edgeWeight || edgeMetadata
      ? {
          edge: {
            ...(edgeType ? { type: edgeType } : {}),
            ...(edgeWeight
              ? { weight: Number.isFinite(numericWeight) ? numericWeight : edgeWeight }
              : {}),
            ...(edgeMetadata ? { metadata: edgeMetadata } : {}),
          },
        }
      : {}),
    ...(contextFields.length > 0 ? { context: { doc_fields: contextFields } } : {}),
  };
}

const IndexKindForm: React.FC<{ schemaFields: string[] }> = ({ schemaFields }) => {
  const { control, watch } = useFormContext<IndexFormData>();
  const indexType = watch("indexType");
  const fullTextSourceType = watch("fullTextSourceType");
  const fullTextArtifacts = useFieldArray({ control, name: "fullTextArtifacts" });
  const graphSources = useFieldArray({ control, name: "graphSources" });

  return (
    <div className="space-y-4">
      <FormField
        control={control}
        name="indexType"
        render={({ field }) => (
          <FormItem>
            <FormLabel>Index type</FormLabel>
            <FormControl>
              <RadioGroup onValueChange={field.onChange} value={field.value} className="flex gap-4">
                {(["embeddings", "full_text", "graph"] as const).map((value) => (
                  <FormItem key={value} className="flex items-center gap-2 space-y-0">
                    <FormControl>
                      <RadioGroupItem value={value} />
                    </FormControl>
                    <FormLabel className="font-normal">
                      {value === "embeddings"
                        ? "Vector"
                        : value === "full_text"
                          ? "Full-text"
                          : "Graph"}
                    </FormLabel>
                  </FormItem>
                ))}
              </RadioGroup>
            </FormControl>
          </FormItem>
        )}
      />
      <FormField
        control={control}
        name="name"
        render={({ field }) => (
          <FormItem>
            <FormLabel>Index name</FormLabel>
            <FormControl>
              <Input placeholder="document_search" {...field} />
            </FormControl>
            <FormMessage />
          </FormItem>
        )}
      />

      {indexType === "embeddings" ? (
        <IndexForm schemaFields={schemaFields} showName={false} allowArtifactSources />
      ) : indexType === "full_text" ? (
        <div className="space-y-3 rounded-md border p-3">
          <FormField
            control={control}
            name="fullTextSourceType"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Text source</FormLabel>
                <FormControl>
                  <RadioGroup
                    onValueChange={field.onChange}
                    value={field.value}
                    className="flex gap-4"
                  >
                    <FormItem className="flex items-center gap-2 space-y-0">
                      <FormControl>
                        <RadioGroupItem value="field" />
                      </FormControl>
                      <FormLabel className="font-normal">Document field</FormLabel>
                    </FormItem>
                    <FormItem className="flex items-center gap-2 space-y-0">
                      <FormControl>
                        <RadioGroupItem value="document" />
                      </FormControl>
                      <FormLabel className="font-normal">Whole document</FormLabel>
                    </FormItem>
                    <FormItem className="flex items-center gap-2 space-y-0">
                      <FormControl>
                        <RadioGroupItem value="artifacts" />
                      </FormControl>
                      <FormLabel className="font-normal">Artifact streams</FormLabel>
                    </FormItem>
                  </RadioGroup>
                </FormControl>
              </FormItem>
            )}
          />
          {fullTextSourceType === "field" ? (
            <FormField
              control={control}
              name="fullTextField"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Field</FormLabel>
                  <FormControl>
                    <Combobox
                      options={schemaFields.map((value) => ({ value, label: value }))}
                      value={field.value}
                      onChange={field.onChange}
                      placeholder="Select or enter field"
                      allowCustomValue
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
          ) : fullTextSourceType === "artifacts" ? (
            <div className="space-y-2">
              <p className="text-xs text-muted-foreground">
                Each chunk or textual asset is indexed as an independent member.
              </p>
              <FormField
                control={control}
                name="fullTextField"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Artifact content field (optional)</FormLabel>
                    <FormControl>
                      <Input placeholder="text" {...field} />
                    </FormControl>
                    <p className="text-xs text-muted-foreground">
                      Use this when every artifact record exposes the searchable content under the
                      same field.
                    </p>
                    <FormMessage />
                  </FormItem>
                )}
              />
              {fullTextArtifacts.fields.map((source, index) => (
                <div key={source.id} className="flex items-start gap-2">
                  <FormField
                    control={control}
                    name={`fullTextArtifacts.${index}.artifact`}
                    render={({ field }) => (
                      <FormItem className="flex-1">
                        <FormLabel>Artifact</FormLabel>
                        <FormControl>
                          <Input placeholder="document_chunks_v1" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  {fullTextArtifacts.fields.length > 1 && (
                    <Button
                      type="button"
                      variant="ghost"
                      className="mt-6"
                      onClick={() => fullTextArtifacts.remove(index)}
                    >
                      Remove
                    </Button>
                  )}
                </div>
              ))}
              <Button
                type="button"
                variant="outline"
                disabled={fullTextArtifacts.fields.length >= 64}
                onClick={() => fullTextArtifacts.append({ artifact: "" })}
              >
                Add artifact source
              </Button>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground">
              Index the complete stored document using Antfly&apos;s default document text
              projection.
            </p>
          )}
        </div>
      ) : (
        <div className="space-y-3 rounded-md border p-3">
          <p className="text-xs text-muted-foreground">
            Sources are evaluated in order; earlier sources win when multiple artifacts produce the
            same edge identity.
          </p>
          {graphSources.fields.map((source, index) => (
            <div key={source.id} className="space-y-2 rounded-md border p-3">
              <div className="grid gap-2 sm:grid-cols-2">
                <FormField
                  control={control}
                  name={`graphSources.${index}.artifact`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Artifact</FormLabel>
                      <FormControl>
                        <Input placeholder="relations_v1" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.path`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>JSON path</FormLabel>
                      <FormControl>
                        <Input placeholder="$.relations[*]" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.format`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Format</FormLabel>
                      <Select value={field.value} onValueChange={field.onChange}>
                        <FormControl>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="extraction_relation">Extraction relation</SelectItem>
                          <SelectItem value="extraction_graph">Extraction graph</SelectItem>
                        </SelectContent>
                      </Select>
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.nodeModel`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Node model</FormLabel>
                      <Select value={field.value} onValueChange={field.onChange}>
                        <FormControl>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="document">Document</SelectItem>
                          <SelectItem value="external">External</SelectItem>
                        </SelectContent>
                      </Select>
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.mentionEdgeType`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Mention edge type</FormLabel>
                      <FormControl>
                        <Input placeholder="mentions" {...field} />
                      </FormControl>
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.sourceNode`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Source node template</FormLabel>
                      <FormControl>
                        <Input placeholder="{{ _doc.key }}" {...field} />
                      </FormControl>
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.targetNode`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Target node template</FormLabel>
                      <FormControl>
                        <Input placeholder="{{ _item.target.text }}" {...field} />
                      </FormControl>
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.edgeType`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Edge type template</FormLabel>
                      <FormControl>
                        <Input placeholder="{{ _item.predicate }}" {...field} />
                      </FormControl>
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.edgeWeight`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Edge weight</FormLabel>
                      <FormControl>
                        <Input placeholder="1.0 or {{ _item.confidence }}" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.edgeMetadata`}
                  render={({ field }) => (
                    <FormItem className="sm:col-span-2">
                      <FormLabel>Edge metadata JSON</FormLabel>
                      <FormControl>
                        <Textarea
                          className="min-h-20 font-mono text-xs"
                          placeholder={'{"evidence":"{{ _item.evidence }}"}'}
                          {...field}
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={control}
                  name={`graphSources.${index}.contextFields`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Context fields</FormLabel>
                      <FormControl>
                        <Input placeholder="title, body" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </div>
              {graphSources.fields.length > 1 && (
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    disabled={index === 0}
                    aria-label={`Move graph source ${index + 1} earlier`}
                    onClick={() => graphSources.move(index, index - 1)}
                  >
                    Move earlier
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    disabled={index === graphSources.fields.length - 1}
                    aria-label={`Move graph source ${index + 1} later`}
                    onClick={() => graphSources.move(index, index + 1)}
                  >
                    Move later
                  </Button>
                  <Button type="button" variant="ghost" onClick={() => graphSources.remove(index)}>
                    Remove source
                  </Button>
                </div>
              )}
            </div>
          ))}
          <Button
            type="button"
            variant="outline"
            disabled={graphSources.fields.length >= 64}
            onClick={() =>
              graphSources.append({
                artifact: "",
                path: "",
                format: "extraction_relation",
                mentionEdgeType: "",
                nodeModel: "document",
                sourceNode: "",
                targetNode: "",
                edgeType: "",
                edgeWeight: "",
                edgeMetadata: "",
                contextFields: "",
              })
            }
          >
            Add graph source
          </Button>
        </div>
      )}
    </div>
  );
};

const CreateIndexDialog: React.FC<CreateIndexDialogProps> = ({
  open,
  onClose,
  tableName,
  onIndexCreated,
  schema,
}) => {
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<"form" | "json">("form");
  const [jsonPayload, setJsonPayload] = useState<IndexConfig>({
    name: "",
    type: "embeddings",
    dimension: 0,
    embedder: { provider: "ollama", model: "" },
  });
  const [jsonSource, setJsonSource] = useState("");
  const [jsonFormBaseline, setJsonFormBaseline] = useState("");
  const [jsonValidationError, setJsonValidationError] = useState<string | null>(null);
  const form = useForm<IndexFormData>({
    resolver: zodResolver(indexFormSchema),
    defaultValues: {
      name: "",
      indexType: "embeddings",
      sourceType: "field",
      field: "",
      template: "",
      artifactSources: [{ artifact: "", sourceArtifact: "", field: "text" }],
      fullTextSourceType: "field",
      fullTextField: "",
      fullTextArtifacts: [{ artifact: "" }],
      graphSources: [
        {
          artifact: "",
          path: "$.relations[*]",
          format: "extraction_relation",
          mentionEdgeType: "",
          nodeModel: "document",
          sourceNode: "",
          targetNode: "",
          edgeType: "",
          edgeWeight: "",
          edgeMetadata: "",
          contextFields: "",
        },
      ],
      dimension: 0,
      embedder: {
        provider: "ollama",
        model: "",
      },
      chunker: undefined,
    },
  });
  const { watch } = form;

  useEffect(() => {
    if (viewMode === "form") {
      const subscription = watch((data) => {
        let indexConfig: IndexConfig;
        if (data.indexType === "full_text") {
          indexConfig = {
            name: data.name || "",
            type: "full_text",
            ...(data.fullTextSourceType === "artifacts"
              ? {
                  sources: (data.fullTextArtifacts ?? [])
                    .filter((source) => source?.artifact)
                    .map((source) => ({ artifact: source?.artifact || "" })),
                  ...(data.fullTextField?.trim() ? { field: data.fullTextField.trim() } : {}),
                }
              : data.fullTextSourceType === "field"
                ? { field: data.fullTextField || "" }
                : {}),
          } as IndexConfig;
        } else if (data.indexType === "graph") {
          indexConfig = {
            name: data.name || "",
            type: "graph",
            sources: (data.graphSources ?? [])
              .filter((source) => source?.artifact)
              .map((source) =>
                buildGraphSourceConfig({
                  artifact: source?.artifact || "",
                  path: source?.path,
                  format: source?.format ?? "extraction_relation",
                  mentionEdgeType: source?.mentionEdgeType,
                  nodeModel: source?.nodeModel ?? "document",
                  sourceNode: source?.sourceNode,
                  targetNode: source?.targetNode,
                  edgeType: source?.edgeType,
                  edgeWeight: source?.edgeWeight,
                  edgeMetadata: source?.edgeMetadata,
                  contextFields: source?.contextFields,
                })
              ),
          } as IndexConfig;
        } else {
          const sourceType = data.sourceType ?? "field";
          const artifactSources = data.artifactSources ?? [];
          indexConfig =
            sourceType === "artifacts"
              ? ({
                  name: data.name || "",
                  type: "embeddings",
                  dimension: data.dimension || undefined,
                  sources: artifactSources
                    .filter((source) => source?.artifact)
                    .map((source) => ({ artifact: source?.artifact || "" })),
                  enrichments: artifactSources
                    .filter((source) => source?.artifact)
                    .map((source) => ({
                      name: source?.artifact || "",
                      kind: "embedding" as const,
                      field: source?.field || "text",
                      ...(source?.sourceArtifact
                        ? { source_artifact_name: source.sourceArtifact }
                        : {}),
                      ...(data.dimension ? { expected_dims: data.dimension } : {}),
                    })),
                  embedder: data.embedder as GeneratorConfig,
                } as IndexConfig)
              : ({
                  name: data.name || "",
                  type: "embeddings",
                  dimension: data.dimension || 0,
                  field: sourceType === "field" ? data.field : undefined,
                  template: sourceType === "template" ? data.template : undefined,
                  embedder: data.embedder as GeneratorConfig,
                  chunker: data.chunker || undefined,
                } as IndexConfig);
        }
        setJsonPayload(indexConfig);
      });
      return () => subscription.unsubscribe();
    }
  }, [watch, viewMode]);

  const onSubmit = async (data: IndexFormData) => {
    setError(null);
    try {
      let indexConfig: IndexConfig;
      if (viewMode === "json") {
        indexConfig = parseAdvancedIndexConfig(jsonSource);
      } else if (data.indexType === "full_text") {
        indexConfig =
          data.fullTextSourceType === "artifacts"
            ? ({
                ...artifactFullTextIndexConfig(
                  data.name,
                  ...data.fullTextArtifacts.map((source) => source.artifact.trim())
                ),
                ...(data.fullTextField?.trim() ? { field: data.fullTextField.trim() } : {}),
              } as IndexConfig)
            : ({
                name: data.name,
                type: "full_text",
                ...(data.fullTextSourceType === "field"
                  ? { field: data.fullTextField?.trim() }
                  : {}),
              } as IndexConfig);
      } else if (data.indexType === "graph") {
        indexConfig = {
          name: data.name,
          type: "graph",
          sources: graphIndexSources(...data.graphSources.map(buildGraphSourceConfig)),
        } as IndexConfig;
      } else {
        let embedderConfig: EmbedderConfig;
        const { provider, model, api_key, url, region } = data.embedder;
        switch (provider) {
          case "ollama":
            embedderConfig = { provider: "ollama", model, url };
            break;
          case "gemini":
            embedderConfig = { provider: "gemini", model, api_key };
            break;
          case "vertex":
            embedderConfig = { provider: "vertex", model };
            break;
          case "openai":
            embedderConfig = { provider: "openai", model, api_key, url };
            break;
          case "openrouter":
            embedderConfig = { provider: "openrouter", model, api_key };
            break;
          case "bedrock":
            embedderConfig = {
              provider: "bedrock",
              model,
              region,
            };
            break;
          case "cohere":
            embedderConfig = { provider: "cohere", model, api_key };
            break;
          case "mock":
            embedderConfig = { provider: "mock", model };
            break;
          case "antfly":
            embedderConfig = { provider: "antfly", model };
            break;
          default:
            throw new Error("Invalid provider");
        }

        indexConfig =
          data.sourceType === "artifacts"
            ? artifactEmbeddingIndexConfig(data.name, {
                sources: data.artifactSources.map((source) => ({
                  artifact: source.artifact.trim(),
                  ...(source.sourceArtifact?.trim()
                    ? { sourceArtifact: source.sourceArtifact.trim() }
                    : {}),
                  field: source.field?.trim() || "text",
                })),
                embedder: embedderConfig,
                ...(data.dimension ? { dimension: data.dimension } : {}),
              })
            : ({
                name: data.name,
                type: "embeddings" as const,
                dimension: data.dimension || 0,
                field: data.sourceType === "field" ? data.field : undefined,
                template: data.sourceType === "template" ? data.template : undefined,
                embedder: embedderConfig,
                chunker: data.chunker || undefined,
              } as IndexConfig);
      }
      const { indexName, request } = createIndexArguments(indexConfig);
      await api.indexes.create(tableName, indexName, request);
      onIndexCreated();
      onClose();
    } catch (e) {
      setError(e instanceof Error && e.message ? e.message : "Failed to create index.");
      console.error(e);
    }
  };

  const handleViewChange = (checked: boolean) => {
    if (checked) {
      const source = JSON.stringify(jsonPayload, null, 2);
      setJsonSource(source);
      setJsonFormBaseline(source);
      setJsonValidationError(null);
      setViewMode("json");
      return;
    }
    if (jsonSource !== jsonFormBaseline) {
      const discard = window.confirm(
        "Switching back to the form will discard Raw JSON edits that the form cannot represent. Continue?"
      );
      if (!discard) return;
      try {
        setJsonPayload(parseAdvancedIndexConfig(jsonFormBaseline));
      } catch {
        // The baseline is produced by the typed form and should always parse.
        // Preserve the current payload if an invariant is violated rather than
        // turning a view transition into an unhandled UI exception.
      }
      setJsonSource(jsonFormBaseline);
      setJsonValidationError(null);
    }
    setViewMode("form");
  };

  const handleJsonChange = (source: string) => {
    setJsonSource(source);
    try {
      const parsed = parseAdvancedIndexConfig(source);
      setJsonPayload(parsed);
      setJsonValidationError(null);
    } catch (validationError) {
      setJsonValidationError(
        validationError instanceof Error ? validationError.message : "Invalid index configuration."
      );
    }
  };

  const schemaFields = getSchemaFieldNames(schema);
  const selectedIndexType = watch("indexType");
  const indexTypeLabel =
    selectedIndexType === "full_text"
      ? "full-text"
      : selectedIndexType === "graph"
        ? "graph"
        : "vector";

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-h-[90vh] max-w-[640px] overflow-y-auto">
        <div className="flex justify-between items-center mb-2">
          <DialogTitle>Create New Index</DialogTitle>
          <div className="flex items-center gap-2">
            <p>Raw JSON</p>
            <Switch checked={viewMode === "json"} onCheckedChange={handleViewChange} />
          </div>
        </div>
        <DialogDescription>Create a new {indexTypeLabel} index for your table.</DialogDescription>

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <Form
          form={form}
          onSubmit={
            viewMode === "json"
              ? (event) => {
                  event.preventDefault();
                  void onSubmit(form.getValues());
                }
              : form.handleSubmit(onSubmit)
          }
        >
          {viewMode === "json" ? (
            <div className="space-y-2">
              <Textarea
                aria-label="Advanced index JSON"
                className="min-h-96 resize-y font-mono text-xs"
                spellCheck={false}
                value={jsonSource}
                onChange={(event) => handleJsonChange(event.target.value)}
              />
              {jsonValidationError ? (
                <p className="text-sm text-destructive" role="alert">
                  {jsonValidationError}
                </p>
              ) : (
                <p className="text-xs text-muted-foreground">
                  JSON syntax and source structure are valid. The server validates all advanced
                  options when you create the index.
                </p>
              )}
            </div>
          ) : (
            <IndexKindForm schemaFields={schemaFields} />
          )}
          <FormActions>
            <DialogTrigger asChild>
              <Button variant="ghost" type="button">
                Cancel
              </Button>
            </DialogTrigger>
            <Button type="submit" disabled={viewMode === "json" && jsonValidationError !== null}>
              Create
            </Button>
          </FormActions>
        </Form>
      </DialogContent>
    </Dialog>
  );
};

export default CreateIndexDialog;
