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
  Switch,
} from "@antfly/design-system";
import {
  artifactEmbeddingIndexConfig,
  type EmbedderConfig,
  type GeneratorConfig,
  type IndexConfig,
} from "@antfly/sdk";
import { zodResolver } from "@hookform/resolvers/zod";
import type React from "react";
import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { api, type TableSchema } from "../api";
import { createIndexArguments } from "../lib/create-index";
import IndexForm from "./IndexForm";
import JsonViewer from "./JsonViewer";

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

const indexFormSchema = z
  .object({
    name: z.string().trim().min(1, "Index name is required."),
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
    vectorSpace: z.string().optional(),
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
      model: z.string().trim().min(1, "Model is required."),
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
  const form = useForm<IndexFormData>({
    resolver: zodResolver(indexFormSchema),
    defaultValues: {
      name: "",
      sourceType: "field",
      field: "",
      template: "",
      artifactSources: [{ artifact: "", sourceArtifact: "", field: "text" }],
      vectorSpace: undefined,
      dimension: 0,
      embedder: {
        provider: "ollama",
        model: "",
      },
      chunker: undefined,
    },
  });
  const { watch, reset } = form;

  useEffect(() => {
    if (viewMode === "form") {
      const subscription = watch((data) => {
        const { sourceType, chunker, artifactSources = [], vectorSpace, ...rest } = data;
        const indexConfig =
          sourceType === "artifacts"
            ? ({
                name: rest.name || "",
                type: "embeddings" as const,
                dimension: rest.dimension || undefined,
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
                    ...(rest.dimension ? { expected_dims: rest.dimension } : {}),
                    ...(vectorSpace ? { vector_space: vectorSpace } : {}),
                  })),
                embedder: rest.embedder as GeneratorConfig,
              } as IndexConfig)
            : ({
                name: rest.name || "",
                type: "embeddings" as const,
                dimension: rest.dimension || 0,
                field: sourceType === "field" ? rest.field : undefined,
                template: sourceType === "template" ? rest.template : undefined,
                embedder: rest.embedder as GeneratorConfig,
                chunker: chunker || undefined,
              } as IndexConfig);
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
        indexConfig = jsonPayload as IndexConfig;
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
                ...(data.vectorSpace?.trim() ? { vectorSpace: data.vectorSpace.trim() } : {}),
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
    const newMode = checked ? "json" : "form";
    if (newMode === "form") {
      if (!("embedder" in jsonPayload)) {
        return;
      }
      const { name, dimension, field, template, embedder } = jsonPayload;
      const chunker = "chunker" in jsonPayload ? jsonPayload.chunker : undefined;
      const sources =
        "sources" in jsonPayload && Array.isArray(jsonPayload.sources) ? jsonPayload.sources : [];
      const enrichments =
        "enrichments" in jsonPayload && Array.isArray(jsonPayload.enrichments)
          ? jsonPayload.enrichments
          : [];
      const sourceType = sources.length > 0 ? "artifacts" : field ? "field" : "template";
      const vectorSpaces = enrichments.flatMap((candidate) =>
        typeof candidate === "object" &&
        candidate !== null &&
        "vector_space" in candidate &&
        typeof candidate.vector_space === "string" &&
        candidate.vector_space.length > 0
          ? [candidate.vector_space]
          : []
      );
      const uniqueVectorSpaces = [...new Set(vectorSpaces)];
      reset({
        name,
        dimension,
        field,
        template,
        sourceType,
        embedder,
        chunker: chunker || undefined,
        vectorSpace: uniqueVectorSpaces.length === 1 ? uniqueVectorSpaces[0] : undefined,
        artifactSources: sources.map((source) => {
          const enrichment = enrichments.find(
            (candidate) =>
              typeof candidate === "object" &&
              candidate !== null &&
              "name" in candidate &&
              candidate.name === source.artifact
          );
          return {
            artifact: source.artifact,
            sourceArtifact:
              typeof enrichment === "object" &&
              enrichment !== null &&
              "source_artifact_name" in enrichment &&
              typeof enrichment.source_artifact_name === "string"
                ? enrichment.source_artifact_name
                : "",
            field:
              typeof enrichment === "object" &&
              enrichment !== null &&
              "field" in enrichment &&
              typeof enrichment.field === "string"
                ? enrichment.field
                : "text",
          };
        }),
      });
    }
    setViewMode(newMode);
  };

  const schemaFields = getSchemaFieldNames(schema);

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
        <DialogDescription>Create a new vector index for your table.</DialogDescription>

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <Form form={form} onSubmit={form.handleSubmit(onSubmit)}>
          {viewMode === "json" ? (
            <JsonViewer json={jsonPayload} />
          ) : (
            <IndexForm schemaFields={schemaFields} />
          )}
          <FormActions>
            <DialogTrigger asChild>
              <Button variant="ghost" type="button">
                Cancel
              </Button>
            </DialogTrigger>
            <Button type="submit">Create</Button>
          </FormActions>
        </Form>
      </DialogContent>
    </Dialog>
  );
};

export default CreateIndexDialog;
