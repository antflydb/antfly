import type { EmbedderConfig, GeneratorConfig, IndexConfig } from "@antfly/sdk";
import { zodResolver } from "@hookform/resolvers/zod";
import type React from "react";
import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { Form } from "@/components/ui/form";
import { api, type TableSchema } from "../api";
import IndexForm from "./IndexForm";
import JsonViewer from "./JsonViewer";
import { Button } from "./ui/button";
import { Dialog, DialogContent, DialogDescription, DialogTitle, DialogTrigger } from "./ui/dialog";
import { Switch } from "./ui/switch";

interface CreateIndexDialogProps {
  open: boolean;
  onClose: () => void;
  tableName: string;
  onIndexCreated: () => void;
  schema: TableSchema | null;
}

const indexFormSchema = z.object({
  name: z.string().min(1, "Index name is required."),
  dimension: z.number().optional(),
  field: z.string().optional(),
  template: z.string().optional(),
  sourceType: z.enum(["field", "template"]),
  embedder: z.object({
    provider: z.enum([
      "antfly",
      "termite",
      "ollama",
      "gemini",
      "vertex",
      "openai",
      "openrouter",
      "bedrock",
      "cohere",
      "mock",
    ]),
    model: z.string().min(1, "Model is required."),
    api_key: z.string().optional(),
    url: z.string().optional(),
    aws_access_key_id: z.string().optional(),
    aws_secret_access_key: z.string().optional(),
    region: z.string().optional(),
  }),
  chunker: z
    .object({
      provider: z.enum(["termite", "mock", "antfly"]),
      strategy: z.enum(["hugot", "fixed"]),
      api_url: z.string().optional(),
      target_tokens: z.number().optional(),
      overlap_tokens: z.number().optional(),
      separator: z.string().optional(),
      max_chunks: z.number().optional(),
      threshold: z.number().optional(),
    })
    .optional(),
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
        const { sourceType, chunker, ...rest } = data;
        const indexConfig = {
          name: rest.name || "",
          type: "embeddings" as const,
          dimension: rest.dimension || 0,
          field: sourceType === "field" ? rest.field : undefined,
          template: sourceType === "template" ? rest.template : undefined,
          embedder: rest.embedder as GeneratorConfig,
          chunker: chunker || undefined,
        } as IndexConfig;
        setJsonPayload(indexConfig);
      });
      return () => subscription.unsubscribe();
    }
  }, [watch, viewMode]);

  const onSubmit = async (data: IndexFormData) => {
    try {
      let indexConfig: IndexConfig;
      if (viewMode === "json") {
        indexConfig = jsonPayload as IndexConfig;
      } else {
        let embedderConfig: EmbedderConfig;
        const { provider, model, api_key, url, region } = data.embedder;
        switch (provider) {
          case "termite":
            embedderConfig = { provider: "termite", model };
            break;
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

        indexConfig = {
          name: data.name,
          type: "embeddings" as const,
          dimension: data.dimension || 0,
          field: data.sourceType === "field" ? data.field : undefined,
          template: data.sourceType === "template" ? data.template : undefined,
          embedder: embedderConfig,
          chunker: data.chunker || undefined,
        } as IndexConfig;
      }
      await api.indexes.create(tableName, indexConfig);
      onIndexCreated();
      onClose();
    } catch (e) {
      setError("Failed to create index.");
      console.error(e);
    }
  };

  const handleViewChange = (checked: boolean) => {
    const newMode = checked ? "json" : "form";
    if (newMode === "form") {
      if (!("embedder" in jsonPayload) || !("dimension" in jsonPayload)) {
        return;
      }
      const { name, dimension, field, template, embedder } = jsonPayload;
      const chunker = "chunker" in jsonPayload ? jsonPayload.chunker : undefined;
      const sourceType = field ? "field" : "template";
      reset({
        name,
        dimension,
        field,
        template,
        sourceType,
        embedder,
        chunker: chunker || undefined,
      });
    }
    setViewMode(newMode);
  };

  const schemaFields =
    schema?.document_schemas && Object.values(schema.document_schemas)[0]
      ? Object.keys(Object.values(schema.document_schemas)[0].schema.properties)
      : [];

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-[450px]">
        <div className="flex justify-between items-center mb-2">
          <DialogTitle>Create New Index</DialogTitle>
          <div className="flex items-center gap-2">
            <p>Raw JSON</p>
            <Switch checked={viewMode === "json"} onCheckedChange={handleViewChange} />
          </div>
        </div>
        <DialogDescription>Create a new vector index for your table.</DialogDescription>

        {error && <p className="text-red-500">{error}</p>}

        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)}>
            {viewMode === "json" ? (
              <JsonViewer json={jsonPayload} />
            ) : (
              <div className="flex flex-col gap-3">
                <IndexForm schemaFields={schemaFields} />
              </div>
            )}
            <div className="flex gap-3 mt-4 justify-end">
              <DialogTrigger asChild>
                <Button variant="ghost" type="button">
                  Cancel
                </Button>
              </DialogTrigger>
              <Button type="submit">Create</Button>
            </div>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
};

export default CreateIndexDialog;
