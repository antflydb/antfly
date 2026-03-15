import type { EmbedderProvider } from "@antfly/sdk";
import { embedderProviders } from "@antfly/sdk";
import type React from "react";
import { useEffect, useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useApiConfig } from "@/hooks/use-api-config";
import ChunkingForm from "./ChunkingForm";
import { Combobox } from "./Combobox";

interface ModelsResponse {
  chunkers: Record<string, unknown>;
  rerankers: Record<string, unknown>;
  ner: Record<string, unknown>;
  embedders: Record<string, unknown>;
  generators: Record<string, unknown>;
}

const staticModelSuggestions: Record<EmbedderProvider, string[]> = {
  antfly: ["all-MiniLM-L6-v2"],
  termite: [], // Populated dynamically from Termite service
  ollama: ["all-minilm", "nomic-embed-text", "embeddinggemma"],
  gemini: ["embeddinggemma", "gemini-embedding-001"],
  vertex: ["text-embedding-004", "text-multilingual-embedding-002"],
  openai: ["text-embedding-3-small", "text-embedding-3-large"],
  openrouter: ["openai/text-embedding-3-small", "openai/text-embedding-3-large"],
  bedrock: [
    "amazon.titan-embed-text-v2:0",
    "amazon.titan-embed-image-v1",
    "cohere.embed-multilingual-v3",
    "cohere.embed-english-v3",
  ],
  cohere: ["embed-english-v3.0", "embed-multilingual-v3.0", "embed-english-light-v3.0"],
  mock: [],
};

interface IndexFormProps {
  fieldPrefix?: string;
  schemaFields?: string[];
}

const IndexForm: React.FC<IndexFormProps> = ({ fieldPrefix = "", schemaFields = [] }) => {
  const { control, watch } = useFormContext();
  const { termiteApiUrl } = useApiConfig();
  const prefix = fieldPrefix ? `${fieldPrefix}.` : "";

  // Termite model detection
  const [termiteEmbedders, setTermiteEmbedders] = useState<string[]>([]);
  const [termiteModelsLoading, setTermiteModelsLoading] = useState(false);

  useEffect(() => {
    const fetchTermiteModels = async () => {
      setTermiteModelsLoading(true);
      try {
        const response = await fetch(`${termiteApiUrl}/api/models`);
        if (response.ok) {
          const data: ModelsResponse = await response.json();
          setTermiteEmbedders(Object.keys(data.embedders || {}));
        }
      } catch {
        // Termite might not be running - this is fine
        console.debug("Termite not available for model detection");
      } finally {
        setTermiteModelsLoading(false);
      }
    };
    fetchTermiteModels();
  }, [termiteApiUrl]);

  const modelSuggestions = useMemo(
    () => ({
      ...staticModelSuggestions,
      termite: termiteEmbedders,
    }),
    [termiteEmbedders]
  );

  const sourceType = watch(`${prefix}sourceType`, "field");
  const provider = watch(`${prefix}embedder.provider`, "ollama");

  return (
    <>
      <FormField
        control={control}
        name={`${prefix}embedder.provider`}
        defaultValue="ollama"
        render={({ field }) => (
          <FormItem>
            <FormLabel>Provider</FormLabel>
            <Select
              value={field.value}
              onValueChange={(value) => field.onChange(value as EmbedderProvider)}
            >
              <FormControl>
                <SelectTrigger>
                  <SelectValue placeholder="Select a provider" />
                </SelectTrigger>
              </FormControl>
              <SelectContent>
                {embedderProviders.map((p: string) => (
                  <SelectItem key={p} value={p}>
                    {p}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <FormMessage />
          </FormItem>
        )}
      />
      <FormField
        control={control}
        name={`${prefix}name`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Index Name</FormLabel>
            <FormControl>
              <Input placeholder="Enter index name" {...field} />
            </FormControl>
            <FormMessage />
          </FormItem>
        )}
      />

      <FormField
        control={control}
        name={`${prefix}sourceType`}
        defaultValue="field"
        render={({ field }) => (
          <FormItem className="space-y-3">
            <FormLabel>Source Type</FormLabel>
            <FormControl>
              <RadioGroup
                onValueChange={field.onChange}
                value={field.value}
                className="flex space-x-2"
              >
                <FormItem className="flex items-center space-x-3 space-y-0">
                  <FormControl>
                    <RadioGroupItem value="field" />
                  </FormControl>
                  <FormLabel className="font-normal">Field</FormLabel>
                </FormItem>
                <FormItem className="flex items-center space-x-3 space-y-0">
                  <FormControl>
                    <RadioGroupItem value="template" />
                  </FormControl>
                  <FormLabel className="font-normal">Template</FormLabel>
                </FormItem>
              </RadioGroup>
            </FormControl>
            <FormMessage />
          </FormItem>
        )}
      />

      {sourceType === "field" ? (
        <FormField
          control={control}
          name={`${prefix}field`}
          render={({ field }) => (
            <FormItem>
              <FormLabel>Field</FormLabel>
              <FormControl>
                <Combobox
                  options={schemaFields.map((f) => ({
                    value: f,
                    label: f,
                  }))}
                  value={field.value}
                  onChange={field.onChange}
                  placeholder="Select or enter field name"
                  searchPlaceholder="Search fields..."
                  emptyText="No fields found."
                  allowCustomValue={true}
                />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
      ) : (
        <FormField
          control={control}
          name={`${prefix}template`}
          render={({ field }) => (
            <FormItem>
              <FormLabel>Template</FormLabel>
              <FormControl>
                <Input type="text" {...field} />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
      )}

      <FormField
        control={control}
        name={`${prefix}embedder.model`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Embedder Model</FormLabel>
            <FormControl>
              <Combobox
                options={(modelSuggestions[provider as EmbedderProvider] || []).map(
                  (suggestion) => ({
                    value: suggestion,
                    label: suggestion,
                  })
                )}
                value={field.value}
                onChange={field.onChange}
                placeholder="Select or enter model name"
                searchPlaceholder="Search models..."
                emptyText="No models found."
                allowCustomValue={true}
              />
            </FormControl>
            {provider === "termite" && termiteModelsLoading && (
              <p className="text-xs text-muted-foreground">Loading models from Termite...</p>
            )}
            {provider === "termite" && !termiteModelsLoading && (
              <p className="text-xs text-muted-foreground">
                {termiteEmbedders.length > 0
                  ? "Select a model or enter a custom name for Termite to pull."
                  : "Enter a model name (e.g., sentence-transformers/all-MiniLM-L6-v2) for Termite to pull."}
              </p>
            )}
            <FormMessage />
          </FormItem>
        )}
      />

      <Accordion type="single" collapsible className="w-full">
        <AccordionItem value="advanced-settings" className="border-b last:border-b-0">
          <AccordionTrigger>Advanced</AccordionTrigger>
          <AccordionContent>
            <div className="flex flex-col gap-3 mt-3">
              <FormField
                control={control}
                name={`${prefix}dimension`}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Dimension</FormLabel>
                    <FormControl>
                      <Input type="number" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <div className="border-t pt-3 mt-3">
                <Accordion type="single" collapsible>
                  <AccordionItem value="chunking" className="border-b last:border-b-0">
                    <AccordionTrigger>Document Chunking (Optional)</AccordionTrigger>
                    <AccordionContent>
                      <div className="flex flex-col gap-3 mt-3">
                        <p className="text-sm text-muted-foreground">
                          Configure chunking to split documents into smaller segments before
                          embedding.
                        </p>
                        <ChunkingForm fieldPrefix={`${prefix}chunker`} />
                      </div>
                    </AccordionContent>
                  </AccordionItem>
                </Accordion>
              </div>

              {provider === "gemini" && (
                <FormField
                  control={control}
                  name={`${prefix}embedder.api_key`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>API Key</FormLabel>
                      <FormControl>
                        <Input type="password" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              )}
              {provider === "openai" && (
                <>
                  <FormField
                    control={control}
                    name={`${prefix}embedder.api_key`}
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>API Key</FormLabel>
                        <FormControl>
                          <Input type="password" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={control}
                    name={`${prefix}embedder.url`}
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Base URL</FormLabel>
                        <FormControl>
                          <Input type="text" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </>
              )}
              {provider === "bedrock" && (
                <>
                  <FormField
                    control={control}
                    name={`${prefix}embedder.aws_access_key_id`}
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>AWS Access Key ID</FormLabel>
                        <FormControl>
                          <Input type="text" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={control}
                    name={`${prefix}embedder.aws_secret_access_key`}
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>AWS Secret Access Key</FormLabel>
                        <FormControl>
                          <Input type="password" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={control}
                    name={`${prefix}embedder.region`}
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>AWS Region</FormLabel>
                        <FormControl>
                          <Input type="text" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </>
              )}
              {provider === "ollama" && (
                <FormField
                  control={control}
                  name={`${prefix}embedder.url`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Base URL</FormLabel>
                      <FormControl>
                        <Input type="text" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              )}
              {provider === "cohere" && (
                <FormField
                  control={control}
                  name={`${prefix}embedder.api_key`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>API Key</FormLabel>
                      <FormControl>
                        <Input type="password" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              )}
              {provider === "openrouter" && (
                <FormField
                  control={control}
                  name={`${prefix}embedder.api_key`}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>API Key</FormLabel>
                      <FormControl>
                        <Input type="password" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              )}
            </div>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </>
  );
};

export default IndexForm;
