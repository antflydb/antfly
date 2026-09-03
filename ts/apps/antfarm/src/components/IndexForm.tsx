import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
  Button,
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
} from "@antfly/design-system";
import { embedderProviders } from "@antfly/sdk";
import type React from "react";
import { useMemo } from "react";
import { useFieldArray, useFormContext } from "react-hook-form";
import { liveModelSuggestions, useConnectedModels } from "@/hooks/use-connections";
import ChunkingForm from "./ChunkingForm";
import { Combobox } from "./Combobox";

type IndexEmbedderProvider = (typeof embedderProviders)[number];

const staticModelSuggestions: Record<IndexEmbedderProvider, string[]> = {
  antfly: ["all-MiniLM-L6-v2"],
  ollama: ["all-minilm", "nomic-embed-text", "embeddinggemma"],
  openai: ["text-embedding-3-small", "text-embedding-3-large"],
  bedrock: [
    "amazon.titan-embed-text-v2:0",
    "amazon.titan-embed-image-v1",
    "cohere.embed-multilingual-v3",
    "cohere.embed-english-v3",
  ],
};

interface IndexFormProps {
  fieldPrefix?: string;
  schemaFields?: string[];
  showName?: boolean;
  allowArtifactSources?: boolean;
}

const IndexForm: React.FC<IndexFormProps> = ({
  fieldPrefix = "",
  schemaFields = [],
  showName = true,
  allowArtifactSources = false,
}) => {
  const { control, watch } = useFormContext();
  const prefix = fieldPrefix ? `${fieldPrefix}.` : "";
  const artifactSources = useFieldArray({
    control,
    name: `${prefix}artifactSources`,
  });

  // Live model lists per configured provider from /db/v1/connections.
  const { providers: connectedProviders, loading: inferenceModelsLoading } = useConnectedModels();
  const liveEmbedders = useMemo(
    () => liveModelSuggestions(connectedProviders, "embedder"),
    [connectedProviders]
  );

  const modelSuggestions = useMemo(() => {
    const merged: Record<IndexEmbedderProvider, string[]> = {
      ...staticModelSuggestions,
    };
    // Live provider listings win over static suggestions; unconfigured or
    // failing providers keep their static lists.
    for (const [providerType, models] of Object.entries(liveEmbedders)) {
      if (models.length > 0 && providerType in merged) {
        merged[providerType as IndexEmbedderProvider] = models;
      }
    }
    return merged;
  }, [liveEmbedders]);

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
              onValueChange={(value) => field.onChange(value as IndexEmbedderProvider)}
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
      {showName && (
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
      )}
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
                {allowArtifactSources && (
                  <FormItem className="flex items-center space-x-3 space-y-0">
                    <FormControl>
                      <RadioGroupItem value="artifacts" />
                    </FormControl>
                    <FormLabel className="font-normal">Artifacts</FormLabel>
                  </FormItem>
                )}
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
      ) : sourceType === "template" ? (
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
      ) : (
        <div className="space-y-3 rounded-md border p-3">
          <div>
            <FormLabel>Embedding artifact streams</FormLabel>
            <p className="text-xs text-muted-foreground">
              Add document- and chunk-level embedding outputs that share this vector space.
            </p>
          </div>
          {artifactSources.fields.map((source, index) => (
            <div key={source.id} className="space-y-2 rounded-md border p-3">
              <FormField
                control={control}
                name={`${prefix}artifactSources.${index}.artifact`}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Embedding artifact</FormLabel>
                    <FormControl>
                      <Input placeholder="document_dense_v1" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={control}
                name={`${prefix}artifactSources.${index}.sourceArtifact`}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Upstream artifact (optional)</FormLabel>
                    <FormControl>
                      <Input placeholder="document_chunks_v1" {...field} />
                    </FormControl>
                    <p className="text-xs text-muted-foreground">
                      Leave empty for a document-level vector; set a chunk artifact for chunk
                      vectors.
                    </p>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={control}
                name={`${prefix}artifactSources.${index}.field`}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Embedding input field</FormLabel>
                    <FormControl>
                      <Input placeholder="text" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              {artifactSources.fields.length > 1 && (
                <Button type="button" variant="ghost" onClick={() => artifactSources.remove(index)}>
                  Remove source
                </Button>
              )}
            </div>
          ))}
          <Button
            type="button"
            variant="outline"
            disabled={artifactSources.fields.length >= 64}
            onClick={() =>
              artifactSources.append({ artifact: "", sourceArtifact: "", field: "text" })
            }
          >
            Add artifact source
          </Button>
        </div>
      )}

      <FormField
        control={control}
        name={`${prefix}embedder.model`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Embedder Model</FormLabel>
            <FormControl>
              <Combobox
                options={(modelSuggestions[provider as IndexEmbedderProvider] || []).map(
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
            {provider === "antfly" && inferenceModelsLoading && (
              <p className="text-xs text-muted-foreground">
                Loading models from Antfly inference...
              </p>
            )}
            {provider === "antfly" && !inferenceModelsLoading && (
              <p className="text-xs text-muted-foreground">
                {(liveEmbedders.antfly?.length ?? 0) > 0
                  ? "Select a model or enter a custom name for Antfly inference to pull."
                  : "Enter a model name (e.g., sentence-transformers/all-MiniLM-L6-v2) for Antfly inference to pull."}
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

              {sourceType !== "artifacts" && (
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
              )}

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
