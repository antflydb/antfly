import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
  Button,
  DashboardToolbar,
  Form,
  FormActions,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
  Input,
  Switch,
} from "@antfly/design-system";
import type { IndexConfig } from "@antfly/sdk";
import { zodResolver } from "@hookform/resolvers/zod";
import type React from "react";
import { useState } from "react";
import { useFieldArray, useForm } from "react-hook-form";
import { z } from "zod";
import type { TableSchema } from "../../api";
import JsonViewer from "../JsonViewer";
import DocumentSchemasForm from "./DocumentSchemasForm";
import IndexField from "./IndexField";

const indexConfigSchema = z.object({
  name: z.string().min(1),
  type: z.enum(["embeddings", "full_text"]),
  sourceType: z.enum(["field", "template"]).optional(),
  field: z.string().optional(),
  template: z.string().optional(),
  dimension: z.number().optional(),
  embedder: z
    .object({
      provider: z.enum(["ollama", "gemini", "openai", "bedrock", "mock"]),
      model: z.string().min(1),
      api_key: z.string().optional(),
      url: z.string().optional(),
      aws_access_key_id: z.string().optional(),
      aws_secret_access_key: z.string().optional(),
      region: z.string().optional(),
    })
    .optional(),
});

const tableSchemaFormSchema = z
  .object({
    name: z.string().min(1, "Table name is required."),
    num_shards: z.number().min(1),
    indexes: z.array(indexConfigSchema),
  })
  .superRefine((data, ctx) => {
    data.indexes.forEach((index, i) => {
      if (index.type === "embeddings") {
        if (!index.sourceType) {
          ctx.addIssue({
            code: "custom",
            path: [`indexes`, i, "sourceType"],
            message: "Source type is required for vector indexes.",
          });
        }
        if (!index.embedder) {
          ctx.addIssue({
            code: "custom",
            path: [`indexes`, i, "embedder"],
            message: "Embedder config is required for vector indexes.",
          });
        }
      }
    });
  });

type TableSchemaFormData = z.infer<typeof tableSchemaFormSchema>;

interface TableSchemaFormProps {
  onSubmit: (data: {
    name: string;
    schema: TableSchema;
    num_shards: number;
    indexes: IndexConfig[];
  }) => void;
  theme: string;
}

interface JsonPayload {
  name: string;
  schema: TableSchema;
  num_shards: number;
  indexes: IndexConfig[];
}

const TableSchemaForm: React.FC<TableSchemaFormProps> = ({ onSubmit }) => {
  const [viewMode, setViewMode] = useState<"form" | "json">("form");
  const [currentSchema, setCurrentSchema] = useState<TableSchema>({
    document_schemas: {},
  });
  const [jsonPayload, setJsonPayload] = useState<JsonPayload>({
    name: "",
    schema: { document_schemas: {} },
    num_shards: 1,
    indexes: [],
  });

  const form = useForm<TableSchemaFormData>({
    resolver: zodResolver(tableSchemaFormSchema),
    defaultValues: {
      name: "",
      num_shards: 1,
      indexes: [],
    },
  });
  const { control, handleSubmit } = form;
  const {
    fields: indexFields,
    append: appendIndex,
    remove: removeIndex,
  } = useFieldArray({
    control,
    name: "indexes",
  });

  const schemaFields = Object.values(currentSchema.document_schemas || {}).flatMap((docSchema) =>
    Object.keys(docSchema.schema?.properties || {})
  );

  const handleSchemaChange = (schema: TableSchema) => {
    setCurrentSchema(schema);

    // Update JSON payload
    const formData = form.getValues();
    const formattedIndexes = (formData.indexes || []).map((index) => {
      const { sourceType, ...rest } = index as IndexConfig & {
        sourceType: "field" | "template";
      };
      if (sourceType === "field") {
        return { ...rest, template: undefined };
      }
      return { ...rest, field: undefined };
    });

    setJsonPayload({
      name: formData.name || "",
      schema: schema,
      num_shards: formData.num_shards || 1,
      indexes: formattedIndexes,
    });
  };

  const handleFormSubmit = (data: TableSchemaFormData) => {
    if (viewMode === "json") {
      onSubmit(jsonPayload as JsonPayload);
      return;
    }

    const formattedIndexes = data.indexes.map((index) => {
      const { sourceType, ...rest } = index;
      if (sourceType === "field") {
        return { ...rest, template: undefined };
      }
      return { ...rest, field: undefined };
    });

    onSubmit({
      name: data.name,
      schema: currentSchema,
      num_shards: data.num_shards,
      indexes: formattedIndexes,
    });
  };

  const handleViewChange = (checked: boolean) => {
    const newMode = checked ? "json" : "form";
    setViewMode(newMode);
  };

  return (
    <Form form={form} onSubmit={handleSubmit(handleFormSubmit)}>
      <DashboardToolbar className="mb-4 justify-between">
        <h2>Create Table with Schema</h2>
        <div className="flex items-center gap-2">
          <p>Raw JSON</p>
          <Switch checked={viewMode === "json"} onCheckedChange={handleViewChange} />
        </div>
      </DashboardToolbar>
      {viewMode === "json" ? (
        <JsonViewer json={jsonPayload} />
      ) : (
        <>
          <FormField
            control={control}
            name="name"
            render={({ field }) => (
              <FormItem className="mb-4">
                <FormLabel>Table Name</FormLabel>
                <FormControl>
                  <Input {...field} required placeholder="Required: Table Name" />
                </FormControl>
                <FormDescription>Required: Name of this Table.</FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <DocumentSchemasForm
            onSubmit={handleSchemaChange}
            theme=""
            title="Document Schemas"
            renderAsForm={false}
          />

          <h3 className="mt-6 mb-2 font-semibold">Semantic Indexes</h3>
          {indexFields.map((field, index) => (
            <IndexField
              key={field.id}
              index={index}
              onRemove={() => removeIndex(index)}
              schemaFields={schemaFields}
            />
          ))}
          <Button
            type="button"
            variant="outline"
            onClick={() =>
              appendIndex({
                name: "",
                type: "embeddings",
                sourceType: "field",
                field: "",
                dimension: 0,
                embedder: {
                  provider: "ollama",
                  model: "all-minilm",
                },
              })
            }
            className="mt-2 mb-4"
          >
            Add Index
          </Button>

          <Accordion type="single" collapsible className="w-full">
            <AccordionItem value="advanced-settings" className="border-b last:border-b-0">
              <AccordionTrigger>Advanced</AccordionTrigger>
              <AccordionContent>
                <FormField
                  control={control}
                  name="num_shards"
                  render={({ field }) => (
                    <FormItem className="mt-4">
                      <FormLabel>Number of Shards</FormLabel>
                      <FormControl>
                        <Input type="number" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        </>
      )}

      <FormActions className="justify-start mt-2">
        <Button type="submit" variant="default">
          Create Table
        </Button>
      </FormActions>
    </Form>
  );
};

export default TableSchemaForm;
