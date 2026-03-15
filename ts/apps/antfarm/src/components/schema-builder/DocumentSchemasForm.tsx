import { zodResolver } from "@hookform/resolvers/zod";
import { MagnifyingGlassIcon, ReloadIcon } from "@radix-ui/react-icons";
import type React from "react";
import { useCallback, useEffect, useState } from "react";
import { useFieldArray, useForm } from "react-hook-form";
import { z } from "zod";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { useFieldDetection } from "@/hooks/use-field-detection";
import type { JSONSchema, JSONSchemaProperty, TableSchema } from "../../api";
import JsonViewer from "../JsonViewer";
import { ImportJsonDialog } from "./ImportJsonDialog";
import SchemaEditor from "./SchemaEditor";
import {
  type FieldDetectionInfo,
  getDefaultAntflyType,
  inferJSONType,
  RESERVED_FIELD_NAMES,
} from "./schema-utils";

const antflyTypeEnum = z.enum([
  "boolean",
  "link",
  "text",
  "html",
  "numeric",
  "search_as_you_type",
  "keyword",
  "datetime",
  "geopoint",
  "geoshape",
  "embedding",
  "blob",
]);

const jsonSchemaPropertySchema = z.object({
  name: z.string().min(1),
  type: z.enum(["string", "number", "integer", "boolean", "object", "array"]),
  description: z.string().optional(),
  "x-antfly-index": z.boolean().optional(),
  "x-antfly-types": z.array(antflyTypeEnum).optional(),
});

const documentSchemaSchema = z.object({
  name: z.string().min(1),
  properties: z.array(jsonSchemaPropertySchema),
  "x-antfly-include-in-all": z.array(z.string()).optional(),
});

const documentSchemasFormSchema = z.object({
  default_type: z.string().optional(),
  document_schemas: z.array(documentSchemaSchema),
});

type DocumentSchemasFormData = z.infer<typeof documentSchemasFormSchema>;

interface DocumentSchemasFormProps {
  onSubmit: (schema: Omit<TableSchema, "key"> & { key?: string }) => void;
  theme: string;
  initialSchema?: TableSchema | null;
  title?: string;
  renderAsForm?: boolean;
  tableName?: string;
}

interface JsonPayload {
  schema: TableSchema;
}

const DocumentSchemasForm: React.FC<DocumentSchemasFormProps> = ({
  onSubmit,
  initialSchema,
  title = "Edit Document Schemas",
  renderAsForm = true,
  tableName,
}) => {
  "use no memo";
  const [viewMode, setViewMode] = useState<"form" | "json">("form");
  const [jsonPayload, setJsonPayload] = useState<JsonPayload>({
    schema: initialSchema || { document_schemas: {} },
  });
  const [detectionMetaMap, setDetectionMetaMap] = useState<Map<string, FieldDetectionInfo>>(
    new Map()
  );

  const { detect, isDetecting, detectionError, detectedFields, detectionGroups, sampleCount } =
    useFieldDetection(tableName);

  const form = useForm<DocumentSchemasFormData>({
    resolver: zodResolver(documentSchemasFormSchema),
    defaultValues: {
      default_type: initialSchema?.default_type || "",
      document_schemas: initialSchema
        ? Object.entries(initialSchema.document_schemas || {}).map(
            ([schemaName, schemaDetails]) => ({
              name: schemaName,
              "x-antfly-include-in-all": schemaDetails.schema["x-antfly-include-in-all"] || [],
              properties: Object.entries(schemaDetails.schema.properties || {}).map(
                ([propName, propDetails]) => {
                  const prop = propDetails as JSONSchemaProperty;
                  return {
                    name: propName,
                    type: prop.type,
                    description: prop.description,
                    "x-antfly-index": prop["x-antfly-index"],
                    "x-antfly-types": prop["x-antfly-types"],
                  };
                }
              ),
            })
          )
        : [],
    },
  });

  const { watch, reset } = form;
  const {
    fields: documentSchemaFields,
    append: appendDocumentSchema,
    remove: removeDocumentSchema,
  } = useFieldArray({
    control: form.control,
    name: "document_schemas",
  });

  const handleDetect = useCallback(async () => {
    await detect();

    // Use detectionGroups from the hook (set via state after detect() resolves)
  }, [detect]);

  // After detection completes, merge grouped results into schemas
  useEffect(() => {
    if (detectionGroups.length === 0) return;

    type SchemaProperty = DocumentSchemasFormData["document_schemas"][number]["properties"][number];
    const schemas = watch("document_schemas") || [];

    for (const group of detectionGroups) {
      // Find existing schema with matching name
      let targetIndex = schemas.findIndex((s) => s.name === group.typeName);

      if (targetIndex === -1) {
        // Create new schema for this type
        appendDocumentSchema({ name: group.typeName, properties: [] });
        targetIndex = schemas.length; // will be appended at the end
      }

      const currentProps =
        (watch(`document_schemas.${targetIndex}.properties`) as SchemaProperty[]) || [];
      const existingNames = new Set(currentProps.map((p) => p.name));

      const newFields: SchemaProperty[] = group.fields
        .filter((detected) => !existingNames.has(detected.name))
        .map((detected) => ({
          name: detected.name,
          type: (detected.inferredType === "array"
            ? "array"
            : detected.inferredType) as SchemaProperty["type"],
          description: "",
          "x-antfly-index": true,
          "x-antfly-types": detected.suggestedAntflyTypes as SchemaProperty["x-antfly-types"],
        }));

      if (newFields.length > 0) {
        form.setValue(`document_schemas.${targetIndex}.properties`, [
          ...currentProps,
          ...newFields,
        ]);
      }
    }

    // Build combined meta map from all groups
    const newMetaMap = new Map<string, FieldDetectionInfo>();
    for (const group of detectionGroups) {
      for (const detected of group.fields) {
        newMetaMap.set(detected.name, {
          frequency: detected.frequency,
          sampleCount: detected.sampleCount,
          exampleValue: detected.exampleValue,
        });
      }
    }
    setDetectionMetaMap(newMetaMap);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    detectionGroups, // Create new schema for this type
    appendDocumentSchema,
    form.setValue,
    watch,
  ]);

  const [isJsonDetectDialogOpen, setJsonDetectDialogOpen] = useState(false);

  const handleDetectFromJson = useCallback(
    (jsonString: string) => {
      if (!jsonString.trim()) return;

      // Parse JSON or JSONL into documents
      const docs: Record<string, unknown>[] = [];
      const lines = jsonString.trim().split("\n");

      // Try JSONL first (each line is a JSON object)
      let isJsonl = lines.length > 1;
      if (isJsonl) {
        try {
          for (const line of lines) {
            const parsed = JSON.parse(line.trim());
            if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
              docs.push(parsed as Record<string, unknown>);
            }
          }
        } catch {
          isJsonl = false;
        }
      }

      if (!isJsonl) {
        try {
          const parsed = JSON.parse(jsonString);
          if (Array.isArray(parsed)) {
            for (const item of parsed) {
              if (typeof item === "object" && item !== null) {
                docs.push(item as Record<string, unknown>);
              }
            }
          } else if (typeof parsed === "object" && parsed !== null) {
            docs.push(parsed as Record<string, unknown>);
          }
        } catch {
          return; // Invalid JSON
        }
      }

      if (docs.length === 0) return;

      // Group by _type
      const reservedNames = new Set(RESERVED_FIELD_NAMES);
      const typeGroups = new Map<string, Record<string, unknown>[]>();
      for (const doc of docs) {
        const typeName = typeof doc._type === "string" ? doc._type : "default";
        const group = typeGroups.get(typeName) || [];
        group.push(doc);
        typeGroups.set(typeName, group);
      }

      type SchemaProperty =
        DocumentSchemasFormData["document_schemas"][number]["properties"][number];
      const schemas = watch("document_schemas") || [];
      const newMetaMap = new Map<string, FieldDetectionInfo>();

      for (const [typeName, typeDocs] of typeGroups) {
        // Detect fields for this type
        const fieldMap = new Map<
          string,
          {
            name: string;
            exampleValue: unknown;
            seenCount: number;
            inferredType: ReturnType<typeof inferJSONType>;
          }
        >();

        for (const doc of typeDocs) {
          for (const [key, value] of Object.entries(doc)) {
            if (key.startsWith("_") || reservedNames.has(key)) continue;
            const existing = fieldMap.get(key);
            if (existing) {
              existing.seenCount++;
              if (existing.exampleValue === null && value !== null) {
                existing.exampleValue = value;
              }
            } else {
              fieldMap.set(key, {
                name: key,
                exampleValue: value,
                seenCount: 1,
                inferredType: inferJSONType(value),
              });
            }
          }
        }

        // Find or create schema for this type
        let targetIndex = schemas.findIndex((s) => s.name === typeName);
        if (targetIndex === -1) {
          appendDocumentSchema({ name: typeName, properties: [] });
          targetIndex = schemas.length;
        }

        const currentProps =
          (watch(`document_schemas.${targetIndex}.properties`) as SchemaProperty[]) || [];
        const existingNames = new Set(currentProps.map((p) => p.name));

        const newFields: SchemaProperty[] = Array.from(fieldMap.values())
          .sort((a, b) => b.seenCount - a.seenCount)
          .filter((f) => !existingNames.has(f.name))
          .map((f) => ({
            name: f.name,
            type: (f.inferredType === "array" ? "array" : f.inferredType) as SchemaProperty["type"],
            description: "",
            "x-antfly-index": true,
            "x-antfly-types": getDefaultAntflyType(
              f.inferredType,
              f.exampleValue
            ) as SchemaProperty["x-antfly-types"],
          }));

        if (newFields.length > 0) {
          form.setValue(`document_schemas.${targetIndex}.properties`, [
            ...currentProps,
            ...newFields,
          ]);
        }

        // Build meta map
        for (const f of fieldMap.values()) {
          newMetaMap.set(f.name, {
            frequency: f.seenCount / typeDocs.length,
            sampleCount: typeDocs.length,
            exampleValue: f.exampleValue,
          });
        }
      }

      setDetectionMetaMap(newMetaMap);
    },
    [watch, form, appendDocumentSchema]
  );

  const definedSchemaNames = watch("document_schemas")?.map((s) => s.name) || [];

  useEffect(() => {
    if (viewMode === "form") {
      const subscription = watch((data) => {
        const formattedSchema: TableSchema = {
          default_type: data.default_type,
          document_schemas: (data.document_schemas || []).reduce(
            (acc, schema) => {
              if (!schema || !schema.name) return acc;
              const jsonSchema: JSONSchema = {
                type: "object",
                properties: (schema.properties || []).reduce(
                  (propAcc, prop) => {
                    if (!prop || !prop.name) return propAcc;
                    const { name, ...rest } = prop;
                    propAcc[name] = rest as JSONSchemaProperty;
                    return propAcc;
                  },
                  {} as { [key: string]: JSONSchemaProperty }
                ),
              };

              // Add x-antfly-include-in-all if present and non-empty
              const includeInAll =
                schema["x-antfly-include-in-all"]?.filter(
                  (f): f is string => typeof f === "string"
                ) || [];
              if (includeInAll.length > 0) {
                jsonSchema["x-antfly-include-in-all"] = includeInAll;
              }

              acc[schema.name] = {
                schema: jsonSchema,
              };
              return acc;
            },
            {} as { [key: string]: { schema: JSONSchema } }
          ),
        };

        setJsonPayload({
          schema: formattedSchema,
        });
      });
      return () => subscription.unsubscribe();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [viewMode, watch]);

  const handleFormSubmit = (data: DocumentSchemasFormData) => {
    if (viewMode === "json") {
      onSubmit(jsonPayload.schema);
      return;
    }

    // Transform the array of schemas into the map format required by the API
    const formattedSchema: TableSchema = {
      default_type: data.default_type,
      document_schemas: data.document_schemas.reduce(
        (acc, schema) => {
          const jsonSchema: JSONSchema = {
            type: "object",
            properties: schema.properties.reduce(
              (propAcc, prop) => {
                const { name, ...rest } = prop;
                propAcc[name] = rest;
                return propAcc;
              },
              {} as { [key: string]: JSONSchemaProperty }
            ),
          };

          // Add x-antfly-include-in-all if present and non-empty
          const includeInAll =
            schema["x-antfly-include-in-all"]?.filter((f): f is string => typeof f === "string") ||
            [];
          if (includeInAll.length > 0) {
            jsonSchema["x-antfly-include-in-all"] = includeInAll;
          }

          acc[schema.name] = {
            schema: jsonSchema,
          };
          return acc;
        },
        {} as { [key: string]: { schema: JSONSchema } }
      ),
    };

    onSubmit(formattedSchema);
  };

  const handleViewChange = (checked: boolean) => {
    const newMode = checked ? "json" : "form";
    if (newMode === "form") {
      const { schema } = jsonPayload;

      if (schema) {
        const document_schemas = Object.entries(schema.document_schemas || {}).map(
          ([schemaName, schemaDetails]: [string, { schema: JSONSchema }]) => ({
            name: schemaName,
            "x-antfly-include-in-all": schemaDetails.schema["x-antfly-include-in-all"] || [],
            properties: Object.entries(schemaDetails.schema.properties || {}).map(
              ([propName, propDetails]) => {
                const prop = propDetails as JSONSchemaProperty;
                return {
                  name: propName,
                  type: prop.type,
                  description: prop.description,
                  "x-antfly-index": prop["x-antfly-index"],
                  "x-antfly-types": prop["x-antfly-types"],
                };
              }
            ),
          })
        );

        const newData: DocumentSchemasFormData = {
          default_type: schema.default_type || "",
          document_schemas: document_schemas,
        };

        reset(newData);
      }
    }
    setViewMode(newMode);
  };

  const content = (
    <>
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-xl font-semibold">{title}</h3>
        {renderAsForm && (
          <div className="flex items-center gap-2">
            <p>Raw JSON</p>
            <Switch checked={viewMode === "json"} onCheckedChange={handleViewChange} />
          </div>
        )}
      </div>
      {viewMode === "json" && renderAsForm ? (
        <JsonViewer json={jsonPayload.schema} />
      ) : (
        <>
          <h4 className="text-lg font-semibold mb-2">Document Schemas</h4>

          <div className="flex gap-2 mb-4">
            {tableName && (
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={isDetecting}
                onClick={handleDetect}
              >
                {isDetecting && <ReloadIcon className="h-4 w-4 mr-1 animate-spin" />}
                <MagnifyingGlassIcon className="h-4 w-4 mr-1" />
                Detect from Data
              </Button>
            )}
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => setJsonDetectDialogOpen(true)}
            >
              <MagnifyingGlassIcon className="h-4 w-4 mr-1" />
              Detect from JSON
            </Button>
          </div>

          {detectionError && (
            <Alert variant="destructive" className="mb-4">
              <AlertDescription>{detectionError}</AlertDescription>
            </Alert>
          )}

          {sampleCount > 0 && detectedFields.length > 0 && (
            <div className="text-sm text-muted-foreground mb-4">
              <p>
                Detected {detectedFields.length} fields from {sampleCount} sampled documents
              </p>
              {detectionGroups.length > 1 && (
                <p className="mt-1">
                  Found {detectionGroups.length} document types:{" "}
                  {detectionGroups
                    .map((g) => `${g.typeName} (${g.docCount} docs, ${g.fields.length} fields)`)
                    .join(", ")}
                </p>
              )}
            </div>
          )}

          {documentSchemaFields.map((field, index) => (
            <SchemaEditor
              key={field.id}
              schemaIndex={index}
              onRemove={() => removeDocumentSchema(index)}
              detectionMetaMap={detectionMetaMap}
            />
          ))}
          <Button
            type="button"
            variant="outline"
            onClick={() => appendDocumentSchema({ name: "", properties: [] })}
            className="mt-2 mb-4"
          >
            Add Schema Type
          </Button>

          {definedSchemaNames.length > 0 && (
            <FormField
              control={form.control}
              name="default_type"
              render={({ field }) => (
                <FormItem className="mb-4">
                  <FormLabel>Default Schema Type</FormLabel>
                  <Select onValueChange={field.onChange} value={field.value}>
                    <FormControl>
                      <SelectTrigger>
                        <SelectValue placeholder="Select a default schema type" />
                      </SelectTrigger>
                    </FormControl>
                    <SelectContent>
                      {definedSchemaNames
                        .filter((name) => name !== "")
                        .map((name: string) => (
                          <SelectItem key={name} value={name}>
                            {name}
                          </SelectItem>
                        ))}
                    </SelectContent>
                  </Select>
                  <FormMessage />
                </FormItem>
              )}
            />
          )}
        </>
      )}

      {renderAsForm && (
        <div className="mt-6 flex gap-2">
          <Button type="submit" variant="default">
            Update Schema
          </Button>
        </div>
      )}

      <ImportJsonDialog
        open={isJsonDetectDialogOpen}
        onClose={() => setJsonDetectDialogOpen(false)}
        onImport={handleDetectFromJson}
      />
    </>
  );

  return renderAsForm ? (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(handleFormSubmit)}>{content}</form>
    </Form>
  ) : (
    <Form {...form}>{content}</Form>
  );
};

export default DocumentSchemasForm;
