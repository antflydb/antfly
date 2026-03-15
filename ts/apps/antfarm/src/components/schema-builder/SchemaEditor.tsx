import { Cross2Icon, PlusIcon } from "@radix-ui/react-icons";
import { Info } from "lucide-react";
import { InputData, jsonInputForTargetLanguage, quicktype } from "quicktype-core";
import type React from "react";
import { useState } from "react";
import { useFieldArray, useFormContext } from "react-hook-form";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import MultiSelect from "../MultiSelect";
import { ImportJsonDialog } from "./ImportJsonDialog";
import SchemaFieldRow from "./SchemaFieldRow";
import { type FieldDetectionInfo, RESERVED_FIELD_NAMES } from "./schema-utils";

interface SchemaEditorProps {
  schemaIndex: number;
  onRemove: () => void;
  detectionMetaMap?: Map<string, FieldDetectionInfo>;
}

interface SchemaProperty {
  name: string;
  type: string;
  description?: string;
  "x-antfly-index"?: boolean;
  "x-antfly-types"?: string[];
}

const SchemaEditor: React.FC<SchemaEditorProps> = ({
  schemaIndex,
  onRemove,
  detectionMetaMap = new Map(),
}) => {
  const { control, setValue, watch } = useFormContext();
  const { fields, append, remove } = useFieldArray({
    control,
    name: `document_schemas.${schemaIndex}.properties`,
  });

  const [isImportDialogOpen, setImportDialogOpen] = useState(false);
  const [expandedFieldId, setExpandedFieldId] = useState<string | null>(null);

  const properties = watch(`document_schemas.${schemaIndex}.properties`) || [];

  async function handleImport(jsonString: string) {
    if (!jsonString.trim()) return;

    try {
      const lines = jsonString.trim().split("\n");
      let samples = [jsonString];
      if (lines.length > 1) {
        try {
          for (const line of lines) {
            JSON.parse(line);
          }
          samples = lines;
        } catch (e) {
          console.warn("Input is not valid JSONL, treating as single JSON blob.", e);
        }
      }

      const jsonInput = jsonInputForTargetLanguage("json-schema");
      await jsonInput.addSource({ name: "Root", samples });

      const inputData = new InputData();
      inputData.addInput(jsonInput);

      const { lines: schemaLines } = await quicktype({
        inputData,
        lang: "json-schema",
        indentation: "  ",
      });

      const schema = JSON.parse(schemaLines.join("\n"));
      let schemaProperties = schema.properties;

      if (schema.type === "array" && schema.items?.properties) {
        schemaProperties = schema.items.properties;
      } else if (schema.definitions) {
        const firstDefinitionKey = Object.keys(schema.definitions)[0];
        if (firstDefinitionKey) {
          schemaProperties = schema.definitions[firstDefinitionKey].properties;
        }
      }

      if (!schemaProperties) {
        console.warn('Could not find "properties" in the generated schema.');
        return;
      }

      const reservedSet = new Set(RESERVED_FIELD_NAMES);
      const newFields = Object.keys(schemaProperties)
        .filter((key) => !reservedSet.has(key))
        .map((key) => {
          const prop = schemaProperties[key];
          return {
            name: key,
            type: prop.type,
            description: prop.description || "",
            "x-antfly-index": true,
            "x-antfly-types": [],
          };
        });

      setValue(`document_schemas.${schemaIndex}.properties`, newFields);
    } catch (error) {
      console.error("Failed to import from JSON:", error);
    }
  }

  const toggleExpand = (fieldId: string) => {
    setExpandedFieldId((prev) => (prev === fieldId ? null : fieldId));
  };

  return (
    <div className="p-4 border border-border rounded-md mb-4">
      <div className="flex justify-between items-center mb-4">
        <div className="flex gap-4">
          <FormField
            control={control}
            name={`document_schemas.${schemaIndex}.name`}
            render={({ field }) => (
              <FormItem>
                <FormLabel>Schema Name</FormLabel>
                <FormControl>
                  <Input {...field} placeholder="Required: Schema Name" />
                </FormControl>
                <FormDescription>Required: Name of this Document Schema.</FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />
          <FormField
            control={control}
            name={`document_schemas.${schemaIndex}.key`}
            render={({ field }) => (
              <FormItem>
                <FormLabel>Document Key</FormLabel>
                <FormControl>
                  <Input {...field} placeholder="Optional: Document Key" />
                </FormControl>
                <FormDescription>Optional: Field to use as Document Key.</FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />
        </div>
        <Button onClick={onRemove} aria-label="delete schema" variant="ghost" size="icon">
          <Cross2Icon />
        </Button>
      </div>

      <FormField
        control={control}
        name={`document_schemas.${schemaIndex}.x-antfly-include-in-all`}
        defaultValue={[]}
        render={({ field }) => {
          const textBasedFields = (properties as SchemaProperty[])
            .filter((prop: SchemaProperty) => {
              if (!prop?.name) return false;
              const types = prop["x-antfly-types"] || [];
              const hasTextTypes = types.some((t: string) =>
                ["text", "html", "keyword", "search_as_you_type", "link"].includes(t)
              );
              const isStringType = prop.type === "string" && types.length === 0;
              return hasTextTypes || isStringType;
            })
            .map((prop: SchemaProperty) => prop.name);

          return (
            <FormItem className="mb-4">
              <FormLabel>Include in _all Field</FormLabel>
              <FormControl>
                <MultiSelect
                  options={textBasedFields.map((name: string) => ({
                    label: name,
                    value: name,
                  }))}
                  value={field.value || []}
                  onChange={field.onChange}
                  placeholder="Select fields to include in _all"
                />
              </FormControl>
              <FormDescription>
                Select text-based fields to include in the _all search field for full-text search
                across multiple fields.
              </FormDescription>
              <FormMessage />
            </FormItem>
          );
        }}
      />

      <h4 className="text-lg font-semibold mb-2">Fields</h4>

      <Alert className="mb-4">
        <Info className="h-4 w-4" />
        <AlertTitle>Info</AlertTitle>
        <AlertDescription>
          The _type field is reserved and will be used to determine the document type on upload.
        </AlertDescription>
      </Alert>

      <div className="flex gap-2 mb-4">
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => {
            append({
              name: "",
              type: "string",
              description: "",
              "x-antfly-index": true,
              "x-antfly-types": [],
            });
          }}
        >
          <PlusIcon className="h-4 w-4 mr-1" />
          Add Field
        </Button>
        <Button type="button" variant="outline" size="sm" onClick={() => setImportDialogOpen(true)}>
          Import from JSON
        </Button>
      </div>

      {fields.length > 0 ? (
        <div className="border rounded-md">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[28px]" />
                <TableHead>Field</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Seen</TableHead>
                <TableHead>Antfly Types</TableHead>
                <TableHead className="w-[40px]" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {fields.map((field, index) => {
                const fieldName = properties[index]?.name;
                return (
                  <SchemaFieldRow
                    key={field.id}
                    schemaIndex={schemaIndex}
                    fieldIndex={index}
                    isExpanded={expandedFieldId === field.id}
                    onToggleExpand={() => toggleExpand(field.id)}
                    onRemove={() => remove(index)}
                    detectionInfo={fieldName ? detectionMetaMap.get(fieldName) : undefined}
                  />
                );
              })}
            </TableBody>
          </Table>
        </div>
      ) : (
        <div className="border rounded-md p-8 text-center text-muted-foreground">
          <p>No fields defined yet.</p>
          <p className="text-sm mt-1">Add fields manually or import from JSON.</p>
        </div>
      )}

      <ImportJsonDialog
        open={isImportDialogOpen}
        onClose={() => setImportDialogOpen(false)}
        onImport={handleImport}
      />
    </div>
  );
};

export default SchemaEditor;
