import type { BatchRequest } from "@antfly/sdk";
import { zodResolver } from "@hookform/resolvers/zod";
import { CircleAlert, CircleCheck } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/shadcn-io/spinner";
import { Textarea } from "@/components/ui/textarea";

import { api, type TableSchema } from "../api";

interface BulkInsertProps {
  tableName: string;
}

interface TableGetResponse {
  schema: TableSchema;
}

const insertSchema = z.object({
  jsonFile: z.any().refine((files) => files?.length === 1, "File is required."),
  idField: z.string().min(1, "ID Field is required."),
});

type InsertFormData = z.infer<typeof insertSchema>;

const BulkInsert: React.FC<BulkInsertProps> = ({ tableName }) => {
  const [fileContent, setFileContent] = useState<string>("");
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const idFieldInputRef = useRef<HTMLInputElement>(null);
  const [idFieldSuggestions, setIdFieldSuggestions] = useState<string[]>([]);

  const form = useForm<InsertFormData>({
    resolver: zodResolver(insertSchema),
  });

  const { watch, setValue } = form;
  const idField = watch("idField");
  const jsonFile = watch("jsonFile");

  useEffect(() => {
    const fetchTableSchema = async () => {
      if (!tableName) return;
      try {
        const response = (await api.tables.get(tableName)) as unknown as TableGetResponse;
        if (response.schema && Object.keys(response.schema).length > 0) {
          const tableSchema = response.schema;
          const defaultSchemaName = tableSchema.default_type;
          if (defaultSchemaName && tableSchema.document_schemas) {
            const defaultSchema = tableSchema.document_schemas[defaultSchemaName];
            if (defaultSchema?.key) {
              setValue("idField", defaultSchema.key);
            }
          }
        }
      } catch {
        // This is a 404, so we can ignore it.
      }
    };
    fetchTableSchema();
  }, [tableName, setValue]);

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = event.target.files?.[0];
    if (selectedFile) {
      setValue("jsonFile", event.target.files);
      const reader = new FileReader();
      reader.onload = () => {
        const content = reader.result as string;
        setFileContent(content);
        if (content) {
          try {
            const firstLine = content.split("\n")[0];
            if (firstLine) {
              const doc = JSON.parse(firstLine);
              const keys = Object.keys(doc);
              setIdFieldSuggestions(keys);
            }
          } catch (err) {
            console.error("Failed to parse first line of file:", err);
            setIdFieldSuggestions([]);
          }
        }
      };
      reader.onerror = () => {
        console.error("Failed to read file.");
        setIdFieldSuggestions([]);
      };
      reader.readAsText(selectedFile);
    }
  };

  const insertFieldAtCursor = useCallback(
    (fieldName: string) => {
      const input = idFieldInputRef.current;
      const template = `{{${fieldName}}}`;
      if (input) {
        const start = input.selectionStart ?? input.value.length;
        const end = input.selectionEnd ?? start;
        const newValue = input.value.slice(0, start) + template + input.value.slice(end);
        setValue("idField", newValue, { shouldValidate: true });
        requestAnimationFrame(() => {
          const cursorPos = start + template.length;
          input.focus();
          input.setSelectionRange(cursorPos, cursorPos);
        });
      } else {
        const current = form.getValues("idField") ?? "";
        setValue("idField", current + template, { shouldValidate: true });
      }
    },
    [setValue, form]
  );

  const onSubmit = useCallback(async () => {
    if (!jsonFile || !idField || !tableName) {
      setError("Please select a file and specify the ID field.");
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    const reader = new FileReader();
    reader.onload = async () => {
      const content = reader.result as string;
      if (!content) {
        setError("Failed to read file content.");
        setLoading(false);
        return;
      }

      try {
        const lines = content.split("\n").filter((line) => line.trim() !== "");
        const inserts: BatchRequest["inserts"] = {};
        const isTemplate = idField.includes("{{");

        for (const line of lines) {
          const doc = JSON.parse(line);
          let id: string;

          if (isTemplate) {
            id = idField.replace(/\{\{(\w+)\}\}/g, (_, key: string) => {
              const val = doc[key];
              if (val === undefined || val === null) {
                throw new Error(
                  `Template field "{{${key}}}" not found in document: ${line.slice(0, 100)}`
                );
              }
              return String(val);
            });
          } else {
            id = doc[idField];
          }

          if (!id) {
            throw new Error(
              `ID field "${idField}" produced an empty ID for document: ${line.slice(0, 100)}`
            );
          }
          inserts[id] = doc;
        }

        const batchRequest: BatchRequest = { inserts };
        await api.tables.batch(tableName, batchRequest);
        setSuccess(`Successfully inserted ${Object.keys(inserts).length} documents.`);
        form.reset();
        setFileContent("");
        setIdFieldSuggestions([]);
        if (fileInputRef.current) {
          fileInputRef.current.value = "";
        }
      } catch (err) {
        if (err instanceof Error) {
          setError(`Failed to process file: ${err.message}`);
        } else {
          setError("An unknown error occurred.");
        }
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    reader.onerror = () => {
      setError("Failed to read file.");
      setLoading(false);
    };

    reader.readAsText(jsonFile[0]);
  }, [jsonFile, idField, tableName, form]);

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Insert from File</CardTitle>
        </CardHeader>
        <CardContent>
          <Form {...form}>
            <form className="flex flex-col gap-4" onSubmit={form.handleSubmit(onSubmit)}>
              <p className="text-gray-500 mb-4">
                Upload a newline-delimited JSON file. Each line should be a valid JSON object. The
                ID field supports Handlebars-style templates like{" "}
                <code className="text-xs bg-muted px-1 py-0.5 rounded">
                  {"{{category}}-{{slug}}"}
                </code>{" "}
                to compose IDs from multiple fields.
              </p>

              <FormField
                control={form.control}
                name="jsonFile"
                render={() => (
                  <FormItem>
                    <FormLabel>JSON File</FormLabel>
                    <FormControl>
                      <input
                        type="file"
                        accept=".json,.jsonl"
                        onChange={handleFileChange}
                        ref={fileInputRef}
                        className="block w-full max-w-full text-sm text-gray-500 truncate file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-violet-50 file:text-violet-700 hover:file:bg-violet-100"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              {fileContent && (
                <Textarea
                  value={fileContent}
                  readOnly
                  placeholder="File content will appear here"
                  rows={8}
                  className="w-full max-h-64 overflow-auto break-all whitespace-pre-wrap"
                />
              )}

              <FormField
                control={form.control}
                name="idField"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>ID Field</FormLabel>
                    <FormControl>
                      <Input
                        {...field}
                        ref={(el) => {
                          field.ref(el);
                          (
                            idFieldInputRef as React.MutableRefObject<HTMLInputElement | null>
                          ).current = el;
                        }}
                        placeholder="e.g. id or {{category}}-{{slug}}"
                      />
                    </FormControl>
                    <FormDescription>
                      A field name (e.g. <code className="text-xs">id</code>) or a template (e.g.{" "}
                      <code className="text-xs">{"{{category}}-{{slug}}"}</code>).
                    </FormDescription>
                    {idFieldSuggestions.length > 0 && (
                      <div className="flex flex-wrap gap-1 pt-1">
                        {idFieldSuggestions.map((name) => (
                          <Badge
                            key={name}
                            variant="secondary"
                            className="cursor-pointer hover:bg-secondary/80"
                            onClick={() => insertFieldAtCursor(name)}
                          >
                            {name}
                          </Badge>
                        ))}
                      </div>
                    )}
                    <FormMessage />
                  </FormItem>
                )}
              />

              <Button
                type="submit"
                disabled={!jsonFile || !idField || loading}
                className="self-start"
              >
                {loading ? <Spinner /> : "Upload File"}
              </Button>
            </form>
          </Form>
        </CardContent>
      </Card>

      {error && (
        <Alert variant="destructive">
          <CircleAlert className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {success && (
        <Alert>
          <CircleCheck className="h-4 w-4" />
          <AlertTitle>Success</AlertTitle>
          <AlertDescription>{success}</AlertDescription>
        </Alert>
      )}
    </div>
  );
};

export default BulkInsert;
