import type { BatchRequest } from "@antfly/sdk";
import { CircleAlert, CircleCheck } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/shadcn-io/spinner";
import { Textarea } from "@/components/ui/textarea";
import { api, type JSONSchemaProperty, type TableSchema } from "../api";

interface DocumentBuilderProps {
  tableName: string;
  schema?: TableSchema | null;
}

const DocumentBuilder: React.FC<DocumentBuilderProps> = ({ tableName, schema }) => {
  const [documentValues, setDocumentValues] = useState<Record<string, unknown>>({});
  const [selectedSchemaType, setSelectedSchemaType] = useState<string>("");
  const [documentId, setDocumentId] = useState<string>("");
  const [builderLoading, setBuilderLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  // Helper function to generate default values for a schema property
  const generateDefaultValue = useCallback((property: JSONSchemaProperty): unknown => {
    switch (property.type) {
      case "string":
        return "";
      case "number":
      case "integer":
        return 0;
      case "boolean":
        return false;
      case "array":
        return [];
      case "object":
        if (property.properties) {
          const obj: Record<string, unknown> = {};
          Object.entries(property.properties).forEach(([key, prop]) => {
            obj[key] = generateDefaultValue(prop);
          });
          return obj;
        }
        return {};
      default:
        return "";
    }
  }, []);

  // Initialize document values when schema type changes
  useEffect(() => {
    if (selectedSchemaType && schema?.document_schemas?.[selectedSchemaType]?.schema.properties) {
      const properties = schema.document_schemas[selectedSchemaType].schema.properties;
      const defaultValues: Record<string, unknown> = {};
      Object.entries(properties).forEach(([key, property]) => {
        defaultValues[key] = generateDefaultValue(property);
      });
      setDocumentValues(defaultValues);
      setDocumentId(""); // Reset document ID when schema type changes
    }
  }, [selectedSchemaType, schema, generateDefaultValue]);

  // Set default schema type on schema change
  useEffect(() => {
    if (schema?.default_type && schema.document_schemas?.[schema.default_type]) {
      setSelectedSchemaType(schema.default_type);
    }
  }, [schema]);

  const handleSubmitSingleDocument = useCallback(async () => {
    if (!tableName || !selectedSchemaType || !schema) {
      setError("Please select a schema type and fill in the document.");
      return;
    }

    setBuilderLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const docSchema = schema.document_schemas?.[selectedSchemaType];
      if (!docSchema) {
        throw new Error(`Schema type "${selectedSchemaType}" not found.`);
      }

      if (!documentId.trim()) {
        throw new Error("Document ID is required.");
      }

      const batchRequest: BatchRequest = {
        inserts: {
          [documentId]: documentValues,
        },
      };

      await api.tables.batch(tableName, batchRequest);
      setSuccess("Successfully inserted document.");

      // Reset document ID and values to defaults
      setDocumentId("");

      // Reset document values to defaults
      const properties = schema.document_schemas?.[selectedSchemaType]?.schema.properties;
      const defaultValues: Record<string, unknown> = {};
      if (properties) {
        Object.entries(properties).forEach(([key, property]) => {
          defaultValues[key] = generateDefaultValue(property);
        });
      }
      setDocumentValues(defaultValues);
    } catch (err) {
      if (err instanceof Error) {
        setError(`Failed to insert document: ${err.message}`);
      } else {
        setError("An unknown error occurred.");
      }
      console.error(err);
    } finally {
      setBuilderLoading(false);
    }
  }, [tableName, selectedSchemaType, schema, documentValues, documentId, generateDefaultValue]);

  const renderFormField = (fieldName: string, property: JSONSchemaProperty, value: unknown) => {
    const updateValue = (newValue: unknown) => {
      setDocumentValues((prev) => ({ ...prev, [fieldName]: newValue }));
    };

    switch (property.type) {
      case "string":
        return (
          <label key={fieldName} htmlFor={`field-${fieldName}`} className="block space-y-2">
            <span className="text-sm font-medium">{fieldName}</span>
            <Input
              id={`field-${fieldName}`}
              value={typeof value === "string" ? value : ""}
              onChange={(e) => updateValue(e.target.value)}
              placeholder={property.description || `Enter ${fieldName}`}
            />
          </label>
        );
      case "number":
      case "integer":
        return (
          <label key={fieldName} htmlFor={`field-${fieldName}`} className="block space-y-2">
            <span className="text-sm font-medium">{fieldName}</span>
            <Input
              id={`field-${fieldName}`}
              type="number"
              value={typeof value === "number" ? value : ""}
              onChange={(e) =>
                updateValue(
                  property.type === "integer"
                    ? parseInt(e.target.value, 10) || 0
                    : parseFloat(e.target.value) || 0
                )
              }
              placeholder={property.description || `Enter ${fieldName}`}
            />
          </label>
        );
      case "boolean":
        return (
          <div key={fieldName} className="space-y-2">
            <label className="text-sm font-medium flex items-center gap-2">
              <input
                type="checkbox"
                checked={Boolean(value)}
                onChange={(e) => updateValue(e.target.checked)}
                className="h-4 w-4"
              />
              {fieldName}
            </label>
          </div>
        );
      case "array":
      case "object":
        return (
          <label key={fieldName} htmlFor={`field-${fieldName}`} className="block space-y-2">
            <span className="text-sm font-medium">{fieldName}</span>
            <Textarea
              id={`field-${fieldName}`}
              value={JSON.stringify(value, null, 2)}
              onChange={(e) => {
                try {
                  const parsed = JSON.parse(e.target.value);
                  updateValue(parsed);
                } catch {
                  // Invalid JSON, don't update
                }
              }}
              placeholder={property.description || `Enter ${fieldName} as JSON`}
              rows={3}
              className="font-mono"
            />
          </label>
        );
      default:
        return (
          <label key={fieldName} htmlFor={`field-${fieldName}`} className="block space-y-2">
            <span className="text-sm font-medium">{fieldName}</span>
            <Input
              id={`field-${fieldName}`}
              value={String(value || "")}
              onChange={(e) => updateValue(e.target.value)}
              placeholder={property.description || `Enter ${fieldName}`}
            />
          </label>
        );
    }
  };

  if (!schema || !schema.document_schemas || Object.keys(schema.document_schemas).length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Document Builder</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-gray-500">
            No document schemas available for this table. Please configure a schema first.
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Build Document from Schema</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col gap-4">
            <p className="text-gray-500 mb-4">
              Create a document using the table schema as a guide.
            </p>

            <label className="flex flex-col gap-2">
              <span className="text-sm font-medium">Document Type</span>
              <select
                value={selectedSchemaType}
                onChange={(e) => setSelectedSchemaType(e.target.value)}
                className="border border-gray-300 rounded-md px-3 py-2"
              >
                <option value="">Select a document type</option>
                {Object.keys(schema.document_schemas).map((schemaName) => (
                  <option key={schemaName} value={schemaName}>
                    {schemaName}
                  </option>
                ))}
              </select>
            </label>

            {selectedSchemaType && schema.document_schemas[selectedSchemaType] && (
              <div className="space-y-4">
                <label htmlFor="document-id" className="block space-y-2">
                  <span className="text-sm font-medium">Document ID *</span>
                  <Input
                    id="document-id"
                    value={documentId}
                    onChange={(e) => setDocumentId(e.target.value)}
                    placeholder="Enter unique document ID"
                  />
                </label>

                <h4 className="text-lg font-semibold">Document Fields</h4>
                {Object.entries(schema.document_schemas[selectedSchemaType].schema.properties).map(
                  ([fieldName, property]) =>
                    renderFormField(fieldName, property, documentValues[fieldName])
                )}

                <Button
                  onClick={handleSubmitSingleDocument}
                  disabled={!selectedSchemaType || builderLoading}
                  className="self-start"
                >
                  {builderLoading ? <Spinner /> : "Insert Document"}
                </Button>
              </div>
            )}
          </div>
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

export default DocumentBuilder;
