import { ChevronDownIcon, ChevronRightIcon, Cross2Icon } from "@radix-ui/react-icons";
import type React from "react";
import { get, useFormContext } from "react-hook-form";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { TableCell, TableRow } from "@/components/ui/table";
import MultiSelect from "../MultiSelect";
import {
  ANTFLY_TYPES,
  type FieldDetectionInfo,
  RESERVED_FIELD_NAMES,
  truncateValue,
} from "./schema-utils";

interface SchemaFieldRowProps {
  schemaIndex: number;
  fieldIndex: number;
  isExpanded: boolean;
  onToggleExpand: () => void;
  onRemove: () => void;
  detectionInfo?: FieldDetectionInfo;
}

const SchemaFieldRow: React.FC<SchemaFieldRowProps> = ({
  schemaIndex,
  fieldIndex,
  isExpanded,
  onToggleExpand,
  onRemove,
  detectionInfo,
}) => {
  const {
    control,
    formState: { errors },
    watch,
  } = useFormContext();

  const basePath = `document_schemas.${schemaIndex}.properties.${fieldIndex}`;
  const name = watch(`${basePath}.name`) || "";
  const type = watch(`${basePath}.type`) || "string";
  const isIndexed = watch(`${basePath}.x-antfly-index`);
  const antflyTypes = watch(`${basePath}.x-antfly-types`) || [];
  const fieldNameError = get(errors, `${basePath}.name`);

  return (
    <>
      <TableRow
        className="cursor-pointer hover:bg-muted/50"
        onClick={onToggleExpand}
        data-state={isExpanded ? "open" : "closed"}
      >
        <TableCell className="w-[28px] pr-0">
          {isExpanded ? (
            <ChevronDownIcon className="h-4 w-4 text-muted-foreground" />
          ) : (
            <ChevronRightIcon className="h-4 w-4 text-muted-foreground" />
          )}
        </TableCell>
        <TableCell className="font-mono text-sm">
          {name || <span className="text-muted-foreground italic">unnamed</span>}
          {fieldNameError && <span className="text-destructive ml-2 text-xs">*</span>}
        </TableCell>
        <TableCell>
          <Badge variant="secondary">{type}</Badge>
        </TableCell>
        <TableCell>
          {detectionInfo && (
            <Badge variant="outline">{Math.round(detectionInfo.frequency * 100)}%</Badge>
          )}
        </TableCell>
        <TableCell>
          <div className="flex gap-1 flex-wrap">
            {antflyTypes.length > 0 ? (
              antflyTypes.map((t: string) => (
                <Badge key={t} variant="secondary" className="text-xs">
                  {t}
                </Badge>
              ))
            ) : (
              <span className="text-muted-foreground text-xs">none</span>
            )}
          </div>
        </TableCell>
        <TableCell className="w-[40px]">
          <Button
            onClick={(e) => {
              e.stopPropagation();
              onRemove();
            }}
            aria-label="delete field"
            variant="ghost"
            size="icon"
            className="h-7 w-7"
          >
            <Cross2Icon className="h-3.5 w-3.5" />
          </Button>
        </TableCell>
      </TableRow>

      {isExpanded && (
        <TableRow className="hover:bg-transparent">
          <TableCell colSpan={6} className="bg-muted/30 p-4 border-b">
            <div className="grid grid-cols-2 gap-4">
              <FormField
                control={control}
                name={`${basePath}.name`}
                rules={{
                  required: "Field name is required",
                  validate: (value) =>
                    !RESERVED_FIELD_NAMES.includes(value) ||
                    `Field name cannot be one of ${RESERVED_FIELD_NAMES.join(", ")}`,
                }}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Field Name</FormLabel>
                    <FormControl>
                      <Input
                        {...field}
                        placeholder="Enter field name"
                        onClick={(e) => e.stopPropagation()}
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={control}
                name={`${basePath}.type`}
                defaultValue="string"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Type</FormLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FormControl>
                        <SelectTrigger onClick={(e) => e.stopPropagation()}>
                          <SelectValue placeholder="Select a type" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value="string">String</SelectItem>
                        <SelectItem value="number">Number</SelectItem>
                        <SelectItem value="integer">Integer</SelectItem>
                        <SelectItem value="boolean">Boolean</SelectItem>
                        <SelectItem value="object">Object</SelectItem>
                        <SelectItem value="array">Array</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={control}
                name={`${basePath}.description`}
                render={({ field }) => (
                  <FormItem className="col-span-2">
                    <FormLabel>Description</FormLabel>
                    <FormControl>
                      <Input
                        {...field}
                        placeholder="Enter description"
                        onClick={(e) => e.stopPropagation()}
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={control}
                name={`${basePath}.x-antfly-index`}
                defaultValue={true}
                render={({ field }) => (
                  <FormItem>
                    <div className="flex items-center gap-2">
                      <FormControl>
                        <Checkbox
                          checked={field.value}
                          onCheckedChange={field.onChange}
                          onClick={(e) => e.stopPropagation()}
                        />
                      </FormControl>
                      <FormLabel>Index this field</FormLabel>
                    </div>
                    <FormMessage />
                  </FormItem>
                )}
              />

              {isIndexed && (
                <FormField
                  control={control}
                  name={`${basePath}.x-antfly-types`}
                  defaultValue={[]}
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Antfly Types</FormLabel>
                      <FormControl>
                        <MultiSelect
                          options={ANTFLY_TYPES.map((t) => ({
                            label: t.label,
                            value: t.value,
                          }))}
                          value={field.value}
                          onChange={field.onChange}
                          placeholder="Select types"
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              )}

              {detectionInfo && (
                <div className="col-span-2 text-sm text-muted-foreground border-t pt-3 mt-1">
                  <p>
                    <span className="font-medium">Example:</span>{" "}
                    <code className="bg-muted px-1.5 py-0.5 rounded text-xs">
                      {truncateValue(detectionInfo.exampleValue)}
                    </code>
                  </p>
                  <p className="mt-1">
                    Seen in {Math.round(detectionInfo.frequency * 100)}% of{" "}
                    {detectionInfo.sampleCount} sampled documents
                  </p>
                </div>
              )}
            </div>
          </TableCell>
        </TableRow>
      )}
    </>
  );
};

export default SchemaFieldRow;
