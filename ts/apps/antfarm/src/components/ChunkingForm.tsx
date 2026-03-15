import type React from "react";
import { useFormContext } from "react-hook-form";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface ChunkingFormProps {
  fieldPrefix?: string;
}

const ChunkingForm: React.FC<ChunkingFormProps> = ({ fieldPrefix = "" }) => {
  const { control, watch } = useFormContext();
  const prefix = fieldPrefix ? `${fieldPrefix}.` : "";

  // Watch the model field to conditionally show threshold
  const model = watch(`${prefix}model`);

  return (
    <>
      {/* Hidden provider field - always "termite" */}
      <FormField
        control={control}
        name={`${prefix}provider`}
        render={({ field }) => <input type="hidden" {...field} value="termite" />}
      />

      <FormField
        control={control}
        name={`${prefix}model`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Chunking Model</FormLabel>
            <Select value={field.value} onValueChange={field.onChange}>
              <FormControl>
                <SelectTrigger>
                  <SelectValue placeholder="Select model" />
                </SelectTrigger>
              </FormControl>
              <SelectContent>
                <SelectItem value="fixed">Fixed Token Size</SelectItem>
                <SelectItem value="chonky-mmbert-small-multilingual-1">
                  Chonky (ONNX Semantic)
                </SelectItem>
              </SelectContent>
            </Select>
            <p className="text-sm text-muted-foreground">
              Fixed uses simple token-based chunking. Other models use ONNX-accelerated chunking
              (requires models in chunker_models_dir).
            </p>
            <FormMessage />
          </FormItem>
        )}
      />

      <FormField
        control={control}
        name={`${prefix}target_tokens`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Target Tokens</FormLabel>
            <FormControl>
              <Input
                type="number"
                {...field}
                onChange={(e) => field.onChange(parseInt(e.target.value, 10) || 0)}
              />
            </FormControl>
            <p className="text-sm text-muted-foreground">Target number of tokens per chunk</p>
            <FormMessage />
          </FormItem>
        )}
      />

      <FormField
        control={control}
        name={`${prefix}overlap_tokens`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Overlap Tokens</FormLabel>
            <FormControl>
              <Input
                type="number"
                {...field}
                onChange={(e) => field.onChange(parseInt(e.target.value, 10) || 0)}
              />
            </FormControl>
            <p className="text-sm text-muted-foreground">
              Number of tokens to overlap between chunks
            </p>
            <FormMessage />
          </FormItem>
        )}
      />

      <FormField
        control={control}
        name={`${prefix}separator`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Separator</FormLabel>
            <FormControl>
              <Input type="text" {...field} placeholder="\n\n" />
            </FormControl>
            <p className="text-sm text-muted-foreground">
              Text separator for chunking (e.g., paragraph breaks)
            </p>
            <FormMessage />
          </FormItem>
        )}
      />

      <FormField
        control={control}
        name={`${prefix}max_chunks`}
        render={({ field }) => (
          <FormItem>
            <FormLabel>Max Chunks</FormLabel>
            <FormControl>
              <Input
                type="number"
                {...field}
                onChange={(e) => field.onChange(parseInt(e.target.value, 10) || 0)}
              />
            </FormControl>
            <p className="text-sm text-muted-foreground">Maximum number of chunks per document</p>
            <FormMessage />
          </FormItem>
        )}
      />

      {/* Threshold field - only shown for non-fixed models */}
      {model && model !== "fixed" && (
        <FormField
          control={control}
          name={`${prefix}threshold`}
          render={({ field }) => (
            <FormItem>
              <FormLabel>Threshold</FormLabel>
              <FormControl>
                <Input
                  type="number"
                  step="0.01"
                  {...field}
                  onChange={(e) => field.onChange(parseFloat(e.target.value) || 0)}
                />
              </FormControl>
              <p className="text-sm text-muted-foreground">
                Semantic similarity threshold for Hugot chunking (0.0 - 1.0)
              </p>
              <FormMessage />
            </FormItem>
          )}
        />
      )}

      {/* Advanced settings accordion */}
      <Accordion type="single" collapsible>
        <AccordionItem value="advanced" className="border-b last:border-b-0">
          <AccordionTrigger>Advanced Settings</AccordionTrigger>
          <AccordionContent>
            <FormField
              control={control}
              name={`${prefix}api_url`}
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Termite API URL</FormLabel>
                  <FormControl>
                    <Input type="text" {...field} placeholder="http://localhost:11433" />
                  </FormControl>
                  <p className="text-sm text-muted-foreground">
                    Override the default Termite service URL (leave empty to use cluster default)
                  </p>
                  <FormMessage />
                </FormItem>
              )}
            />
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </>
  );
};

export default ChunkingForm;
