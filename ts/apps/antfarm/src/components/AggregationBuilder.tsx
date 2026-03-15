import { PlusIcon } from "@radix-ui/react-icons";
import type React from "react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { BasicField, SearchableField } from "@/utils/fieldUtils";
import FieldSelector from "./querybuilder/FieldSelector";

const BUCKETING_TYPES = [
  { value: "terms", label: "Terms" },
  { value: "range", label: "Range" },
  { value: "date_range", label: "Date Range" },
  { value: "histogram", label: "Histogram" },
  { value: "date_histogram", label: "Date Histogram" },
] as const;

const METRIC_TYPES = [
  { value: "sum", label: "Sum" },
  { value: "avg", label: "Average" },
  { value: "min", label: "Min" },
  { value: "max", label: "Max" },
  { value: "count", label: "Count" },
  { value: "stats", label: "Stats" },
  { value: "cardinality", label: "Cardinality" },
] as const;

const CALENDAR_INTERVALS = ["minute", "hour", "day", "week", "month", "quarter", "year"] as const;

type AggregationType =
  | (typeof BUCKETING_TYPES)[number]["value"]
  | (typeof METRIC_TYPES)[number]["value"];

interface RangeEntry {
  name: string;
  from?: string;
  to?: string;
}

export interface AggregationConfig {
  type: AggregationType;
  field: string;
  size?: number;
  ranges?: RangeEntry[];
  date_ranges?: RangeEntry[];
  interval?: number;
  calendar_interval?: string;
}

interface AggregationBuilderProps {
  availableFields?: SearchableField[];
  availableBasicFields?: BasicField[];
  onAdd: (name: string, config: AggregationConfig) => void;
}

const AggregationBuilder: React.FC<AggregationBuilderProps> = ({
  availableFields = [],
  availableBasicFields = [],
  onAdd,
}) => {
  const [name, setName] = useState("");
  const [type, setType] = useState<AggregationType>("terms");
  const [field, setField] = useState("");
  const [size, setSize] = useState(10);
  const [interval, setInterval] = useState(10);
  const [calendarInterval, setCalendarInterval] = useState<string>("day");
  const [ranges, setRanges] = useState<RangeEntry[]>([{ name: "", from: "", to: "" }]);

  const isBucketing = BUCKETING_TYPES.some((t) => t.value === type);

  const handleAdd = () => {
    if (!name.trim() || !field.trim()) return;

    const config: AggregationConfig = { type, field };

    if (type === "terms") {
      config.size = size;
    }

    if (type === "range") {
      config.ranges = ranges
        .filter((r) => r.name)
        .map((r) => ({
          name: r.name,
          ...(r.from ? { from: r.from } : {}),
          ...(r.to ? { to: r.to } : {}),
        }));
    }

    if (type === "date_range") {
      config.date_ranges = ranges
        .filter((r) => r.name)
        .map((r) => ({
          name: r.name,
          ...(r.from ? { from: r.from } : {}),
          ...(r.to ? { to: r.to } : {}),
        }));
    }

    if (type === "histogram") {
      config.interval = interval;
    }

    if (type === "date_histogram") {
      config.calendar_interval = calendarInterval;
    }

    onAdd(name, config);

    // Reset form
    setName("");
    setField("");
    setType("terms");
    setSize(10);
    setRanges([{ name: "", from: "", to: "" }]);
  };

  const addRange = () => {
    setRanges([...ranges, { name: "", from: "", to: "" }]);
  };

  const updateRange = (index: number, key: keyof RangeEntry, value: string) => {
    const updated = [...ranges];
    updated[index] = { ...updated[index], [key]: value };
    setRanges(updated);
  };

  const removeRange = (index: number) => {
    setRanges(ranges.filter((_, i) => i !== index));
  };

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-2">
        <div>
          <Label className="text-xs mb-1 block">Name</Label>
          <Input
            placeholder="e.g., by_category"
            value={name}
            onChange={(e) => setName(e.target.value)}
            className="h-8"
          />
        </div>
        <div>
          <Label className="text-xs mb-1 block">Type</Label>
          <Select value={type} onValueChange={(v) => setType(v as AggregationType)}>
            <SelectTrigger className="h-8">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectLabel>Bucketing</SelectLabel>
                {BUCKETING_TYPES.map((t) => (
                  <SelectItem key={t.value} value={t.value}>
                    {t.label}
                  </SelectItem>
                ))}
              </SelectGroup>
              <SelectGroup>
                <SelectLabel>Metrics</SelectLabel>
                {METRIC_TYPES.map((t) => (
                  <SelectItem key={t.value} value={t.value}>
                    {t.label}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
      </div>

      <div>
        <Label className="text-xs mb-1 block">Field</Label>
        <Input
          placeholder="e.g., category"
          value={field}
          onChange={(e) => setField(e.target.value)}
          className="h-8"
        />
        <FieldSelector
          availableFields={isBucketing ? availableBasicFields : availableFields}
          onFieldSelect={setField}
        />
      </div>

      {/* Size control for bucketing types (except histogram/date_histogram) */}
      {isBucketing &&
        type !== "histogram" &&
        type !== "date_histogram" &&
        type !== "range" &&
        type !== "date_range" && (
          <div>
            <Label className="text-xs mb-1 block">Size</Label>
            <Input
              type="number"
              value={size}
              onChange={(e) => setSize(parseInt(e.target.value, 10) || 10)}
              className="h-8 w-24"
              min={1}
            />
          </div>
        )}

      {/* Range config */}
      {(type === "range" || type === "date_range") && (
        <div className="space-y-2">
          <Label className="text-xs">Ranges</Label>
          {ranges.map((range, i) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: ranges are reorderable form inputs identified by position
            <div key={i} className="flex gap-2 items-center">
              <Input
                placeholder="Name"
                value={range.name}
                onChange={(e) => updateRange(i, "name", e.target.value)}
                className="h-7 text-xs flex-1"
              />
              <Input
                placeholder={type === "date_range" ? "From (date)" : "From"}
                type={type === "range" ? "number" : "text"}
                value={range.from || ""}
                onChange={(e) => updateRange(i, "from", e.target.value)}
                className="h-7 text-xs flex-1"
              />
              <Input
                placeholder={type === "date_range" ? "To (date)" : "To"}
                type={type === "range" ? "number" : "text"}
                value={range.to || ""}
                onChange={(e) => updateRange(i, "to", e.target.value)}
                className="h-7 text-xs flex-1"
              />
              {ranges.length > 1 && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => removeRange(i)}
                  className="h-7 w-7 p-0 shrink-0"
                >
                  ×
                </Button>
              )}
            </div>
          ))}
          <Button onClick={addRange} variant="outline" size="sm" className="h-7 text-xs">
            <PlusIcon className="h-3 w-3 mr-1" /> Add Range
          </Button>
        </div>
      )}

      {/* Histogram interval */}
      {type === "histogram" && (
        <div>
          <Label className="text-xs mb-1 block">Interval</Label>
          <Input
            type="number"
            value={interval}
            onChange={(e) => setInterval(parseFloat(e.target.value) || 10)}
            className="h-8 w-32"
            min={1}
          />
        </div>
      )}

      {/* Date histogram calendar interval */}
      {type === "date_histogram" && (
        <div>
          <Label className="text-xs mb-1 block">Calendar Interval</Label>
          <Select value={calendarInterval} onValueChange={setCalendarInterval}>
            <SelectTrigger className="h-8 w-40">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {CALENDAR_INTERVALS.map((ci) => (
                <SelectItem key={ci} value={ci}>
                  {ci.charAt(0).toUpperCase() + ci.slice(1)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      <Button
        onClick={handleAdd}
        className="w-full h-8"
        size="sm"
        disabled={!name.trim() || !field.trim()}
      >
        <PlusIcon className="h-3 w-3 mr-1" />
        Add Aggregation
      </Button>
    </div>
  );
};

export default AggregationBuilder;
