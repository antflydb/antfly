import type { bleve_components } from "@antfly/sdk";
import { Cross2Icon } from "@radix-ui/react-icons";
import type React from "react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { SearchableField } from "../../utils/fieldUtils";
import FieldSelector from "./FieldSelector";

type Query = bleve_components["schemas"]["Query"];

const getQueryType = (query: Query): string => {
  if ("match_phrase" in query) return "Match Phrase";
  if ("prefix" in query) return "Prefix";
  if ("wildcard" in query) return "Wildcard";
  if ("regexp" in query) return "Regexp";
  if ("query" in query) return "Query String";
  if (("min" in query || "max" in query) && !("disjuncts" in query)) {
    const q = query as { min?: unknown; max?: unknown };
    if (typeof q.min !== "string" && typeof q.max !== "string") {
      return "Numeric Range";
    }
  }
  if ("term" in query && ("fuzziness" in query || "prefix_length" in query)) return "Fuzzy";
  if ("term" in query) return "Term";
  if ("match" in query) return "Match";
  if ("must" in query || "should" in query || "must_not" in query) return "Boolean";
  if ("match_all" in query) return "Match All";
  if ("match_none" in query) return "Match None";
  if ("conjuncts" in query) return "Conjunction";
  if ("disjuncts" in query) return "Disjunction";
  // Safety-net for simplified DSL returned by AI query builder
  if ("and" in query) return "Conjunction";
  if ("or" in query) return "Disjunction";
  return "Match All"; // Fallback
};

const defaultQueries: Record<string, () => Query> = {
  "Match All": () => ({ match_all: {} }),
  "Match None": () => ({ match_none: {} }),
  Term: () => ({ term: "" }),
  Match: () => ({ match: "" }),
  "Match Phrase": () => ({ match_phrase: "" }),
  Prefix: () => ({ prefix: "" }),
  Wildcard: () => ({ wildcard: "" }),
  Regexp: () => ({ regexp: "" }),
  Fuzzy: () => ({ term: "", fuzziness: 1 }),
  "Query String": () => ({ query: "" }),
  "Numeric Range": () => ({ min: null, max: null }),
  Boolean: () => ({ must: { conjuncts: [] } }),
  Conjunction: () => ({ conjuncts: [] }),
  Disjunction: () => ({ disjuncts: [] }),
};

interface QueryNodeProps {
  query: Query;
  onChange: (query: Query) => void;
  onDelete?: () => void;
  availableFields?: SearchableField[];
}

const QueryNode: React.FC<QueryNodeProps> = ({
  query,
  onChange,
  onDelete,
  availableFields = [],
}) => {
  const queryType = getQueryType(query);
  const isComplex = ["Boolean", "Conjunction", "Disjunction"].includes(queryType);

  const handleTypeChange = (newType: string) => {
    onChange(defaultQueries[newType]());
  };

  const handleFieldChange = (field: string, value: string | number | boolean | null) => {
    onChange({ ...query, [field]: value });
  };

  // Helper function to render field selection with input + badges
  const renderFieldSelection = (
    currentField: string | undefined,
    onFieldSelect: (field: string) => void
  ) => (
    <div className="flex flex-col gap-2 mt-2">
      <div>
        <Label className="text-xs mb-1 block">Field</Label>
        <Input
          placeholder="Type field name..."
          value={currentField || ""}
          onChange={(e) => onFieldSelect(e.target.value)}
          className="h-8"
        />
      </div>
      <FieldSelector availableFields={availableFields} onFieldSelect={onFieldSelect} />
    </div>
  );

  const renderQueryFields = () => {
    switch (queryType) {
      case "Term": {
        const q = query as bleve_components["schemas"]["TermQuery"];
        return (
          <div className="flex flex-col gap-2">
            <Input
              placeholder="Term"
              value={q.term}
              onChange={(e) => handleFieldChange("term", e.target.value)}
              className="h-8"
            />
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Match": {
        const q = query as bleve_components["schemas"]["MatchQuery"];
        return (
          <div className="flex flex-col gap-2">
            <Input
              placeholder="Match"
              value={q.match}
              onChange={(e) => handleFieldChange("match", e.target.value)}
              className="h-8"
            />
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Match Phrase": {
        const q = query as bleve_components["schemas"]["MatchPhraseQuery"];
        return (
          <div className="flex flex-col gap-2">
            <Input
              placeholder="Match Phrase"
              value={q.match_phrase}
              onChange={(e) => handleFieldChange("match_phrase", e.target.value)}
              className="h-8"
            />
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Prefix": {
        const q = query as bleve_components["schemas"]["PrefixQuery"];
        return (
          <div className="flex flex-col gap-2">
            <Input
              placeholder="Prefix"
              value={q.prefix}
              onChange={(e) => handleFieldChange("prefix", e.target.value)}
              className="h-8"
            />
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Wildcard": {
        const q = query as bleve_components["schemas"]["WildcardQuery"];
        return (
          <div className="flex flex-col gap-2">
            <Input
              placeholder="Wildcard"
              value={q.wildcard}
              onChange={(e) => handleFieldChange("wildcard", e.target.value)}
              className="h-8"
            />
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Regexp": {
        const q = query as bleve_components["schemas"]["RegexpQuery"];
        return (
          <div className="flex flex-col gap-2">
            <Input
              placeholder="Regexp"
              value={q.regexp}
              onChange={(e) => handleFieldChange("regexp", e.target.value)}
              className="h-8"
            />
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Fuzzy": {
        const q = query as bleve_components["schemas"]["FuzzyQuery"];
        return (
          <div className="flex flex-col gap-2">
            <div className="flex gap-2">
              <Input
                placeholder="Term"
                value={q.term}
                onChange={(e) => handleFieldChange("term", e.target.value)}
                className="h-8"
              />
              <Input
                placeholder="Fuzziness"
                type="number"
                value={q.fuzziness || ""}
                onChange={(e) =>
                  handleFieldChange(
                    "fuzziness",
                    e.target.value ? parseInt(e.target.value, 10) : null
                  )
                }
                className="w-20 h-8"
              />
            </div>
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Query String": {
        const q = query as bleve_components["schemas"]["QueryStringQuery"];
        return (
          <Input
            placeholder="Query"
            value={q.query}
            onChange={(e) => handleFieldChange("query", e.target.value)}
            className="h-8"
          />
        );
      }
      case "Numeric Range": {
        const q = query as bleve_components["schemas"]["NumericRangeQuery"];
        return (
          <div className="flex flex-col gap-2">
            <div className="flex gap-2">
              <Input
                placeholder="Min"
                type="number"
                value={q.min ?? ""}
                onChange={(e) =>
                  handleFieldChange(
                    "min",
                    e.target.value === "" ? null : parseFloat(e.target.value)
                  )
                }
                className="h-8"
              />
              <Input
                placeholder="Max"
                type="number"
                value={q.max ?? ""}
                onChange={(e) =>
                  handleFieldChange(
                    "max",
                    e.target.value === "" ? null : parseFloat(e.target.value)
                  )
                }
                className="h-8"
              />
              <div className="flex items-center gap-2">
                <label htmlFor="inclusive-min" className="flex items-center gap-2">
                  <Checkbox
                    id="inclusive-min"
                    checked={q.inclusive_min ?? true}
                    onCheckedChange={(checked) => handleFieldChange("inclusive_min", checked)}
                  />
                  <span className="text-sm">Inc Min</span>
                </label>
              </div>
              <div className="flex items-center gap-2">
                <label htmlFor="inclusive-max" className="flex items-center gap-2">
                  <Checkbox
                    id="inclusive-max"
                    checked={q.inclusive_max ?? true}
                    onCheckedChange={(checked) => handleFieldChange("inclusive_max", checked)}
                  />
                  <span className="text-sm">Inc Max</span>
                </label>
              </div>
            </div>
            {renderFieldSelection(q.field, (field) => handleFieldChange("field", field))}
          </div>
        );
      }
      case "Boolean": {
        const q = query as bleve_components["schemas"]["BooleanQuery"];
        return (
          <div className="mt-2 flex flex-col gap-2">
            <div className="bg-muted/50 rounded-lg p-2 space-y-2">
              <h4 className="font-semibold text-xs text-foreground uppercase tracking-wide">
                Must
              </h4>
              <QueryGroup
                title="Conjuncts"
                queries={q.must?.conjuncts || []}
                onChange={(newQueries) => onChange({ ...q, must: { conjuncts: newQueries } })}
                availableFields={availableFields}
              />
            </div>
            <div className="bg-muted/50 rounded-lg p-2 space-y-2">
              <h4 className="font-semibold text-xs text-foreground uppercase tracking-wide">
                Should
              </h4>
              <QueryGroup
                title="Disjuncts"
                queries={q.should?.disjuncts || []}
                onChange={(newQueries) => onChange({ ...q, should: { disjuncts: newQueries } })}
                availableFields={availableFields}
              />
            </div>
            <div className="bg-muted/50 rounded-lg p-2 space-y-2">
              <h4 className="font-semibold text-xs text-foreground uppercase tracking-wide">
                Must Not
              </h4>
              <QueryGroup
                title="Disjuncts"
                queries={q.must_not?.disjuncts || []}
                onChange={(newQueries) => onChange({ ...q, must_not: { disjuncts: newQueries } })}
                availableFields={availableFields}
              />
            </div>
          </div>
        );
      }
      case "Conjunction": {
        const q = query as bleve_components["schemas"]["ConjunctionQuery"];
        return (
          <div className="mt-2">
            <QueryGroup
              title="Conjuncts"
              queries={q.conjuncts || []}
              onChange={(newQueries) => onChange({ ...q, conjuncts: newQueries })}
              availableFields={availableFields}
            />
          </div>
        );
      }
      case "Disjunction": {
        const q = query as bleve_components["schemas"]["DisjunctionQuery"];
        return (
          <div className="mt-2">
            <QueryGroup
              title="Disjuncts"
              queries={q.disjuncts || []}
              onChange={(newQueries) => onChange({ ...q, disjuncts: newQueries })}
              availableFields={availableFields}
            />
          </div>
        );
      }
      default:
        return null;
    }
  };

  return (
    <div className="p-3 mb-2 border rounded-lg bg-card shadow-sm space-y-2">
      <div className="flex items-start gap-2 flex-wrap">
        <div className="shrink-0 w-44">
          <Label className="text-xs mb-1 block">Query Type</Label>
          <Select value={queryType} onValueChange={handleTypeChange}>
            <SelectTrigger className="h-8">
              <SelectValue placeholder="Query Type" />
            </SelectTrigger>
            <SelectContent>
              {Object.keys(defaultQueries).map((type) => (
                <SelectItem key={type} value={type}>
                  {type}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        {!isComplex && <div className="flex-1 min-w-0">{renderQueryFields()}</div>}
        {onDelete && (
          <Button onClick={onDelete} variant="ghost" size="icon" className="h-8 w-8 mt-5">
            <Cross2Icon className="h-3 w-3" />
          </Button>
        )}
      </div>
      {isComplex && renderQueryFields()}
    </div>
  );
};

interface QueryGroupProps {
  title: string;
  queries: Query[];
  onChange: (queries: Query[]) => void;
  availableFields?: SearchableField[];
}

const QueryGroup: React.FC<QueryGroupProps> = ({
  title,
  queries,
  onChange,
  availableFields = [],
}) => {
  const addQuery = () => {
    onChange([...queries, defaultQueries["Match All"]()]);
  };

  const updateQuery = (index: number, newQuery: Query) => {
    const newQueries = [...queries];
    newQueries[index] = newQuery;
    onChange(newQueries);
  };

  const deleteQuery = (index: number) => {
    const newQueries = queries.filter((_, i) => i !== index);
    onChange(newQueries);
  };

  return (
    <div className="bg-background/50 rounded-lg p-3 space-y-2 border">
      <h4 className="font-semibold text-xs text-muted-foreground uppercase tracking-wide">
        {title}
      </h4>
      <div className="space-y-1.5">
        {queries.map((subQuery, i) => (
          <QueryNode
            // biome-ignore lint/suspicious/noArrayIndexKey: sub-queries don't have stable IDs
            key={i}
            query={subQuery}
            onChange={(newSubQuery) => updateQuery(i, newSubQuery)}
            onDelete={() => deleteQuery(i)}
            availableFields={availableFields}
          />
        ))}
      </div>
      <Button onClick={addQuery} variant="outline" size="sm" className="h-7 w-full text-xs">
        Add Query
      </Button>
    </div>
  );
};

export default QueryNode;
