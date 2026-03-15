import type { bleve_components, components } from "@antfly/sdk";
import { Cross2Icon, PlusIcon } from "@radix-ui/react-icons";
import type React from "react";
import AggregationBuilder, { type AggregationConfig } from "@/components/AggregationBuilder";
import AggregationCard from "@/components/AggregationCard";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import type { BasicField, SearchableField } from "../../utils/fieldUtils";
import FieldSelector from "./FieldSelector";
import QueryNode from "./QueryNode";

type Query = bleve_components["schemas"]["Query"];
type QueryRequest = components["schemas"]["QueryRequest"];

interface QueryBuilderProps {
  value: string;
  onChange: (value: string) => void;
  showQueryNode?: boolean;
  showOrderByAndFacets?: boolean;
  showLimitAndOffset?: boolean;
  disableOffset?: boolean;
  availableFields?: SearchableField[];
  availableBasicFields?: BasicField[];
}

const QueryBuilder: React.FC<QueryBuilderProps> = ({
  value,
  onChange,
  showQueryNode = true,
  showOrderByAndFacets = true,
  showLimitAndOffset = false,
  disableOffset = false,
  availableFields = [],
  availableBasicFields = [],
}) => {
  let queryRequest: QueryRequest;
  try {
    const parsed = JSON.parse(value);
    if (showQueryNode && !showOrderByAndFacets) {
      // For filter queries, the value is just the query object
      queryRequest = { filter_query: parsed };
    } else {
      // For semantic queries, the value is the full query request
      queryRequest = parsed;
    }
  } catch (e) {
    console.error("Failed to parse query JSON:", e);
    // Provide a default valid query if parsing fails
    if (showQueryNode && !showOrderByAndFacets) {
      queryRequest = { filter_query: { match_all: {} } };
    } else {
      queryRequest = { filter_query: { match_all: {} } };
    }
  }

  const handleQueryChange = (newQuery: Query) => {
    if (showQueryNode && !showOrderByAndFacets) {
      // For filter queries, just return the query itself
      onChange(JSON.stringify(newQuery, null, 2));
    } else {
      // For semantic queries, wrap in query request structure
      const newQueryRequest = { ...queryRequest, filter_query: newQuery };
      onChange(JSON.stringify(newQueryRequest, null, 2));
    }
  };

  const handleLimitChange = (limit: number) => {
    const newQueryRequest = { ...queryRequest, limit };
    onChange(JSON.stringify(newQueryRequest, null, 2));
  };

  const handleOffsetChange = (offset: number) => {
    const newQueryRequest = { ...queryRequest, offset };
    onChange(JSON.stringify(newQueryRequest, null, 2));
  };

  const handleOrderByChange = (index: number, field: string, asc: boolean) => {
    const newOrderBy = [...(queryRequest.order_by || [])];
    newOrderBy[index] = { field, desc: !asc };
    const newQueryRequest = { ...queryRequest, order_by: newOrderBy };
    onChange(JSON.stringify(newQueryRequest, null, 2));
  };

  const addOrderBy = () => {
    const newOrderBy = [...(queryRequest.order_by || []), { field: "", desc: false }];
    const newQueryRequest = { ...queryRequest, order_by: newOrderBy };
    onChange(JSON.stringify(newQueryRequest, null, 2));
  };

  const removeOrderBy = (index: number) => {
    const newOrderBy = [...(queryRequest.order_by || [])];
    newOrderBy.splice(index, 1);
    const newQueryRequest = { ...queryRequest, order_by: newOrderBy };
    onChange(JSON.stringify(newQueryRequest, null, 2));
  };

  const handleAddAggregation = (name: string, config: AggregationConfig) => {
    const newAggregations = {
      ...queryRequest.aggregations,
      [name]: config,
    };
    const newQueryRequest = { ...queryRequest, aggregations: newAggregations };
    onChange(JSON.stringify(newQueryRequest, null, 2));
  };

  const removeFacet = (name: string) => {
    const newAggregations = { ...queryRequest.aggregations };
    delete newAggregations[name];
    const newQueryRequest = { ...queryRequest, aggregations: newAggregations };
    onChange(JSON.stringify(newQueryRequest, null, 2));
  };

  return (
    <div>
      {showQueryNode && (
        <QueryNode
          query={(queryRequest.filter_query || { match_all: {} }) as Query}
          onChange={handleQueryChange}
          availableFields={availableFields}
        />
      )}
      {showLimitAndOffset && (
        <div className="flex gap-2">
          <div className="flex-1">
            <Label className="text-xs mb-1 block">Limit</Label>
            <Input
              placeholder="Limit"
              type="number"
              value={queryRequest.limit}
              onChange={(e) => handleLimitChange(parseInt(e.target.value, 10))}
              className="h-9"
            />
          </div>
          {showOrderByAndFacets && (
            <div className="flex-1">
              <Label className="text-xs mb-1 block">Offset</Label>
              <Input
                placeholder={disableOffset ? "Disabled" : "Offset"}
                type="number"
                value={queryRequest.offset ?? ""}
                onChange={(e) =>
                  handleOffsetChange(e.target.value === "" ? 0 : parseInt(e.target.value, 10))
                }
                disabled={disableOffset}
                className="h-9"
              />
            </div>
          )}
        </div>
      )}
      {showOrderByAndFacets && (
        <Accordion type="multiple" className="space-y-2">
          <AccordionItem value="orderby" className="border rounded-lg bg-card/50 px-3">
            <AccordionTrigger className="py-2.5 hover:no-underline">
              <span className="font-medium text-sm">Order By</span>
            </AccordionTrigger>
            <AccordionContent className="pb-3 pt-1 space-y-2">
              {queryRequest.order_by?.map((sortField, index) => (
                <div
                  key={`orderby-${sortField.field}-${sortField.desc}`}
                  className="flex items-center gap-2 p-2 bg-muted/30 rounded border"
                >
                  <Input
                    placeholder="Field name"
                    value={sortField.field}
                    onChange={(e) => handleOrderByChange(index, e.target.value, !sortField.desc)}
                    className="h-8"
                  />
                  <div className="flex items-center gap-1.5 shrink-0">
                    <span className="text-xs">{sortField.desc ? "Desc" : "Asc"}</span>
                    <Switch
                      checked={!sortField.desc}
                      onCheckedChange={(checked) =>
                        handleOrderByChange(index, sortField.field, checked)
                      }
                    />
                  </div>
                  <Button
                    onClick={() => removeOrderBy(index)}
                    size="icon"
                    variant="ghost"
                    className="h-8 w-8 shrink-0"
                  >
                    <Cross2Icon className="h-3 w-3" />
                  </Button>
                </div>
              ))}
              <Button onClick={addOrderBy} variant="outline" size="sm" className="h-8 w-full">
                <PlusIcon className="h-3 w-3 mr-1" />
                Add Order By
              </Button>
              <FieldSelector
                availableFields={availableBasicFields}
                onFieldSelect={(fieldName) => {
                  const newOrderBy = {
                    ...queryRequest.order_by,
                    [fieldName]: true,
                  };
                  onChange(JSON.stringify({ ...queryRequest, order_by: newOrderBy }, null, 2));
                }}
              />
            </AccordionContent>
          </AccordionItem>

          <AccordionItem value="aggregations" className="border rounded-lg bg-card/50 px-3">
            <AccordionTrigger className="py-2.5 hover:no-underline">
              <div className="flex items-center gap-2">
                <span className="font-medium text-sm">Aggregations</span>
                {queryRequest.aggregations && Object.keys(queryRequest.aggregations).length > 0 && (
                  <Badge variant="secondary" className="h-5 text-xs">
                    {Object.keys(queryRequest.aggregations).length}
                  </Badge>
                )}
              </div>
            </AccordionTrigger>
            <AccordionContent className="pb-3 pt-1 space-y-2">
              {queryRequest.aggregations &&
                Object.entries(queryRequest.aggregations).map(([name, aggregation]) => (
                  <AggregationCard
                    key={name}
                    name={name}
                    aggregation={aggregation}
                    onDelete={() => removeFacet(name)}
                  />
                ))}

              <div className="border-t pt-2">
                <AggregationBuilder
                  availableFields={availableFields}
                  availableBasicFields={availableBasicFields}
                  onAdd={handleAddAggregation}
                />
              </div>
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      )}
    </div>
  );
};

export default QueryBuilder;
