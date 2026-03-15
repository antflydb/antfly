import type React from "react";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import type { BasicField, SearchableField } from "../../utils/fieldUtils";

interface FieldSelectorProps {
  availableFields?: SearchableField[] | BasicField[];
  onFieldSelect: (fieldName: string) => void;
  label?: string;
  className?: string;
}

/**
 * FieldSelector - Reusable component for selecting fields from schema
 * Used in query builder, order by, facets, etc.
 */
const FieldSelector: React.FC<FieldSelectorProps> = ({
  availableFields = [],
  onFieldSelect,
  label = "Available fields from schema:",
  className,
}) => {
  if (availableFields.length === 0) {
    return null;
  }

  const isSearchableField = (field: SearchableField | BasicField): field is SearchableField => {
    return "searchField" in field;
  };

  return (
    <div className={className}>
      <Label className="text-sm font-medium text-muted-foreground">{label}</Label>
      <div className="flex flex-wrap gap-2 mt-2">
        {availableFields.map((fieldInfo) => {
          const isSearchable = isSearchableField(fieldInfo);
          const fieldName = isSearchable ? fieldInfo.searchField : fieldInfo.fieldName;
          const displayName = fieldInfo.displayName;
          const title = isSearchable
            ? `Search field: ${fieldInfo.searchField}\nSchema types: ${fieldInfo.schemaTypes.join(", ")}\nAntfly types: ${fieldInfo.antflyTypes.join(", ")}`
            : `Field: ${fieldInfo.fieldName}\nSchema type: ${fieldInfo.schemaType}`;

          return (
            <Badge
              key={fieldName}
              variant="outline"
              className="cursor-pointer hover:bg-accent hover:text-accent-foreground transition-colors"
              onClick={() => onFieldSelect(fieldName)}
              title={title}
            >
              + {displayName}
            </Badge>
          );
        })}
      </div>
    </div>
  );
};

export default FieldSelector;
