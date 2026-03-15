import { Cross2Icon } from "@radix-ui/react-icons";
import type React from "react";
import { Button } from "@/components/ui/button";
import IndexForm from "../IndexForm";

interface IndexFieldProps {
  index: number;
  onRemove: () => void;
  schemaFields: string[];
}

const IndexField: React.FC<IndexFieldProps> = ({ index, onRemove, schemaFields }) => {
  return (
    <div className="flex flex-col gap-4 mb-4 p-4 border border-gray-200 rounded-md">
      <div className="flex justify-between items-center">
        <h4 className="text-lg font-semibold">Index {index + 1}</h4>
        <Button onClick={onRemove} aria-label="delete index" variant="ghost" size="icon">
          <Cross2Icon />
        </Button>
      </div>
      <IndexForm fieldPrefix={`indexes.${index}`} schemaFields={schemaFields} />
    </div>
  );
};

export default IndexField;
