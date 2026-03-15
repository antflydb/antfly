import { ChevronDownIcon } from "@radix-ui/react-icons";
import type React from "react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";

interface MultiSelectProps {
  options: { label: string; value: string }[];
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
}

const MultiSelect: React.FC<MultiSelectProps> = ({
  options,
  value,
  onChange,
  placeholder = "Select...",
}) => {
  const [isOpen, setIsOpen] = useState(false);

  const handleSelect = (selectedValue: string) => {
    const newValue = value.includes(selectedValue)
      ? value.filter((v) => v !== selectedValue)
      : [...value, selectedValue];
    onChange(newValue);
  };

  const selectedLabels = options
    .filter((option) => value.includes(option.value))
    .map((option) => option.label);

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" className="w-full justify-between">
          <div className="flex flex-wrap items-center gap-2">
            {selectedLabels.length > 0
              ? selectedLabels.map((label) => (
                  <Badge key={label} color="gray">
                    {label}
                  </Badge>
                ))
              : placeholder}
          </div>
          <ChevronDownIcon />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[--radix-popover-trigger-width]">
        <div className="flex flex-col gap-2">
          {options.map((option) => (
            <label
              key={option.value}
              htmlFor={`multiselect-${option.value}`}
              className="flex items-center gap-2"
            >
              <Checkbox
                id={`multiselect-${option.value}`}
                checked={value.includes(option.value)}
                onCheckedChange={() => handleSelect(option.value)}
              />
              {option.label}
            </label>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  );
};

export default MultiSelect;
