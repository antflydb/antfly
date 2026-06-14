import type { ProductId } from "@/config/products";
import rawCommandDefinitions from "@/data/command-definitions.json";
import rawCommandIndex from "@/data/command-index.json";

export type CommandType = "navigation" | "action";
export type CommandGroup = "navigation" | "tools" | "quickActions";
export type CommandAction = "toggle-theme";

export interface CommandDefinition {
  id: string;
  type: CommandType;
  group: CommandGroup;
  label: string;
  description: string;
  href?: string;
  action?: CommandAction;
  icon: string;
  product?: ProductId;
  adminOnly?: boolean;
  semantic: boolean;
}

export type CommandMetadata = Omit<CommandDefinition, "semantic">;

export interface SemanticCommand extends CommandMetadata {
  embedding: number[];
}

export interface CommandIndexData {
  model: string;
  dimension: number;
  generatedFrom: string;
  commands: SemanticCommand[];
}

export const commandDefinitions = rawCommandDefinitions as CommandDefinition[];
export const commandIndex = rawCommandIndex as CommandIndexData;
