"use client";

import {
  Tool,
  ToolContent,
  ToolHeader,
  ToolInput,
  ToolOutput,
} from "@/components/ai-elements/tool";
import { SQLResultTable } from "./SQLResultTable";
import { SpatialSQLResultTable } from "./SpatialSQLResultTable";
import {
  parseSQLResult,
  parseSpatialSQLResult,
  type DynamicToolOutput,
} from "@/lib/parseSQLResults";
import type { Geometry } from "geojson";

interface ToolPartProps {
  part: any;
  onFlyTo?: (geometry: Geometry) => void;
}

/**
 * Format tool names from snake_case to Title Case
 */
const formatToolName = (toolName: string): string => {
  return toolName
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
};

/**
 * Component to render tool parts using AI Elements Tool component
 */
export const ToolPart = ({ part, onFlyTo }: ToolPartProps) => {
  // Determine the state based on what's present
  let state:
    | "input-streaming"
    | "input-available"
    | "output-available"
    | "output-error";

  if (part.errorText !== undefined) {
    state = "output-error";
  } else if (part.output !== undefined) {
    state = "output-available";
  } else if (part.input !== undefined) {
    state = "input-available";
  } else {
    state = "input-streaming";
  }

  // Determine if we should default open (only for completed or error states)
  const defaultOpen = state === "output-available" || state === "output-error";

  // Format the tool name for display
  const toolTitle = part.toolName ? formatToolName(part.toolName) : "Tool";

  // Try to parse results (spatial takes priority over regular SQL)
  const spatialSQLResult =
    part.output && !part.errorText ? parseSpatialSQLResult(part.output) : null;
  const sqlResult =
    part.output && !part.errorText && !spatialSQLResult
      ? parseSQLResult(part.output)
      : null;

  // Render output based on type
  const renderOutput = () => {
    if (spatialSQLResult) {
      return (
        <SpatialSQLResultTable result={spatialSQLResult} onFlyTo={onFlyTo} />
      );
    } else if (sqlResult) {
      return <SQLResultTable result={sqlResult} />;
    } else if (part.output) {
      // For other tool results, show the structured content
      return (
        <pre className="text-xs overflow-x-auto bg-white p-2 rounded max-h-40">
          {JSON.stringify(
            part.output.structuredContent || part.output,
            null,
            2
          )}
        </pre>
      );
    }
    return null;
  };

  return (
    <Tool defaultOpen={defaultOpen}>
      <ToolHeader
        title={toolTitle}
        type={part.toolName || "tool"}
        state={state}
      />
      <ToolContent>
        {state !== "input-streaming" && part.input && (
          <ToolInput input={part.input} />
        )}
        {(state === "output-available" || state === "output-error") && (
          <ToolOutput output={renderOutput()} errorText={part.errorText} />
        )}
      </ToolContent>
    </Tool>
  );
};
