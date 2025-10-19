import type { FeatureCollection } from "geojson";

// SQL Query Result (matches Python SQLQueryResult)
export interface SQLQueryResult {
  workspaceId: string;
  rows: Record<string, unknown>[];
  rowCount: number;
  format: "dataframe";
}

// GeoJSON Query Result (matches Python GeoJSONQueryResult)
export interface GeoJSONQueryResult {
  workspaceId: string;
  geojson: FeatureCollection;
  featureCount: number;
  format: "geojson";
}

// Dynamic tool output structure
export interface DynamicToolOutput {
  structuredContent?: {
    result?: SQLQueryResult | GeoJSONQueryResult;
  };
  content?: Array<{
    type: string;
    text?: string;
  }>;
  isError?: boolean;
}

// For component usage
export interface SQLResult {
  rows: Record<string, unknown>[];
  rowCount: number;
}

export interface SpatialSQLResult {
  geojson: FeatureCollection;
  rowCount: number;
}

/**
 * Parse SQL results from dynamic tool output
 */
export const parseSQLResult = (output: DynamicToolOutput): SQLResult | null => {
  // Check if structuredContent is SQLQueryResult
  if (
    output.structuredContent &&
    output.structuredContent.result &&
    output.structuredContent.result.format === "dataframe" &&
    "rows" in output.structuredContent.result &&
    "rowCount" in output.structuredContent.result &&
    Array.isArray(output.structuredContent.result.rows)
  ) {
    const sqlResult = output.structuredContent.result as SQLQueryResult;

    return {
      rows: sqlResult.rows,
      rowCount: sqlResult.rowCount,
    };
  }

  return null;
};

/**
 * Parse Spatial SQL results from dynamic tool output
 */
export const parseSpatialSQLResult = (
  output: DynamicToolOutput
): SpatialSQLResult | null => {
  // Check if structuredContent is GeoJSONQueryResult
  if (
    output.structuredContent &&
    output.structuredContent.result &&
    output.structuredContent.result.format === "geojson" &&
    "geojson" in output.structuredContent.result &&
    "featureCount" in output.structuredContent.result
  ) {
    const geoResult = output.structuredContent.result as GeoJSONQueryResult;
    return {
      geojson: geoResult.geojson,
      rowCount: geoResult.featureCount,
    };
  }

  return null;
};
