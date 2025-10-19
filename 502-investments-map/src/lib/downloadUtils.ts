import type { FeatureCollection } from "geojson";

/**
 * Generic file download helper
 */
export const downloadFile = (blob: Blob, filename: string) => {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.style.visibility = "hidden";

  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);

  URL.revokeObjectURL(url);
};

/**
 * Download a GeoJSON FeatureCollection as a file
 */
export const downloadGeoJSON = (
  geojson: FeatureCollection,
  filename: string = "query_results.geojson"
) => {
  const blob = new Blob([JSON.stringify(geojson, null, 2)], {
    type: "application/geo+json;charset=utf-8;",
  });
  downloadFile(blob, filename);
};

/**
 * Download a GeoJSON file from a URL
 */
export const downloadGeoJSONFromURL = async (url: string, filename: string) => {
  try {
    const response = await fetch(url);
    const blob = await response.blob();
    downloadFile(blob, filename);
  } catch (error) {
    console.error("Error downloading file:", error);
    throw error;
  }
};

/**
 * Convert SQL results to CSV format
 */
export const convertToCSV = (rows: Record<string, unknown>[]): string => {
  if (!rows || rows.length === 0) return "";

  const columns = Object.keys(rows[0]);

  // Escape CSV values
  const escapeCSV = (value: unknown): string => {
    if (value === null || value === undefined) return "";
    const stringValue = String(value);
    // If value contains comma, quote, or newline, wrap in quotes and escape quotes
    if (
      stringValue.includes(",") ||
      stringValue.includes('"') ||
      stringValue.includes("\n")
    ) {
      return `"${stringValue.replace(/"/g, '""')}"`;
    }
    return stringValue;
  };

  // Create header row
  const header = columns.map(escapeCSV).join(",");

  // Create data rows
  const dataRows = rows.map((row) =>
    columns.map((col) => escapeCSV(row[col])).join(",")
  );

  return [header, ...dataRows].join("\n");
};

/**
 * Download SQL results as CSV
 */
export const downloadCSV = (
  csvContent: string,
  filename: string = "query_results.csv"
) => {
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  downloadFile(blob, filename);
};
