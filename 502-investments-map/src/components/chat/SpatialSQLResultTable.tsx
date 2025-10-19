"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { downloadGeoJSON } from "@/lib/downloadUtils";
import type { SpatialSQLResult } from "@/lib/parseSQLResults";
import type { Feature, Geometry } from "geojson";

interface SpatialSQLResultTableProps {
  result: SpatialSQLResult;
  onFlyTo?: (geometry: Geometry) => void;
}

export const SpatialSQLResultTable = ({
  result,
  onFlyTo,
}: SpatialSQLResultTableProps) => {
  if (!result.geojson.features || result.geojson.features.length === 0) {
    return <div className="text-xs text-gray-500 italic">No results found</div>;
  }

  // Extract columns from first feature's properties
  const firstFeature = result.geojson.features[0];
  if (!firstFeature.properties) {
    return (
      <div className="text-xs text-gray-500 italic">No properties found</div>
    );
  }
  const columns = Object.keys(firstFeature.properties);
  const rowCount = result.rowCount;

  // Collapse by default if more than 10 rows
  const shouldCollapseByDefault = rowCount > 10;
  const [isOpen, setIsOpen] = useState(!shouldCollapseByDefault);

  const handleExportGeoJSON = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, "-")
      .slice(0, -5);
    downloadGeoJSON(
      result.geojson,
      `spatial_query_results_${timestamp}.geojson`
    );
  };

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleRowClick = (feature: Feature) => {
    if (onFlyTo && feature && feature.geometry && feature.geometry.type) {
      onFlyTo(feature.geometry);
    }
  };

  const tableContent = (
    <div className="my-2 max-w-full overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            {columns.map((col) => (
              <TableHead key={col} className="text-xs font-semibold">
                {col}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {result.geojson.features.map((feature, idx) => (
            <TableRow
              key={idx}
              className="cursor-pointer hover:bg-blue-50 transition-colors"
              onClick={() => handleRowClick(feature)}
              title="Click to fly to this location on the map"
            >
              {columns.map((col) => (
                <TableCell key={col} className="text-xs">
                  {feature.properties &&
                  feature.properties[col] !== null &&
                  feature.properties[col] !== undefined
                    ? String(feature.properties[col])
                    : "—"}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );

  return (
    <div className="my-2">
      {/* Header with row count and export button */}
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs text-gray-700 font-medium">
          📍 {rowCount} {rowCount === 1 ? "location" : "locations"}
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={handleExportGeoJSON}
          className="h-7 px-3 text-xs shrink-0 hover:bg-primary hover:text-primary-foreground transition-colors"
          title="Export to GeoJSON"
        >
          <Download className="h-3 w-3 mr-1.5" />
          GeoJSON
        </Button>
      </div>

      {/* Collapsible table */}
      <details
        open={isOpen}
        className="cursor-pointer [&_summary]:list-none [&_summary::-webkit-details-marker]:hidden"
      >
        <summary
          onClick={(e) => {
            e.preventDefault();
            handleToggle();
          }}
          className="text-xs text-gray-600 hover:text-gray-900 mb-1 select-none flex items-center"
        >
          <span
            className={`inline-block transition-transform ${
              isOpen ? "rotate-90" : ""
            }`}
          >
            ▶
          </span>
          <span className="ml-1">
            {isOpen ? "Click to collapse table" : "Click to expand table"}
          </span>
        </summary>
        {tableContent}
      </details>
      <div className="text-xs text-gray-500 italic mt-1">
        💡 Click any row to fly to that location on the map
      </div>
    </div>
  );
};
