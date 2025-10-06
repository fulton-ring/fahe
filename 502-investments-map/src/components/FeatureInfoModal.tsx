import { useEffect, useState, useRef } from "react";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableRow } from "@/components/ui/table";
import { useMap, type PointLike } from "react-map-gl/maplibre";
import type { MapMouseEvent } from "maplibre-gl";

interface FeatureProperties {
  [key: string]: string | number | boolean | null;
}

export interface FieldConfig {
  column: string; // The property name in the feature data
  label: string; // The display label
  formatType?: "currency" | "number" | "percent" | "text"; // Predefined formatting types
  format?: (value: any) => string; // Custom formatter function (overrides formatType)
}

interface FeatureInfoModalProps {
  // Optional list of fields to display (in order). If not provided, shows all fields.
  fieldsToDisplay?: FieldConfig[];
}

const FeatureInfoModal = ({ fieldsToDisplay }: FeatureInfoModalProps) => {
  const { current: map } = useMap();
  const [featureProperties, setFeatureProperties] =
    useState<FeatureProperties | null>(null);
  const dragging = useRef(false);

  useEffect(() => {
    if (!map) return;

    const onDragStart = () => {
      dragging.current = true;
    };

    // Wait a tick so the click that finishes the drag can bubble first
    const onDragEnd = () => {
      setTimeout(() => (dragging.current = false), 0);
    };

    const onClick = (e: MapMouseEvent) => {
      if (dragging.current) return;

      const bbox: [PointLike, PointLike] = [
        [e.point.x - 5, e.point.y - 5],
        [e.point.x + 5, e.point.y + 5],
      ];

      const features = map.queryRenderedFeatures(bbox, {
        layers: ["pmtiles-layer"],
      });

      if (features && features.length > 0) {
        const feature = features[0];
        if (feature.properties) {
          setFeatureProperties(feature.properties);
        }
      }
    };

    map.on("dragstart", onDragStart);
    map.on("dragend", onDragEnd);
    map.on("click", onClick);

    return () => {
      map.off("dragstart", onDragStart);
      map.off("dragend", onDragEnd);
      map.off("click", onClick);
    };
  }, [map]);

  return (
    <Dialog
      open={featureProperties !== null}
      onOpenChange={(open) => {
        if (!open) {
          setFeatureProperties(null);
        }
      }}
    >
      <DialogContent className="max-w-[90vw] sm:max-w-2xl">
        <div className="flex flex-col space-y-4">
          <DialogTitle>
            {featureProperties?.county
              ? `${featureProperties.county}, ${featureProperties.state}`
              : "County Information"}
          </DialogTitle>

          {featureProperties && (
            <div className="max-h-[60vh] overflow-auto">
              <Table>
                <TableBody>
                  {getDisplayFields(featureProperties, fieldsToDisplay).map(
                    ({ label, value, column }) => (
                      <TableRow key={column}>
                        <TableCell className="w-1/2 font-medium align-top">
                          <div className="break-words">{label}</div>
                        </TableCell>
                        <TableCell className="w-1/2 text-right align-top">
                          <div className="break-words">{value}</div>
                        </TableCell>
                      </TableRow>
                    )
                  )}
                </TableBody>
              </Table>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
};

// Helper function to get fields to display based on configuration
function getDisplayFields(
  properties: FeatureProperties,
  fieldsToDisplay?: FieldConfig[]
): { label: string; value: string; column: string }[] {
  if (!fieldsToDisplay || fieldsToDisplay.length === 0) {
    // Show all fields, sorted alphabetically with auto-formatting
    return Object.entries(properties)
      .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
      .map(([key, val]) => ({
        column: key,
        label: autoFormatColumnName(key),
        value: autoFormatValue(key, val),
      }));
  }

  // Show only specified fields in the specified order with configured formatting
  return fieldsToDisplay
    .filter((fieldConfig) => fieldConfig.column in properties)
    .map((fieldConfig) => {
      const rawValue = properties[fieldConfig.column];
      let formattedValue: string;

      // Use custom format function if provided
      if (fieldConfig.format) {
        formattedValue = fieldConfig.format(rawValue);
      } else if (fieldConfig.formatType) {
        // Use predefined format type
        formattedValue = formatByType(rawValue, fieldConfig.formatType);
      } else {
        // Fallback to auto-formatting
        formattedValue = autoFormatValue(fieldConfig.column, rawValue);
      }

      return {
        column: fieldConfig.column,
        label: fieldConfig.label,
        value: formattedValue,
      };
    });
}

// Auto-format column names (fallback)
function autoFormatColumnName(key: string): string {
  return key
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

// Auto-format values (fallback)
function autoFormatValue(
  key: string,
  value: string | number | boolean | null
): string {
  if (value === null || value === undefined) {
    return "N/A";
  }

  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }

  if (typeof value === "number") {
    if (
      key.includes("dollar") ||
      key.includes("income") ||
      key.includes("earnings")
    ) {
      return `$${value.toLocaleString()}`;
    }
    if (key.includes("percent")) {
      return `${value.toFixed(1)}%`;
    }
    return value.toLocaleString();
  }

  return String(value);
}

// Format value by predefined type
function formatByType(
  value: string | number | boolean | null,
  formatType: "currency" | "number" | "percent" | "text"
): string {
  if (value === null || value === undefined) {
    return "N/A";
  }

  switch (formatType) {
    case "currency":
      return typeof value === "number"
        ? `$${value.toLocaleString()}`
        : String(value);
    case "number":
      return typeof value === "number" ? value.toLocaleString() : String(value);
    case "percent":
      return typeof value === "number" ? `${value.toFixed(1)}%` : String(value);
    case "text":
    default:
      return String(value);
  }
}

export default FeatureInfoModal;
