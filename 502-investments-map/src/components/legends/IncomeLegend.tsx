"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { DownloadIcon } from "lucide-react";
import { useMapContext } from "@/contexts/MapContext";
import { MapConfig } from "@/config/mapConfig";
import { downloadGeoJSONFromURL } from "@/lib/downloadUtils";

const IncomeLegend = () => {
  const { selectedHouseholdSize, setSelectedHouseholdSize } = useMapContext();

  const breakpoints = MapConfig.incomeLimitBreakpoints;
  const colorScale = MapConfig.colorScale;

  const steps = breakpoints.map((value, index) => ({
    value,
    color: colorScale(index / (breakpoints.length - 1)),
  }));

  const handleDownload = async () => {
    try {
      await downloadGeoJSONFromURL("/income.geojson", "income-limits.geojson");
    } catch (error) {
      console.error("Error downloading file:", error);
    }
  };

  return (
    <>
      {/* Household size selector */}
      <div className="flex flex-col gap-1">
        <span className="text-xs text-muted-foreground">Household Size</span>
        <Select
          value={String(selectedHouseholdSize)}
          onValueChange={(value) => setSelectedHouseholdSize(Number(value))}
        >
          <SelectTrigger className="h-7 w-full text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {[1, 2, 3, 4, 5, 6, 7, 8].map((size) => (
              <SelectItem key={size} value={String(size)} className="text-xs">
                {size} {size === 1 ? "person" : "people"}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Color scale */}
      <div className="flex flex-col gap-2">
        <p className="text-xs font-medium text-muted-foreground">
          Eligible income for {selectedHouseholdSize}{" "}
          {selectedHouseholdSize === 1 ? "person" : "people"}
        </p>
        <div className="flex flex-col gap-1.5">
          {steps.map((step, i) => (
            <div key={i} className="flex items-center gap-2">
              <div
                className="h-4 w-4 flex-shrink-0 rounded-sm border border-gray-200"
                style={{ backgroundColor: step.color }}
              />
              <span className="text-sm text-gray-700">
                ${step.value.toLocaleString()}
                {i === steps.length - 1 && "+"}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Footer info */}
      <div className="border-t pt-3 space-y-2">
        <p className="text-xs text-muted-foreground">
          Data: USDA Income Limits
        </p>
        <Button
          variant="outline"
          size="sm"
          onClick={handleDownload}
          className="w-full"
        >
          <DownloadIcon className="mr-2 h-4 w-4" />
          Download GeoJSON
        </Button>
      </div>
    </>
  );
};

export default IncomeLegend;
