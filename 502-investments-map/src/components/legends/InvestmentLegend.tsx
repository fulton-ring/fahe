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

const InvestmentLegend = () => {
  const { selectedYear, setSelectedYear } = useMapContext();

  const breakpoints = MapConfig.investmentPerCapitaBreakpoints;
  const colorScale = MapConfig.colorScale;
  const availableYears = MapConfig.availableYears;

  const steps = breakpoints.map((value, index) => ({
    value,
    color: colorScale(index / (breakpoints.length - 1)),
  }));

  const handleDownload = async () => {
    try {
      await downloadGeoJSONFromURL(
        "/502-investments.geojson",
        "502-investments.geojson"
      );
    } catch (error) {
      console.error("Error downloading file:", error);
    }
  };

  return (
    <>
      {/* Year selector */}
      <div className="flex flex-col gap-1">
        <span className="text-xs text-muted-foreground">Year</span>
        <Select
          value={String(selectedYear)}
          onValueChange={(value) => setSelectedYear(Number(value))}
        >
          <SelectTrigger className="h-7 w-full text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {availableYears.map((year) => (
              <SelectItem key={year} value={String(year)} className="text-xs">
                {year}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Color scale */}
      <div className="flex flex-col gap-2">
        <p className="text-xs font-medium text-muted-foreground">
          Dollar amount per person
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
          Data: FAHE Section 502 Investments
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

export default InvestmentLegend;
