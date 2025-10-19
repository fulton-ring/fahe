"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { XIcon } from "lucide-react";
import { useMapContext, type LayerType } from "@/contexts/MapContext";
import InvestmentLegend from "./InvestmentLegend";
import IncomeLegend from "./IncomeLegend";

const LegendNavigator = () => {
  const { selectedLayer, setSelectedLayer, setIsLegendVisible } =
    useMapContext();

  // Get title based on selected layer
  const title =
    selectedLayer === "investment"
      ? "Investment per Capita"
      : "Income Limits (2025)";

  return (
    <div className="absolute top-4 right-4 z-10 w-64 rounded-lg border bg-white/80 p-4 shadow-lg backdrop-blur-sm">
      <div className="flex flex-col gap-3">
        {/* Header with Title and Close Button */}
        <div className="flex items-start justify-between gap-2">
          <h3 className="font-semibold text-sm">{title}</h3>
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setIsLegendVisible(false)}
            className="h-7 w-7 -mt-1 -mr-1 shrink-0"
          >
            <XIcon className="h-4 w-4" />
          </Button>
        </div>

        {/* Layer selector */}
        <div className="flex flex-col gap-1">
          <span className="text-xs text-muted-foreground">Layer</span>
          <Select
            value={selectedLayer}
            onValueChange={(value) => setSelectedLayer(value as LayerType)}
          >
            <SelectTrigger className="h-7 w-full text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="investment" className="text-xs">
                Investment/Capita
              </SelectItem>
              <SelectItem value="income" className="text-xs">
                Income Limits
              </SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Render the appropriate legend based on selected layer */}
        {selectedLayer === "investment" ? (
          <InvestmentLegend />
        ) : (
          <IncomeLegend />
        )}
      </div>
    </div>
  );
};

export default LegendNavigator;
