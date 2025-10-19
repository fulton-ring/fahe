"use client";

import { Source, Layer } from "react-map-gl/maplibre";
import { useMapContext } from "@/contexts/MapContext";
import { MapConfig } from "@/config/mapConfig";

interface IncomeLayerProps {
  colorStops: (string | number)[];
}

const IncomeLayer = ({ colorStops }: IncomeLayerProps) => {
  const { selectedHouseholdSize } = useMapContext();

  const incomeColumn = `income_limit_${selectedHouseholdSize}_person`;

  return (
    <Source
      key={`income-${selectedHouseholdSize}`}
      id="pmtiles-source"
      type="vector"
      url="pmtiles:///income.pmtiles"
      promoteId="county_fips"
    >
      <Layer
        id="pmtiles-layer"
        type="fill"
        source-layer="fgb"
        filter={
          ["all", ["has", incomeColumn], [">", ["get", incomeColumn], 0]] as any
        }
        paint={{
          "fill-color": [
            "interpolate",
            ["linear"],
            ["get", incomeColumn] as any,
            ...colorStops,
          ] as any,
          "fill-opacity": [
            "case",
            ["boolean", ["feature-state", "hover"], false],
            MapConfig.fillOpacityHover,
            MapConfig.fillOpacity,
          ] as any,
        }}
      />
      <Layer
        id="pmtiles-outline"
        type="line"
        source-layer="fgb"
        filter={
          ["all", ["has", incomeColumn], [">", ["get", incomeColumn], 0]] as any
        }
        paint={{
          "line-color": MapConfig.lineColor,
          "line-opacity": [
            "case",
            ["boolean", ["feature-state", "hover"], false],
            0.8,
            MapConfig.lineOpacity,
          ] as any,
          "line-width": [
            "case",
            ["boolean", ["feature-state", "hover"], false],
            MapConfig.lineWidthHover,
            MapConfig.lineWidth,
          ] as any,
        }}
      />
    </Source>
  );
};

export default IncomeLayer;
