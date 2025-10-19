"use client";

import { Source, Layer } from "react-map-gl/maplibre";
import { useMapContext } from "@/contexts/MapContext";
import { MapConfig } from "@/config/mapConfig";

interface InvestmentLayerProps {
  colorStops: (string | number)[];
}

const InvestmentLayer = ({ colorStops }: InvestmentLayerProps) => {
  const { selectedYear } = useMapContext();

  return (
    <Source
      key={`investment-${selectedYear}`}
      id="pmtiles-source"
      type="vector"
      url="pmtiles:///investments.pmtiles"
      promoteId="county_fips"
    >
      <Layer
        id="pmtiles-layer"
        type="fill"
        source-layer="fgb"
        filter={
          [
            "all",
            ["==", ["get", "year"], selectedYear],
            ["has", MapConfig.investmentColumn],
            ["has", MapConfig.populationColumn],
            [">", ["get", MapConfig.populationColumn], 0],
          ] as any
        }
        paint={{
          "fill-color": [
            "interpolate",
            ["linear"],
            [
              "/",
              ["get", MapConfig.investmentColumn],
              ["get", MapConfig.populationColumn],
            ] as any,
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
          [
            "all",
            ["==", ["get", "year"], selectedYear],
            ["has", MapConfig.investmentColumn],
            ["has", MapConfig.populationColumn],
            [">", ["get", MapConfig.populationColumn], 0],
          ] as any
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

export default InvestmentLayer;
