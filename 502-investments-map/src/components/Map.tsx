"use client";

import { useEffect, useState, useCallback } from "react";
import { Map as MapContainer, useMap } from "react-map-gl/maplibre";
import bbox from "@turf/bbox";
import { LngLatBoundsLike } from "maplibre-gl";
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PanelRightOpenIcon, MessageSquareIcon } from "lucide-react";
import {
  MapProvider,
  useMapContext,
  type LayerType,
} from "@/contexts/MapContext";
import { MapConfig } from "@/config/mapConfig";
import { useMediaQuery } from "@/hooks/use-media-query";
import LegendNavigator from "./legends/LegendNavigator";
import InvestmentLayer from "./layers/InvestmentLayer";
import IncomeLayer from "./layers/IncomeLayer";
import FeatureInfoModal from "./FeatureInfoModal";
import WelcomeModal from "./WelcomeModal";
import type { Geometry } from "geojson";

interface MapContentProps {
  isChatVisible: boolean;
  onToggleChat: () => void;
  onFlyToReady: (flyToFn: (geometry: Geometry) => void) => void;
}

// Create color stops using the configured color scale and breakpoints
const createColorStops = (breakpoints: number[]) => {
  return breakpoints.flatMap((value, index) => [
    value,
    MapConfig.colorScale(index / (breakpoints.length - 1)),
  ]);
};

// Component to handle hover interactions
const HoverHandler = () => {
  const { current: map } = useMap();
  const { selectedLayer, selectedHouseholdSize } = useMapContext();
  const isDesktop = useMediaQuery("(min-width: 768px)");
  const [hoverInfo, setHoverInfo] = useState<{
    county: string;
    state: string;
    value: number;
    label: string;
    x: number;
    y: number;
  } | null>(null);
  const [hoveredFeatureId, setHoveredFeatureId] = useState<
    string | number | null
  >(null);

  useEffect(() => {
    if (!map) return;

    const handleMouseMove = (e: any) => {
      const features = map.queryRenderedFeatures(e.point, {
        layers: ["pmtiles-layer"],
      });

      if (features && features.length > 0) {
        const feature = features[0];
        const props = feature.properties;

        if (props) {
          let value: number;
          let label: string;

          if (selectedLayer === "investment") {
            value = Math.round(
              props[MapConfig.investmentColumn] /
                props[MapConfig.populationColumn]
            );
            label = "Investment per capita";
          } else {
            value = Math.round(
              props[`income_limit_${selectedHouseholdSize}_person`] || 0
            );
            label = `Income limit (${selectedHouseholdSize} ${
              selectedHouseholdSize === 1 ? "person" : "people"
            })`;
          }

          setHoverInfo({
            county: props.county || "Unknown",
            state: props.state || "Unknown",
            value,
            label,
            x: e.point.x,
            y: e.point.y,
          });

          // Update hover state for opacity change
          if (hoveredFeatureId !== null && hoveredFeatureId !== feature.id) {
            map.setFeatureState(
              {
                source: "pmtiles-source",
                sourceLayer: "fgb",
                id: hoveredFeatureId,
              },
              { hover: false }
            );
          }

          if (feature.id !== undefined) {
            map.setFeatureState(
              {
                source: "pmtiles-source",
                sourceLayer: "fgb",
                id: feature.id,
              },
              { hover: true }
            );
            setHoveredFeatureId(feature.id);
          }

          // Change cursor to pointer
          map.getCanvas().style.cursor = "pointer";
        }
      } else {
        // Clear hover state when not hovering over any feature
        if (hoveredFeatureId !== null) {
          map.setFeatureState(
            {
              source: "pmtiles-source",
              sourceLayer: "fgb",
              id: hoveredFeatureId,
            },
            { hover: false }
          );
          setHoveredFeatureId(null);
        }
        setHoverInfo(null);
        map.getCanvas().style.cursor = "";
      }
    };

    const handleMouseLeave = () => {
      if (hoveredFeatureId !== null) {
        map.setFeatureState(
          {
            source: "pmtiles-source",
            sourceLayer: "fgb",
            id: hoveredFeatureId,
          },
          { hover: false }
        );
        setHoveredFeatureId(null);
      }
      setHoverInfo(null);
      map.getCanvas().style.cursor = "";
    };

    map.on("mousemove", handleMouseMove);
    map.on("mouseleave", "pmtiles-layer", handleMouseLeave);

    return () => {
      map.off("mousemove", handleMouseMove);
      map.off("mouseleave", "pmtiles-layer", handleMouseLeave);
      // Clean up hover state on unmount
      if (hoveredFeatureId !== null) {
        map.setFeatureState(
          {
            source: "pmtiles-source",
            sourceLayer: "fgb",
            id: hoveredFeatureId,
          },
          { hover: false }
        );
      }
    };
  }, [map, hoveredFeatureId, selectedLayer, selectedHouseholdSize]);

  // Only show hover tooltip on desktop
  return hoverInfo && isDesktop ? (
    <Card
      className="pointer-events-none absolute z-10 shadow-lg"
      style={{
        left: hoverInfo.x + 10,
        top: hoverInfo.y + 10,
      }}
    >
      <CardContent className="p-3">
        <div className="space-y-1">
          <p className="font-semibold text-sm">
            {hoverInfo.county}, {hoverInfo.state}
          </p>
          <p className="text-sm text-muted-foreground">
            {hoverInfo.label}:{" "}
            <span className="font-medium text-foreground">
              ${hoverInfo.value.toLocaleString()}
            </span>
          </p>
        </div>
      </CardContent>
    </Card>
  ) : null;
};

// Component to handle fly-to functionality from chat
const FlyToHandler = ({
  onReady,
}: {
  onReady: (flyToFn: (geometry: Geometry) => void) => void;
}) => {
  const { current: map } = useMap();

  useEffect(() => {
    if (map) {
      const handleFlyToGeometry = (geometry: Geometry) => {
        try {
          // Validate geometry exists and has required properties
          if (!geometry || !geometry.type) {
            return;
          }

          // Calculate bounding box for the geometry
          const bounds = bbox(geometry as any) as LngLatBoundsLike;

          // Validate bounds before flying
          if (!bounds || (Array.isArray(bounds) && bounds.length !== 4)) {
            return;
          }

          // Fly to the bounds with padding
          map.fitBounds(bounds, {
            padding: 50,
            duration: 1000,
          });
        } catch (error) {
          console.error("Error flying to geometry:", error);
        }
      };

      onReady(handleFlyToGeometry);
    }
  }, [map, onReady]);

  return null;
};

const MapContent = ({
  isChatVisible,
  onToggleChat,
  onFlyToReady,
}: MapContentProps) => {
  const { selectedLayer, isLegendVisible, setIsLegendVisible } =
    useMapContext();

  // Load the pmtiles protocol once
  useEffect(() => {
    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);

    return () => {
      maplibregl.removeProtocol("pmtiles");
    };
  }, []);

  // Create color stops based on selected layer
  const investmentColorStops = createColorStops(
    MapConfig.investmentPerCapitaBreakpoints
  );
  const incomeColorStops = createColorStops(MapConfig.incomeLimitBreakpoints);

  const handleFlyToReady = useCallback(
    (flyToFn: (geometry: Geometry) => void) => {
      onFlyToReady(flyToFn);
    },
    [onFlyToReady]
  );

  return (
    <MapContainer
      style={{ width: "100%", height: "100%" }}
      initialViewState={{
        longitude: MapConfig.centerLng,
        latitude: MapConfig.centerLat,
        zoom: MapConfig.zoom,
      }}
      minZoom={MapConfig.minZoom}
      maxZoom={MapConfig.maxZoom}
      mapStyle={MapConfig.mapStyle}
    >
      {/* Render appropriate layer based on selection */}
      {selectedLayer === "investment" ? (
        <InvestmentLayer colorStops={investmentColorStops} />
      ) : (
        <IncomeLayer colorStops={incomeColorStops} />
      )}

      <HoverHandler />
      <FeatureInfoModal />

      {/* Legend Navigator - conditionally rendered */}
      {isLegendVisible && <LegendNavigator />}

      {/* Show Legend Button - only visible when legend is hidden */}
      {!isLegendVisible && (
        <div className="absolute top-4 right-4 z-10">
          <Button
            variant="secondary"
            size="icon"
            onClick={() => setIsLegendVisible(true)}
            className="bg-white/90 shadow-lg hover:bg-white"
            title="Show Legend"
          >
            <PanelRightOpenIcon className="h-5 w-5" />
          </Button>
        </div>
      )}

      {/* Chat Toggle Button - bottom right */}
      <div className="absolute bottom-12 right-6 z-10">
        <Button
          onClick={onToggleChat}
          size="icon"
          className="h-14 w-14 rounded-full bg-white/80 hover:bg-white shadow-lg"
          title={isChatVisible ? "Close Chat" : "Open Chat"}
        >
          <MessageSquareIcon className="h-6 w-6 text-black" />
        </Button>
      </div>

      {/* Fly To Handler - provides fly-to function */}
      <FlyToHandler onReady={handleFlyToReady} />

      {/* Welcome Modal */}
      <WelcomeModal />
    </MapContainer>
  );
};

// Wrapper component that provides the MapContext
const Map = (props: MapContentProps) => {
  return (
    <MapProvider>
      <MapContent {...props} />
    </MapProvider>
  );
};

export default Map;
