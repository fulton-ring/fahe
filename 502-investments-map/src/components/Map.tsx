import { useEffect, useState } from "react";
import {
  Map as MapContainer,
  Source,
  Layer,
  useMap,
} from "react-map-gl/maplibre";
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import { interpolateCool } from "d3-scale-chromatic";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PanelRightOpenIcon, MessageSquareIcon } from "lucide-react";
import Legend from "./Legend";
import FeatureInfoModal, { type FieldConfig } from "./FeatureInfoModal";
import ChatSidebar from "./ChatSidebar";
import "maplibre-gl/dist/maplibre-gl.css";

const MapSettings = {
  // Map view settings
  // Centered approximately over the central Appalachians (e.g., West Virginia)
  centerLng: -81.5,
  centerLat: 37.5,
  zoom: 5,
  minZoom: 3,
  maxZoom: 20,

  // Basemap style
  mapStyle: "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",

  // Data visualization settings
  // Color scale function from d3-scale-chromatic
  // Options: interpolateYlGnBu, interpolateViridis, interpolatePlasma,
  //          interpolateWarm, interpolateCool, interpolateBlues, etc.
  colorScale: interpolateCool,

  // Investment per capita breakpoints (in dollars per person)
  // Values will interpolate smoothly between these stops
  investmentPerCapitaBreakpoints: [0, 32, 61, 77, 83, 108],

  // Layer opacity settings (0 = transparent, 1 = opaque)
  fillOpacity: 0.5,
  fillOpacityHover: 0.4, // Opacity when hovering over a county
  lineOpacity: 0.8,

  // Border styling
  lineColor: "#ffffff",
  lineWidth: 0.5,
  lineWidthHover: 2, // Line width when hovering over a county

  // Data column names (if your pmtiles has different column names, update here)
  investmentColumn: "total_investment_dollars",
  populationColumn: "total_population",

  // Temporal filter - set which year to display
  selectedYear: 2023,
  availableYears: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],

  // Fields to display in the feature info modal (in order)
  // Each field specifies: column name, display label, and optional formatting
  fieldsToDisplay: [
    {
      column: "year",
      label: "Year",
      format: (value) => String(value),
    },
    {
      column: "total_investment_dollars",
      label: "Total Investment",
      formatType: "currency",
    },
    {
      column: "total_number_of_investments",
      label: "Number of Investments",
      formatType: "number",
    },
    {
      column: "total_population",
      label: "Total Population",
      formatType: "number",
    },
    {
      column: "average_income_per_household",
      label: "Average Household Income",
      formatType: "currency",
    },
    {
      column: "total_median_earnings",
      label: "Median Earnings",
      formatType: "currency",
    },
    {
      column: "population_below_poverty",
      label: "Population Below Poverty",
      formatType: "number",
    },
    {
      column: "population_below_poverty_percent",
      label: "Poverty Rate",
      formatType: "percent",
    },
    {
      column: "number_of_households",
      label: "Number of Households",
      formatType: "number",
    },
    {
      column: "adults_25_and_older_less_than_high_school_graduate",
      label: "Adults Less Than High School",
      formatType: "number",
    },
    {
      column: "adults_25_and_older_less_than_high_school_graduate_percent",
      label: "Less Than High School Rate",
      formatType: "percent",
    },
    {
      column: "adults_25_and_older_with_bachelor's_degree_or_higher",
      label: "Adults with Bachelor's Degree+",
      formatType: "number",
    },
    {
      column: "adults_25_and_older_with_bachelor's_degree_or_higher_percent",
      label: "Bachelor's Degree+ Rate",
      formatType: "percent",
    },
  ] as FieldConfig[],
};

// Create color stops using the configured color scale and breakpoints
const createColorStops = () => {
  return MapSettings.investmentPerCapitaBreakpoints.flatMap((value, index) => [
    value,
    MapSettings.colorScale(
      index / (MapSettings.investmentPerCapitaBreakpoints.length - 1)
    ),
  ]);
};

// Component to handle hover interactions
const HoverHandler = () => {
  const { current: map } = useMap();
  const [hoverInfo, setHoverInfo] = useState<{
    county: string;
    state: string;
    investmentPerCapita: number;
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
          const investmentPerCapita =
            props[MapSettings.investmentColumn] /
            props[MapSettings.populationColumn];

          setHoverInfo({
            county: props.county || "Unknown",
            state: props.state || "Unknown",
            investmentPerCapita: Math.round(investmentPerCapita),
            x: e.point.x,
            y: e.point.y,
          });

          // Update hover state for opacity change
          if (hoveredFeatureId !== null && hoveredFeatureId !== feature.id) {
            map.setFeatureState(
              {
                source: "pmtiles-source",
                sourceLayer: "final_output",
                id: hoveredFeatureId,
              },
              { hover: false }
            );
          }

          if (feature.id !== undefined) {
            map.setFeatureState(
              {
                source: "pmtiles-source",
                sourceLayer: "final_output",
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
              sourceLayer: "final_output",
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
            sourceLayer: "final_output",
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
            sourceLayer: "final_output",
            id: hoveredFeatureId,
          },
          { hover: false }
        );
      }
    };
  }, [map, hoveredFeatureId]);

  return hoverInfo ? (
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
            Investment per capita:{" "}
            <span className="font-medium text-foreground">
              ${hoverInfo.investmentPerCapita.toLocaleString()}
            </span>
          </p>
        </div>
      </CardContent>
    </Card>
  ) : null;
};

const Map = () => {
  // State for selected year and legend visibility
  const [selectedYear, setSelectedYear] = useState(MapSettings.selectedYear);
  const [isLegendVisible, setIsLegendVisible] = useState(true);

  // State for chat
  const [isChatVisible, setIsChatVisible] = useState(false);

  // Load the pmtiles protocol once
  useEffect(() => {
    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);

    return () => {
      maplibregl.removeProtocol("pmtiles");
    };
  }, []);

  const colorStops = createColorStops();

  const toggleChat = () => {
    setIsChatVisible(!isChatVisible);
  };

  return (
    <MapContainer
      style={{ width: "100%", height: "100%" }}
      initialViewState={{
        longitude: MapSettings.centerLng,
        latitude: MapSettings.centerLat,
        zoom: MapSettings.zoom,
      }}
      minZoom={MapSettings.minZoom}
      maxZoom={MapSettings.maxZoom}
      mapStyle={MapSettings.mapStyle}
    >
      <Source
        id="pmtiles-source"
        type="vector"
        url="pmtiles:///tiles.pmtiles"
        promoteId="county_fips"
      >
        <Layer
          id="pmtiles-layer"
          type="fill"
          source-layer="final_output"
          filter={[
            "all",
            ["==", ["get", "year"], selectedYear],
            ["has", MapSettings.investmentColumn],
            ["has", MapSettings.populationColumn],
            [">", ["get", MapSettings.populationColumn], 0],
          ]}
          paint={{
            "fill-color": [
              "interpolate",
              ["linear"],
              [
                "/",
                ["get", MapSettings.investmentColumn],
                ["get", MapSettings.populationColumn],
              ],
              ...colorStops,
            ],
            "fill-opacity": [
              "case",
              ["boolean", ["feature-state", "hover"], false],
              MapSettings.fillOpacityHover,
              MapSettings.fillOpacity,
            ],
          }}
        />
        <Layer
          id="pmtiles-outline"
          type="line"
          source-layer="final_output"
          filter={[
            "all",
            ["==", ["get", "year"], selectedYear],
            ["has", MapSettings.investmentColumn],
            ["has", MapSettings.populationColumn],
            [">", ["get", MapSettings.populationColumn], 0],
          ]}
          paint={{
            "line-color": MapSettings.lineColor,
            "line-opacity": [
              "case",
              ["boolean", ["feature-state", "hover"], false],
              0.8,
              MapSettings.lineOpacity,
            ],
            "line-width": [
              "case",
              ["boolean", ["feature-state", "hover"], false],
              MapSettings.lineWidthHover,
              MapSettings.lineWidth,
            ],
          }}
        />
      </Source>
      <HoverHandler />
      <FeatureInfoModal fieldsToDisplay={MapSettings.fieldsToDisplay} />

      {/* Legend - conditionally rendered */}
      {isLegendVisible && (
        <Legend
          breakpoints={MapSettings.investmentPerCapitaBreakpoints}
          colorScale={MapSettings.colorScale}
          selectedYear={selectedYear}
          onYearChange={setSelectedYear}
          availableYears={MapSettings.availableYears}
          onClose={() => setIsLegendVisible(false)}
        />
      )}

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
          onClick={toggleChat}
          size="icon"
          className="h-14 w-14 rounded-full bg-white/80 hover:bg-white shadow-lg"
          title={isChatVisible ? "Close Chat" : "Open Chat"}
        >
          <MessageSquareIcon className="h-6 w-6 text-black" />
        </Button>
      </div>

      {/* Chat Sidebar */}
      <ChatSidebar
        isVisible={isChatVisible}
        onClose={() => setIsChatVisible(false)}
      />
    </MapContainer>
  );
};

export default Map;
