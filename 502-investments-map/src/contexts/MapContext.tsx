"use client";

import { createContext, useContext, useState, ReactNode } from "react";

export type LayerType = "investment" | "income";

interface MapContextType {
  // Layer selection
  selectedLayer: LayerType;
  setSelectedLayer: (layer: LayerType) => void;

  // Year selection (for investment layer)
  selectedYear: number;
  setSelectedYear: (year: number) => void;

  // Household size (for income layer)
  selectedHouseholdSize: number;
  setSelectedHouseholdSize: (size: number) => void;

  // Legend visibility
  isLegendVisible: boolean;
  setIsLegendVisible: (visible: boolean) => void;
}

const MapContext = createContext<MapContextType | undefined>(undefined);

export const useMapContext = () => {
  const context = useContext(MapContext);
  if (!context) {
    throw new Error("useMapContext must be used within a MapProvider");
  }
  return context;
};

interface MapProviderProps {
  children: ReactNode;
}

export const MapProvider = ({ children }: MapProviderProps) => {
  const [selectedLayer, setSelectedLayer] = useState<LayerType>("investment");
  const [selectedYear, setSelectedYear] = useState(2023);
  const [selectedHouseholdSize, setSelectedHouseholdSize] = useState(4);
  const [isLegendVisible, setIsLegendVisible] = useState(true);

  return (
    <MapContext.Provider
      value={{
        selectedLayer,
        setSelectedLayer,
        selectedYear,
        setSelectedYear,
        selectedHouseholdSize,
        setSelectedHouseholdSize,
        isLegendVisible,
        setIsLegendVisible,
      }}
    >
      {children}
    </MapContext.Provider>
  );
};
