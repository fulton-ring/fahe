import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { XIcon } from "lucide-react";

interface LegendProps {
  title?: string;
  subtitle?: string;
  breakpoints: number[];
  colorScale: (t: number) => string;
  selectedYear: number;
  onYearChange: (year: number) => void;
  availableYears: number[];
  dataSource?: string;
  onClose?: () => void;
}

const Legend = ({
  title = "Investment per Capita",
  subtitle = "Dollar amount per person",
  breakpoints,
  colorScale,
  selectedYear,
  onYearChange,
  availableYears,
  dataSource = "FAHE Section 502 Investments",
  onClose,
}: LegendProps) => {
  // Generate legend steps from the color scale and breakpoints
  const steps = breakpoints.map((value, index) => ({
    value,
    color: colorScale(index / (breakpoints.length - 1)),
  }));

  return (
    <div className="absolute top-4 right-4 z-10 w-64 rounded-lg border bg-white/95 p-4 shadow-lg backdrop-blur-sm">
      <div className="flex flex-col gap-3">
        {/* Title and Close Button */}
        <div className="flex items-start justify-between gap-2">
          <div className="flex flex-col gap-1 flex-1">
            <h3 className="font-semibold text-sm">{title}</h3>

            {/* Year selector */}
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">Year:</span>
              <Select
                value={String(selectedYear)}
                onValueChange={(value) => onYearChange(Number(value))}
              >
                <SelectTrigger className="h-7 w-24 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {availableYears.map((year) => (
                    <SelectItem
                      key={year}
                      value={String(year)}
                      className="text-xs"
                    >
                      {year}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {onClose && (
            <Button
              variant="ghost"
              size="icon"
              onClick={onClose}
              className="h-7 w-7 -mt-1 -mr-1"
            >
              <XIcon className="h-4 w-4" />
            </Button>
          )}
        </div>

        {/* Color scale */}
        <div className="flex flex-col gap-2">
          <p className="text-xs font-medium text-muted-foreground">
            {subtitle}
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
        <div className="border-t pt-3 text-xs text-muted-foreground">
          <p>Data: {dataSource}</p>
        </div>
      </div>
    </div>
  );
};

export default Legend;
