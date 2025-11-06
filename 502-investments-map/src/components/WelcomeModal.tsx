"use client";

import { useState, useEffect } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import {
  MapIcon,
  DollarSignIcon,
  MessageSquareIcon,
  LayersIcon,
} from "lucide-react";

const STORAGE_KEY = "fahe-map-welcome-dismissed";

const WelcomeModal = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [dontShowAgain, setDontShowAgain] = useState(false);

  useEffect(() => {
    // Check if user has previously dismissed the welcome modal
    const dismissed = localStorage.getItem(STORAGE_KEY);
    if (!dismissed) {
      setIsOpen(true);
    }
  }, []);

  const handleClose = () => {
    if (dontShowAgain) {
      localStorage.setItem(STORAGE_KEY, "true");
    }
    setIsOpen(false);
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && handleClose()}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-xl">
            Welcome to the USDA 502 Investments Map
          </DialogTitle>
          <DialogDescription className="text-base">
            Explore Section 502 housing investments and income limits across
            Appalachian counties
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 mt-4">
          {/* What is this map */}
          <div>
            <h3 className="font-semibold text-sm mb-2 flex items-center gap-2">
              <MapIcon className="h-4 w-4" />
              What is this map?
            </h3>
            <p className="text-sm text-muted-foreground">
              This interactive visualization shows USDA Section 502 housing
              investments and income eligibility limits across Appalachian
              counties from 2015 to 2025. The map helps identify investment
              patterns and understand eligibility criteria for rural housing
              assistance programs.
            </p>
          </div>

          {/* Features */}
          <div>
            <h3 className="font-semibold text-sm mb-2 flex items-center gap-2">
              <LayersIcon className="h-4 w-4" />
              Map Features
            </h3>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li>
                <strong className="text-foreground">
                  Investment per Capita Layer:
                </strong>{" "}
                View investment amounts per person by county (2015-2023). Colors
                indicate investment intensity.
              </li>
              <li>
                <strong className="text-foreground">
                  Income Limits Layer:
                </strong>{" "}
                See 2025 income eligibility limits for different household sizes
                (1-8 people).
              </li>
              <li>
                <strong className="text-foreground">
                  Interactive Tooltips:
                </strong>{" "}
                Hover over counties to see quick statistics.
              </li>
              <li>
                <strong className="text-foreground">
                  Detailed Information:
                </strong>{" "}
                Click on any county to view comprehensive data in a modal.
              </li>
            </ul>
          </div>

          {/* AI Assistant */}
          <div>
            <h3 className="font-semibold text-sm mb-2 flex items-center gap-2">
              <MessageSquareIcon className="h-4 w-4" />
              AI Assistant
            </h3>
            <p className="text-sm text-muted-foreground">
              Click the chat button in the bottom-right corner to ask questions
              about the data. The AI assistant can query specific counties,
              compare regions, analyze trends, and help you understand the
              investment patterns and income limits.
            </p>
          </div>

          {/* How to use */}
          <div>
            <h3 className="font-semibold text-sm mb-2 flex items-center gap-2">
              <DollarSignIcon className="h-4 w-4" />
              Getting Started
            </h3>
            <ol className="space-y-1 text-sm text-muted-foreground list-decimal list-inside">
              <li>
                Use the legend (top-right) to switch between layers and adjust
                filters
              </li>
              <li>Hover over counties to see quick stats</li>
              <li>Click counties for detailed information</li>
              <li>Ask the AI assistant questions about the data</li>
              <li>Download GeoJSON data using the button in the legend</li>
            </ol>
          </div>

          {/* Actions */}
          <div className="flex items-center justify-between pt-4 border-t">
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={dontShowAgain}
                onChange={(e) => setDontShowAgain(e.target.checked)}
                className="rounded"
              />
              <span className="text-muted-foreground">
                Don&apos;t show this again
              </span>
            </label>
            <Button onClick={handleClose}>Get Started</Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default WelcomeModal;
