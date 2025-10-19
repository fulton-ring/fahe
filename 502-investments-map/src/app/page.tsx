"use client";

import { useState, useCallback, useRef } from "react";
import Map from "../components/Map";
import ChatSidebar from "../components/ChatSidebar";
import { ResponsiveChatLayout } from "@/components/ResponsiveChatLayout";
import { useMediaQuery } from "@/hooks/use-media-query";
import type { Geometry } from "geojson";

export default function Home() {
  const [isChatVisible, setIsChatVisible] = useState(false);
  const flyToGeometryRef = useRef<((geometry: Geometry) => void) | null>(null);
  const isDesktop = useMediaQuery("(min-width: 768px)");

  // Wrapper to properly set the fly-to function - memoized to prevent infinite loops
  const handleFlyToReady = useCallback((fn: (geometry: Geometry) => void) => {
    flyToGeometryRef.current = fn;
  }, []);

  return (
    <div style={{ width: "100vw", height: "100vh", overflow: "hidden" }}>
      <ResponsiveChatLayout
        isDesktop={isDesktop}
        isChatVisible={isChatVisible}
        onChatVisibilityChange={setIsChatVisible}
        mapComponent={
          <Map
            isChatVisible={isChatVisible}
            onToggleChat={() => setIsChatVisible(!isChatVisible)}
            onFlyToReady={handleFlyToReady}
          />
        }
        chatComponent={
          <ChatSidebar
            onClose={() => setIsChatVisible(false)}
            onFlyTo={flyToGeometryRef.current || undefined}
          />
        }
      />
    </div>
  );
}
