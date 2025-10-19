"use client";

import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable";
import {
  Drawer,
  DrawerContent,
  DrawerHeader,
  DrawerTitle,
} from "@/components/ui/drawer";
import type { ReactNode } from "react";

interface ResponsiveChatLayoutProps {
  isDesktop: boolean;
  isChatVisible: boolean;
  onChatVisibilityChange?: (visible: boolean) => void;
  mapComponent: ReactNode;
  chatComponent: ReactNode;
}

/**
 * Responsive layout that shows chat as a resizable sidebar on desktop
 * and a bottom drawer on mobile
 */
export const ResponsiveChatLayout = ({
  isDesktop,
  isChatVisible,
  onChatVisibilityChange,
  mapComponent,
  chatComponent,
}: ResponsiveChatLayoutProps) => {
  // Desktop: Resizable sidebar
  if (isDesktop) {
    return (
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel defaultSize={isChatVisible ? 60 : 100} minSize={30}>
          {mapComponent}
        </ResizablePanel>
        {isChatVisible && (
          <>
            <ResizableHandle withHandle />
            <ResizablePanel defaultSize={40} minSize={25} maxSize={70}>
              {chatComponent}
            </ResizablePanel>
          </>
        )}
      </ResizablePanelGroup>
    );
  }

  // Mobile: Bottom drawer
  return (
    <>
      {mapComponent}
      <Drawer
        open={isChatVisible}
        onOpenChange={onChatVisibilityChange}
        direction="bottom"
      >
        <DrawerContent className="h-[85vh] flex flex-col">
          <DrawerHeader className="sr-only">
            <DrawerTitle>Map Assistant</DrawerTitle>
          </DrawerHeader>
          {chatComponent}
        </DrawerContent>
      </Drawer>
    </>
  );
};
