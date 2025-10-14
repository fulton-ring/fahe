import { Button } from "@/components/ui/button";
import { XIcon } from "lucide-react";
import { CopilotChat } from "@copilotkit/react-ui";
import "@copilotkit/react-ui/styles.css";
import McpServerManager from "./MCPServerManager";

interface ChatSidebarProps {
  isVisible: boolean;
  onClose: () => void;
}

const ChatSidebar = ({ isVisible, onClose }: ChatSidebarProps) => {
  if (!isVisible) return null;

  return (
    <div
      className={`fixed top-0 right-0 h-screen bg-white shadow-2xl border-l z-50 transition-transform duration-300 ease-in-out flex flex-col ${
        isVisible ? "translate-x-0" : "translate-x-full"
      }`}
      style={{ width: "400px", maxWidth: "90vw" }}
    >
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b bg-white shrink-0">
        <h2 className="text-lg font-semibold">Map Assistant</h2>
        <Button
          variant="ghost"
          size="icon"
          onClick={onClose}
          className="shrink-0"
        >
          <XIcon className="h-5 w-5" />
        </Button>
      </div>

      {/* Chat Content */}
      <div className="flex-1 min-h-0 flex flex-col">
        {/* <McpServerManager /> */}
        <CopilotChat
          className="h-full"
          //           instructions={`You are an AI assistant helping users understand investment and economic data for Appalachian counties.

          // The map shows:
          // - Total investment dollars by county (2015-2023)
          // - Investment per capita (color-coded)
          // - Population data
          // - Poverty rates and percentages
          // - Education levels (high school completion, bachelor's degrees)
          // - Average household income
          // - Median earnings

          // You have access to a query MCP server that can retrieve detailed data about the investments. Use it when users ask specific questions about the data.

          // Help users:
          // - Understand the data visualizations
          // - Compare different counties
          // - Identify trends and patterns
          // - Explain what the colors and values mean
          // - Answer questions about specific counties or regions

          // Be concise, helpful, and data-focused in your responses.`}
          labels={{
            title: "Map Assistant",
            initial:
              "Hi! 👋 I can help you understand the investment and economic data on this map. What would you like to know?",
          }}
          observabilityHooks={{
            onMessageSent: (message) => {
              console.log("Message sent:", message);
            },
            onError: (error) => {
              console.error("Error:", error);
            },
            onChatStopped: () => {
              console.log("Chat stopped");
            },
          }}
        />
      </div>
    </div>
  );
};

export default ChatSidebar;
