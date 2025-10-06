import { Button } from "@/components/ui/button";
import { XIcon } from "lucide-react";
import { CopilotChat } from "@copilotkit/react-ui";
import "@copilotkit/react-ui/styles.css";

interface ChatSidebarProps {
  isVisible: boolean;
  onClose: () => void;
}

const ChatSidebar = ({ isVisible, onClose }: ChatSidebarProps) => {
  if (!isVisible) return null;

  return (
    <div
      className={`fixed top-0 right-0 h-full bg-white shadow-2xl border-l z-50 transition-transform duration-300 ease-in-out ${
        isVisible ? "translate-x-0" : "translate-x-full"
      }`}
      style={{ width: "400px", maxWidth: "90vw" }}
    >
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b bg-white">
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
      <div className="h-[calc(100%-64px)]">
        <CopilotChat
          instructions={`You are an AI assistant helping users understand investment and economic data for Appalachian counties. 
          
The map shows:
- Total investment dollars by county (2015-2023)
- Investment per capita (color-coded)
- Population data
- Poverty rates and percentages
- Education levels (high school completion, bachelor's degrees)
- Average household income
- Median earnings

Help users:
- Understand the data visualizations
- Compare different counties
- Identify trends and patterns
- Explain what the colors and values mean
- Answer questions about specific counties or regions

Be concise, helpful, and data-focused in your responses.`}
          labels={{
            title: "Map Assistant",
            initial:
              "Hi! 👋 I can help you understand the investment and economic data on this map. What would you like to know?",
          }}
        />
      </div>
    </div>
  );
};

export default ChatSidebar;
