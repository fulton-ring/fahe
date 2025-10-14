import { useCopilotChat } from "@copilotkit/react-core";
import { useEffect } from "react";

function McpServerManager() {
  const { setMcpServers } = useCopilotChat({
    headers: {
      "x-workspace-id": "fahe-502-investments",
    },
  });

  useEffect(() => {
    setMcpServers([
      {
        // Try a sample MCP server at https://mcp.composio.dev/
        // endpoint: "https://marauders-query-mcp.fly.dev/sse",
        endpoint:
          "https://marauders-query-mcp.fly.dev/mcp?workspace_id=fahe-502-investments",
      },
    ]);
  }, [setMcpServers]);

  return null;
}

export default McpServerManager;
