import { openai } from "@ai-sdk/openai";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

import {
  streamText,
  UIMessage,
  convertToModelMessages,
  experimental_createMCPClient as createMCPClient,
  type ToolSet,
  stepCountIs,
} from "ai";

import { MCP_CONFIG } from "@/lib/mcp-config";

// Allow streaming responses up to 2 minutes (for Vercel deployments)
export const maxDuration = 120;

export async function POST(req: Request) {
  const { messages }: { messages: UIMessage[] } = await req.json();

  let mcpClient: Awaited<ReturnType<typeof createMCPClient>> | undefined;
  let mcpTools: ToolSet = {};

  // Try to connect to MCP server
  try {
    const transport = new StreamableHTTPClientTransport(
      new URL(MCP_CONFIG.serverUrl)
    );

    mcpClient = await createMCPClient({ transport });

    // Log session ID after connection
    console.log("✅ Connected to MCP server");
    console.log("Session ID:", transport.sessionId);

    mcpTools = await mcpClient.tools();
    console.log("Available MCP tools:", Object.keys(mcpTools));
    console.log("Session ID after tools:", transport.sessionId);
  } catch (error) {
    console.error("❌ Failed to connect to MCP server:", error);
    console.log("Error details:", error);
    console.log("Continuing without MCP tools...");
    // Continue without MCP tools if connection fails
  }

  try {
    const result = streamText({
      model: openai("gpt-5-mini"),
      system: `You are an AI assistant helping users understand investment and economic data for Appalachian counties.

The map shows:
- Total investment dollars by county (2015-2023)
- Investment per capita (color-coded)
- Population data
- Poverty rates and percentages
- Education levels (high school completion, bachelor's degrees)
- Average household income
- Median earnings

${
  Object.keys(mcpTools).length > 0
    ? `You have access to tools that can query the actual data. Use them when users ask specific questions about:
- Specific counties or states
- Numerical comparisons
- Trends over time
- Statistical analysis

The workspace ID is: "fahe-502-investments"

Prefer to use geojson mode when calling execute_workspace_sql tool to return formatted query results to the user. The application
allows the user to fly to a location if the tool result is a geojson feature collection.

If a tool call fails, acknowledge the error gracefully and try to provide helpful information based on what you know about the data structure and map visualization.`
    : ""
}

Help users:
- Understand the data visualizations
- Compare different counties
- Identify trends and patterns
- Explain what the colors and values mean
- Answer questions about specific counties or regions

Be concise, helpful, and data-focused in your responses.${
        Object.keys(mcpTools).length > 0
          ? " When using tools, explain what data you're retrieving."
          : ""
      }`,
      messages: convertToModelMessages(messages),
      tools: mcpTools,
      stopWhen: stepCountIs(5), // Allow up to 5 steps for multi-step tool calls
      abortSignal: AbortSignal.timeout(120_000), // 2 minute timeout

      // Handle errors during streaming
      onError: ({ error }) => {
        console.error("❌ Error during streaming:", error);
        // The error will be logged but streaming will continue
      },

      // Handle step completion and errors
      onStepFinish: ({ text, toolCalls, toolResults, finishReason, usage }) => {
        // Log any tool call attempts
        if (toolCalls && toolCalls.length > 0) {
          console.log(
            `🔧 Tool calls:`,
            toolCalls.map((tc) => tc.toolName).join(", ")
          );
        }
        // Check tool results for errors
        // According to AI SDK docs, tool errors in streamText are emitted as
        // tool-result parts with isError: true (not thrown as exceptions)
        if (toolResults && toolResults.length > 0) {
          toolResults.forEach((result) => {
            // Check if this is an error result
            if ("isError" in result && result.isError) {
              console.error(`❌ Tool error in ${result.toolName}:`, result);
            } else {
              console.log(`✅ Tool result from ${result.toolName}`);
            }
          });
        }

        // Log step completion status
        if (finishReason === "stop") {
          console.log("✅ Step completed successfully");
        } else if (finishReason === "error") {
          console.error("❌ Step finished with error");
        }
      },

      // Cleanup when finished
      onFinish: async () => {
        if (mcpClient) {
          try {
            await mcpClient.close();
            console.log("✅ MCP client closed");
          } catch (error) {
            console.error("❌ Error closing MCP client:", error);
          }
        }
      },
    });

    return result.toUIMessageStreamResponse();
  } catch (error) {
    // Cleanup on error
    if (mcpClient) {
      try {
        await mcpClient.close();
      } catch (closeError) {
        console.error("❌ Error closing MCP client:", closeError);
      }
    }

    // Log the error
    console.error("❌ Fatal error in chat route:", error);

    // Return error as a chat message so user sees it in the conversation
    const errorMessage =
      error instanceof Error ? error.message : "Unknown error occurred";

    return streamText({
      model: openai("gpt-4o-mini"),
      system:
        "You are a helpful assistant that acknowledges errors gracefully.",
      prompt: `The user's previous message resulted in an error: "${errorMessage}". Apologize for the error, explain that something went wrong, and suggest they try again or rephrase their question. Be concise and helpful.`,
    }).toUIMessageStreamResponse();
  }
}
