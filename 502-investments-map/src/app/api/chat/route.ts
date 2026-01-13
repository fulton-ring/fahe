import { openai } from "@ai-sdk/openai";
import { experimental_createMCPClient as createMCPClient } from "@ai-sdk/mcp";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

import { streamText, UIMessage, convertToModelMessages, stepCountIs } from "ai";

import { MCP_CONFIG } from "@/lib/mcp-config";

// Allow streaming responses up to 2 minutes (for Vercel deployments)
export const maxDuration = 120;

export async function POST(req: Request) {
  const { messages }: { messages: UIMessage[] } = await req.json();

  const transport = new StreamableHTTPClientTransport(
    new URL(MCP_CONFIG.serverUrl),
    MCP_CONFIG.headers
      ? {
          requestInit: {
            headers: MCP_CONFIG.headers,
          },
        }
      : undefined
  );

  const mcpClient = await createMCPClient({ transport });

  // Log session ID after connection
  console.log("✅ Connected to MCP server");
  console.log("Session ID:", transport.sessionId);

  const mcpTools = await mcpClient.tools();
  const datasets = await mcpClient.readResource({
    uri: `analysis://${MCP_CONFIG.analysisId}/datasets`,
  });

  if (!mcpTools) {
    return new Response("No MCP tools found", { status: 500 });
  }

  if (!datasets) {
    return new Response("No datasets found", { status: 500 });
  }

  console.log("Available MCP tools:", Object.keys(mcpTools));
  console.log("Datasets resource:", datasets);

  const datasetsContents = datasets.contents
    .map((content) => {
      if (content.text) return content.text;
      return "";
    })
    .filter(Boolean)
    .join("\n\n");

  console.log("Datasets contents:", datasetsContents);

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
  datasetsContents
    ? `Datasets:\n${JSON.stringify(JSON.parse(datasetsContents), null, 2)}`
    : ""
}

${
  Object.keys(mcpTools).length > 0
    ? `You have access to tools that can query the actual data. Use them when users ask specific questions about:
- Specific counties or states
- Numerical comparisons
- Trends over time
- Statistical analysis

The analysis ID is: "${MCP_CONFIG.analysisId}"

Prefer to use geojson mode when calling execute_sql tool to return formatted query results to the user. The application
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
      tools: mcpTools as any,
      stopWhen: stepCountIs(10), // Allow up to 10 steps for tool calls
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
      model: openai("gpt-5-mini"),
      system:
        "You are a helpful assistant that acknowledges errors gracefully.",
      prompt: `The user's previous message resulted in an error: "${errorMessage}". Apologize for the error, explain that something went wrong, and suggest they try again or rephrase their question. Be concise and helpful.`,
    }).toUIMessageStreamResponse();
  }
}
