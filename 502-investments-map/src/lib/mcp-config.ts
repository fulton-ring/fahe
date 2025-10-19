/**
 * MCP (Model Context Protocol) Server Configuration
 *
 * Configure your MCP server connection here.
 * The MCP server provides tools for querying investment data.
 */

export const MCP_CONFIG = {
  // MCP server URL - update this to match your server
  serverUrl: process.env.MCP_SERVER_URL || "http://localhost:8000/mcp",

  // Enable/disable MCP integration
  enabled: process.env.MCP_ENABLED !== "false",

  // Optional: Add headers for authentication
  headers: process.env.MCP_API_KEY
    ? { Authorization: `Bearer ${process.env.MCP_API_KEY}` }
    : undefined,

  // Timeout for MCP connection (ms)
  connectionTimeout: 5000,
} as const;
