# 502 Investments Map - Next.js

This is a [Next.js](https://nextjs.org/) project that visualizes FAHE Section 502 investments across Appalachian counties.

## Features

- **Interactive Map**: MapLibre GL JS-based map with PMTiles for efficient tile delivery
- **Dual Layer Visualization**: Toggle between investment per capita and income limits
- **Investment Visualization**: Color-coded counties by investment per capita (2015-2023)
- **Income Limits Layer**: View 2025 USDA income eligibility limits for 1-8 person households
- **Welcome Guide**: First-time user modal explaining map features and usage
- **AI Chat Assistant**: Powered by Vercel AI SDK with custom UI
- **Chat History**: Conversations saved to localStorage with create/delete/switch functionality
- **MCP Integration**: Connect to Model Context Protocol servers for data querying
- **SQL Query Tables**: Automatic table rendering for SQL query results
- **Spatial SQL Results**: Interactive GeoJSON tables with click-to-fly map integration
- **Multi-Step Tool Usage**: AI can use multiple tools to answer complex questions
- **Responsive UI**: Built with Tailwind CSS and shadcn/ui components

## Getting Started

### 1. Install dependencies

```bash
npm install
```

### 2. Set up environment variables

Create a `.env.local` file in the project root for local development:

```bash
# Required
OPENAI_API_KEY=your_openai_api_key_here

# Optional: Enable MCP integration
# MCP_ENABLED=true
# MCP_SERVER_URL=http://localhost:8000/mcp
# MCP_API_KEY=your_mcp_api_key_here
# MARAUDERS_ANALYSIS_KEY=your_analysis_key_here
```

**Note:** 
- `.env.local` is for local development only and is gitignored
- For production, set environment variables in the Vercel dashboard (see [Deployment](#deployment) section)
- You can also create a `.env` file with production values (also gitignored) for reference

Get your OpenAI API key from [OpenAI Platform](https://platform.openai.com/api-keys).

See [ENV_SETUP.md](./ENV_SETUP.md) for detailed environment setup and [MCP_INTEGRATION.md](./MCP_INTEGRATION.md) for MCP configuration.

### 3. Run the development server

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## Project Structure

```
src/
├── app/              # Next.js App Router
│   ├── layout.tsx   # Root layout
│   ├── page.tsx     # Home page
│   └── api/
│       └── chat/    # AI chat backend
├── components/       # React components
│   ├── Map.tsx      # Main map component
│   ├── legends/     # Layer-specific legends
│   │   ├── LegendNavigator.tsx
│   │   ├── InvestmentLegend.tsx
│   │   └── IncomeLegend.tsx
│   ├── layers/      # Map data layers
│   │   ├── InvestmentLayer.tsx
│   │   └── IncomeLayer.tsx
│   ├── ChatSidebar.tsx  # AI chat interface
│   └── ui/          # shadcn/ui components
├── contexts/        # React contexts
│   └── MapContext.tsx  # Shared map state
└── lib/             # Utilities
public/              # Static assets
  ├── investments.pmtiles
  └── income.pmtiles
```

## Technologies

- **Next.js 16**: React framework
- **TypeScript**: Type safety
- **Tailwind CSS**: Styling
- **MapLibre GL JS**: Interactive maps
- **PMTiles**: Efficient vector tiles
- **Vercel AI SDK**: Streaming AI chat with OpenAI
- **shadcn/ui**: UI components

## Build

To create a production build:

```bash
npm run build
npm start
```

## Deployment

This project is configured to deploy to [Vercel](https://vercel.com). You can deploy using either the Vercel CLI or the Vercel MCP agent.

### Deploying with Vercel CLI

1. **Install Vercel CLI** (if not already installed):
   ```bash
   npm i -g vercel
   ```

2. **Login to Vercel**:
   ```bash
   vercel login
   ```

3. **Deploy to production**:
   ```bash
   vercel --prod
   ```

### Deploying with Vercel MCP Agent

The Vercel MCP (Model Context Protocol) agent provides an AI-powered way to deploy and manage your Vercel projects. If you have the Vercel MCP server configured in your AI assistant, you can simply ask it to deploy the app.

**Using the MCP Agent:**
- The agent can automatically detect your project configuration
- It can set and update environment variables
- It can trigger deployments and monitor build status
- It can check deployment logs and troubleshoot issues

**Example commands you can use with the MCP agent:**
- "Deploy this app to Vercel"
- "Set the production environment variables"
- "Check the deployment status"
- "View the latest deployment logs"

### Environment Variables for Production

Before deploying, ensure all required environment variables are set in your Vercel project:

**Required:**
- `OPENAI_API_KEY` - Your OpenAI API key for AI chat functionality

**Optional (for MCP integration):**
- `MCP_SERVER_URL` - URL of your MCP server (defaults to `http://localhost:8000/mcp`)
- `MCP_API_KEY` - API key for MCP server authentication
- `MARAUDERS_ANALYSIS_KEY` - Analysis ID for data queries
- `MCP_ENABLED` - Set to `"false"` to disable MCP (defaults to enabled)

**Setting environment variables via CLI:**
```bash
# Add a new environment variable
echo "your_value" | vercel env add VARIABLE_NAME production

# List all environment variables
vercel env ls

# Remove an environment variable
vercel env rm VARIABLE_NAME production
```

**Setting environment variables via Dashboard:**
1. Go to your project on [Vercel Dashboard](https://vercel.com)
2. Navigate to Settings → Environment Variables
3. Add or update variables for Production, Preview, and/or Development environments

### Post-Deployment

After deployment, your app will be available at:
- Production URL: `https://502-investments-map.vercel.app`
- Preview URLs: Generated for each deployment

The app automatically uses environment variables configured in Vercel, so make sure all required variables are set before deploying.

## Learn More

- [Next.js Documentation](https://nextjs.org/docs)
- [MapLibre Documentation](https://maplibre.org/maplibre-gl-js/docs/)
- [Vercel AI SDK Documentation](https://ai-sdk.dev/docs)

## Additional Documentation

- **[MAP_ARCHITECTURE.md](./MAP_ARCHITECTURE.md)** - Map component architecture and organization
- **[CHAT_HISTORY.md](./CHAT_HISTORY.md)** - Chat conversation persistence and management
- **[SQL_TABLE_RENDERING.md](./SQL_TABLE_RENDERING.md)** - How SQL query results are rendered as tables in the chat
- **[SPATIAL_SQL.md](./SPATIAL_SQL.md)** - Spatial SQL queries with interactive GeoJSON tables
- **[MCP_INTEGRATION.md](./MCP_INTEGRATION.md)** - Connect to MCP servers for data querying tools
- **[VERCEL_AI_SDK_MIGRATION.md](./VERCEL_AI_SDK_MIGRATION.md)** - Migration from CopilotKit to Vercel AI SDK
- **[ENV_SETUP.md](./ENV_SETUP.md)** - Environment variable configuration
