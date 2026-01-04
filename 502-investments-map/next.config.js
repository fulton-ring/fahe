/** @type {import('next').NextConfig} */
const nextConfig = {
  // Empty turbopack config to silence error - using webpack for MapLibre worker files
  turbopack: {},
  webpack: (config) => {
    // Fix for MapLibre and other packages that use worker files
    config.module.rules.push({
      test: /\.worker\.(js|ts)$/,
      use: { loader: 'worker-loader' },
    });
    return config;
  },
};

module.exports = nextConfig;

