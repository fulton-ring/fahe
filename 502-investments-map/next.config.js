/** @type {import('next').NextConfig} */
const nextConfig = {
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

