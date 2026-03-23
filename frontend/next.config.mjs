/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  eslint: {
    ignoreDuringBuilds: true,  // ESLint runs in CI separately, not during Docker build
  },
};

export default nextConfig;
