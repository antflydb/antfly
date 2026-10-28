FROM node:24-slim
WORKDIR /app
COPY . .
RUN corepack enable && cd ts && pnpm install --frozen-lockfile && pnpm --filter @antfly/sdk --filter @antfly/embedded --filter @antfly/better-auth build && pnpm --filter @antfly/example-express-auth build
WORKDIR /app/examples/express-auth
ENV NODE_ENV=production DATABASE_PATH=/data/app.aflite PORT=3000
VOLUME /data
EXPOSE 3000
CMD ["node", "server.ts"]
