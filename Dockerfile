FROM node:20-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci

FROM deps AS build
WORKDIR /app
COPY tsconfig*.json ./
COPY src ./src
RUN npm run build

FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --from=build /app/dist ./dist
RUN mkdir -p /app/state && touch /app/.env.production
EXPOSE 3000
CMD ["node", "--localstorage-file=/app/state/.node-localstorage", "dist/main.js"]
