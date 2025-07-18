# syntax = docker/dockerfile:1

# Adjust NODE_VERSION as desired
FROM node:22.17.1-slim AS base

LABEL fly_launch_runtime="NodeJS"

# NodeJS app lives here
WORKDIR /app

# Set production environment
ENV NODE_ENV=production
# 4096MB available memory - 200MB for anything else
ENV NODE_OPTIONS="--max-old-space-size=3896"

ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

# Throw-away build stage to reduce size of final image
FROM base AS build

# Install packages needed to build node modules
RUN apt-get update -qq && \
    apt-get install -y python-is-python3 pkg-config build-essential

# Install node modules
COPY --link package.json package-lock.json .
RUN npm install --production=false

# Copy application code
COPY --link . .

# Remove development dependencies
RUN npm prune --production


# Final stage for app image
FROM base

# Copy built application
COPY --from=build /app /app

# Start the server by default, this can be overwritten at runtime
CMD [ "npm", "run", "start" ]
