FROM node:22-alpine AS build
WORKDIR /web
RUN corepack enable
COPY package.json pnpm-lock.yaml ./
RUN corepack pnpm install --frozen-lockfile
COPY . ./
RUN corepack pnpm build

FROM nginx:1.28-alpine
ENV AUDIT_API_UPSTREAM=http://audit-api:8089 \
    SNAPLINK_API_UPSTREAM=http://snaplink:8082
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /web/dist /usr/share/nginx/html
EXPOSE 8080
