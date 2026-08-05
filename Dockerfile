FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY go.sum ./
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' -o /out/audit-api ./cmd/audit-api && \
    go build -trimpath -ldflags='-s -w' -o /out/audit-governance-worker ./cmd/audit-governance-worker && \
    go build -trimpath -ldflags='-s -w' -o /out/audit-outbox-relay ./cmd/audit-outbox-relay && \
    go build -trimpath -ldflags='-s -w' -o /out/audit-kafka-consumer ./cmd/audit-kafka-consumer && \
    go build -trimpath -ldflags='-s -w' -o /out/audit-projector ./cmd/audit-projector

FROM alpine:3.22
RUN addgroup -S audit && adduser -S -G audit audit && mkdir -p /var/lib/audit && chown -R audit:audit /var/lib/audit
COPY --from=build /out/audit-api /audit-api
COPY --from=build /out/audit-governance-worker /audit-governance-worker
COPY --from=build /out/audit-outbox-relay /audit-outbox-relay
COPY --from=build /out/audit-kafka-consumer /audit-kafka-consumer
COPY --from=build /out/audit-projector /audit-projector
USER audit:audit
WORKDIR /var/lib/audit
EXPOSE 8089
ENTRYPOINT ["/audit-api"]
