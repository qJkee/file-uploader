FROM golang:1.27 AS build
WORKDIR /src

# Dependencies first: source edits then don't re-download the module tree.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Pure-Go SQLite means no cgo, which is what makes a scratch image possible.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /objectstore .

# Staged here because scratch has no shell to mkdir with, and / is root-owned
# 0755 — a non-root process could not create /data at runtime.
RUN mkdir -p /staging/data

FROM scratch
# No /etc/passwd in scratch, so the UID is numeric. 65532 is the conventional
# nonroot UID; the data volume must be writable by it, which is the usual
# first failure when deploying this.
COPY --from=build --chown=65532:65532 /staging/data /data
COPY --from=build /objectstore /objectstore
USER 65532:65532

# Mount ONE volume at /data. Mounting at /data/blobs but not /data/tmp puts
# them on different filesystems and every PUT fails with EXDEV; the startup
# device check catches that at boot rather than per request.
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/objectstore", "-data", "/data", "-addr", ":8080"]
