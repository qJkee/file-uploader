FROM golang:1.27 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /objectstore .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /objectstore /objectstore
# distroless/static:nonroot runs as UID 65532, so the data volume must be
# writable by that UID — the usual first failure when deploying this.
# Mount ONE volume at /data: mounting at /data/blobs but not /data/tmp puts
# them on different filesystems and every PUT fails with EXDEV. The startup
# device check catches that at boot rather than per request.
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/objectstore", "-data", "/data", "-addr", ":8080"]
