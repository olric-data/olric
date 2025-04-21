FROM golang:latest as build
WORKDIR /src/
COPY . /src/
RUN go mod download
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /usr/bin/olric-server /src/cmd/olric-server

FROM gcr.io/distroless/base-debian12
COPY --from=build /usr/bin/olric-server /usr/bin/olric-server
COPY --from=build /src/olric-server-docker.yaml /etc/olric-server.yaml

EXPOSE 3320 3322
ENTRYPOINT ["/usr/bin/olric-server", "-c", "/etc/olric-server.yaml"]
