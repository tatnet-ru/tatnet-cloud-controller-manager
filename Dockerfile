# Build the CCM statically, then ship it on distroless static.
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/tatnet-cloud-controller-manager ./cmd/tatnet-cloud-controller-manager

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tatnet-cloud-controller-manager /usr/local/bin/tatnet-cloud-controller-manager
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/tatnet-cloud-controller-manager"]
