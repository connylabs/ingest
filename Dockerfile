FROM --platform=$BUILDPLATFORM golang:1.18 as build

WORKDIR /build

COPY go.sum go.mod ./
RUN go mod download

COPY . .

ARG TARGETOS TARGETARCH

RUN CONTAINERIZE_BUILD=false GOARCH=$TARGETARCH GOOS=$TARGETOS make

FROM gcr.io/distroless/base-debian11
ARG TARGETOS TARGETARCH

COPY --from=build /build/bin/$TARGETOS/$TARGETARCH/ingest /usr/local/bin/
COPY --from=build /build/bin/plugin/$TARGETOS/$TARGETARCH /root/.config/ingest/plugins/

ENTRYPOINT ["/usr/local/bin/ingest"]
