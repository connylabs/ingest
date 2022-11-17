FROM gcr.io/distroless/base-debian11
ARG TARGETOS=linux
ARG TARGETARCH=amd64

COPY bin/$TARGETOS/$TARGETARCH/ingest /usr/local/bin/
COPY bin/plugin/$TARGETOS/$TARGETARCH/ /root/.config/ingest/plugins/

ENTRYPOINT ["/usr/local/bin/ingest"]
