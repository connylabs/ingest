FROM gcr.io/distroless/base-debian11
ARG TARGETOS
ARG TARGETARCH

COPY bin/$TARGETOS/$TARGETARCH/ingest /usr/local/bin/
COPY bin/plugin/$TARGETOS/$TARGETARCH/ /root/.config/ingest/plugins/

ENTRYPOINT ["/usr/local/bin/ingest"]
