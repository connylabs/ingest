FROM gcr.io/distroless/base-debian11

COPY bin/linux/amd64/ingest /usr/local/bin/
COPY bin/plugin/linux/amd64/ /root/.config/ingest/plugins/

ENTRYPOINT ["/usr/local/bin/ingest"]
