#!/usr/bin/env bash
INGEST_BINARY="${INGEST_BINARY:-ingest}"
PLUGIN_DIR="${PLUGIN_DIR:-~/.config/ingest/plugins}"
ACCESS_KEY_ID="${ACCESS_KEY_ID:-AKIAIOSFODNN7EXAMPLE}"
SECRET_ACCESS_KEY="${SECRET_ACCESS_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
DEFAULT_FETCH_INTERVAL="${DEFAULT_FETCH_INTERVAL:-0}"
DEFAULT_PAGINATION="${DEFAULT_PAGINATION:-10}"
NATS_BINARY="${NATS_BINARY:-docker run --rm --network host --entrypoint nats natsio/nats-box}"
MC_HOST_e2e="${MC_HOST_e2e:-http://$ACCESS_KEY_ID:$SECRET_ACCESS_KEY@172.17.0.1:9000}"
MINIO_CLIENT_BINARY="${MINIO_CLIENT_BINARY:-docker run --rm --network host --env MC_HOST_e2e=$MC_HOST_e2e minio/mc}"

retry() {
    local COUNT="${1:-10}"
    local SLEEP="${2:-5}"
    local ERROR=$3
    [ -n "$ERROR" ] && ERROR="$ERROR "
    shift 3
    for c in $(seq 1 "$COUNT"); do
        if "$@"; then
            return 0
        else
            printf "%s(attempt %d/%d)\n" "$ERROR" "$c" "$COUNT" | color "$YELLOW" 1>&2
            if [ "$c" != "$COUNT" ]; then
                printf "retrying in %d seconds...\n" "$SLEEP" | color "$YELLOW" 1>&2
                sleep "$SLEEP"
            fi
        fi
    done
    return 1
}

# Blocks until the HTTP endpoint returns 200.
block_until_ready() {
    retry 30 5 "the $2 endpoint is not ready yet" curl --fail --silent "$1" > /dev/null
}

mc() {
    MC_HOST_e2e=$MC_HOST_e2e $MINIO_CLIENT_BINARY "$@"
}

start_minio() {
    docker run --name minio --detach --rm -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=$ACCESS_KEY_ID" -e "MINIO_ROOT_PASSWORD=$SECRET_ACCESS_KEY" quay.io/minio/minio:RELEASE.2021-10-23T03-28-24Z server /data --console-address ":9001"
    block_until_ready http://localhost:9000/minio/health/ready minio
    mc mb e2e/source
    mc cp lib.sh e2e/source/prefix/file
    mc mb e2e/destination
}

stop_minio() {
    docker rm --force minio || true
}

check_objects() {
    [ "$(mc ls "$1" --summarize | tail -n1 | cut -d' ' -f3)" -eq "$2" ]
}

start_nats() {
    docker run --rm --name jetstream --detach -p 4222:4222 -p 8222:8222 nats:2.6.1 -js --http_port 8222
    until $NATS_BINARY restore jetstream; do printf .; sleep 1; done
    block_until_ready http://localhost:8222/ nats
}

stop_nats() {
    docker rm --force jetstream || true
}

check_messages() {
    [ "$($NATS_BINARY consumer info "$1" "$2" | grep Unprocessed | tr -s ' '| cut -d' ' -f4)" -eq "$3" ]
}
