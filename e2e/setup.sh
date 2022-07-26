#!/usr/bin/env bash
# shellcheck disable=SC1091
. lib.sh

setup_suite() {
    start_minio
    start_nats
}
