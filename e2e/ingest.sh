#!/usr/bin/env bash
# shellcheck disable=SC1091
. lib.sh

test_ingest() {
    # Run ingest in on-off mode to put all documents in the queue.
    assert "$INGEST_BINARY --config=config --plugins=$PLUGIN_DIR --mode=enqueue" "the importer should exit 0"
    assert "retry 5 1 'the messages are not yet in NATS' check_messages ingest ingest 1" "there should be exactly 1 message in the ingest stream"
    # Run a one-line HTTP server that returns the last line of the request to fd 3.
    exec 3< <(out=$(mktemp -d)/e2e && mkfifo "$out" && nc -Nlp 8080 < <(cat "$out") | tee >(read -r && printf 'HTTP/1.1 200 OK\r\n\r\n' > "$out") | tail -n1)
    # Run the dequeuer in the background.
    "$INGEST_BINARY" --config=config --plugins="$PLUGIN_DIR" --mode=dequeue &
    # Clean up the dequeuer.
    dequeuer=$! && trap 'kill $dequeuer' ERR EXIT
    assert "retry 5 1 'the objects are not yet in Minio' check_objects e2e/destination 2" "there should be exactly two objects in Minio"
    assert_equals '["s3://destination/prefix/file"]' "$(cat <&3)" "the webhook should receive exactly one file in the list of documents"
}
