.PHONY: fmt lint lint-go gen-mock

BIN_DIR := bin
PROJECT := ingest
PKG := github.com/connylabs/$(PROJECT)

GO_FILES ?= $$(find . -name '*.go' -not -path './vendor/*')
GO_PKGS ?= $$(go list ./... | grep -v "$(PKG)/vendor")
SRC := $(shell find . -type f -name '*.go' -not -path "./vendor/*")

EMBEDMD_BINARY := $(shell pwd)/$(BIN_DIR)/embedmd
GOLINT_BINARY := $(shell pwd)/$(BIN_DIR)/golint
MOCKERY_BINARY := $(shell pwd)/$(BIN_DIR)/mockery

$(BIN_DIR):
	mkdir -p bin

README.md: $(EMBEDMD_BINARY) ingest.go
	$(EMBEDMD_BINARY) -w $@

fmt:
	@echo $(GO_PKGS)
	gofmt -w -s $(GO_FILES)

lint: lint-go

gen-mock: mocks/queue.go mocks/enqueuer.go mocks/dequeuer.go mocks/subscription.go mocks/identifiable.go mocks/object.go mocks/minio_client.go

mocks/queue.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --filename $(@F) --name="Queue"
	sed -i 's@github.com/nats-io/@github.com/nats-io/nats.go@g' $@

mocks/enqueuer.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --filename $(@F) --name="Enqueuer"

mocks/dequeuer.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --filename $(@F) --name="Dequeuer"

mocks/subscription.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --filename $(@F) --name="Subscription"
	sed -i 's@github.com/nats-io/@github.com/nats-io/nats.go@g' $@

mocks/identifiable.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --filename $(@F) --name="Identifiable"

mocks/object.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --filename $(@F) --name="Object"

mocks/minio_client.go: storage/s3/s3.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --srcpkg github.com/connylabs/ingest/storage/s3 --filename $(@F) --name="MinioClient"

lint-go: $(GOLINT_BINARY)
	@echo 'go vet $(GO_PKGS)'
	@vet_res=$$(go vet $(GO_PKGS) 2>&1); if [ -n "$$vet_res" ]; then \
		echo ""; \
		echo "Go vet found issues. Please check the reported issues"; \
		echo "and fix them if necessary before submitting the code for review:"; \
		echo "$$vet_res"; \
		exit 1; \
	fi
	@echo '$(GOLINT_BINARY) $(GO_PKGS)'
	@lint_res=$$($(GOLINT_BINARY) $(GO_PKGS)); if [ -n "$$lint_res" ]; then \
		echo ""; \
		echo "Golint found style issues. Please check the reported issues"; \
		echo "and fix them if necessary before submitting the code for review:"; \
		echo "$$lint_res"; \
		exit 1; \
	fi
	@echo 'gofmt -d -s $(GO_FILES)'
	@fmt_res=$$(gofmt -d -s $(GO_FILES)); if [ -n "$$fmt_res" ]; then \
		echo ""; \
		echo "Gofmt found style issues. Please check the reported issues"; \
		echo "and fix them if necessary before submitting the code for review:"; \
		echo "$$fmt_res"; \
		exit 1; \
	fi


$(GOLINT_BINARY): | $(BIN_DIR)
	go build -o $@ golang.org/x/lint/golint

$(MOCKERY_BINARY): | $(BIN_DIR)
	go build -o $@ github.com/vektra/mockery/v2

$(EMBEDMD_BINARY): | $(BIN_DIR)
	go build -o $@ github.com/campoy/embedmd
