.PHONY: fmt lint lint-go gen-mock

BIN_DIR := bin
PROJECT := ingest
PKG := github.com/mietright/$(PROJECT)

GO_FILES ?= $$(find . -name '*.go' -not -path './vendor/*')
SRC := $(shell find . -type f -name '*.go' -not -path "./vendor/*")

GOLINT_BINARY := $(shell pwd)/$(BIN_DIR)/golint
MOCKERY_BINARY := $(shell pwd)/$(BIN_DIR)/mockery

$(BIN_DIR):
	mkdir -p bin

fmt:
	@echo $(GO_PKGS)
	gofmt -w -s $(GO_FILES)

lint: lint-go

gen-mock: mocks/Queue.go mocks/Enqueuer.go mocks/Dequeuer.go mocks/Subscription.go mocks/Identifiable.go mocks/Object.go

mocks/Queue.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --name="Queue"
	sed -i 's@github.com/nats-io/@github.com/nats-io/nats.go@g' $@

mocks/Enqueuer.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --name="Enqueuer"

mocks/Dequeuer.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --name="Dequeuer"

mocks/Subscription.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --name="Subscription"
	sed -i 's@github.com/nats-io/@github.com/nats-io/nats.go@g' $@

mocks/Identifiable.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --name="Identifiable"

mocks/Object.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --name="Object"

mocks/MinioClient.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --srcpkg github.com/mietright/ingest/dequeue --name="MinioClient"

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
