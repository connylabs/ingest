.PHONY: build test fmt lint lint-go gen-mock vendor

OS ?= $(shell go env GOOS)
ARCH ?= $(shell go env GOARCH)
ALL_ARCH := amd64 arm arm64
BIN_DIR := bin
PLUGIN_DIR := $(BIN_DIR)/plugin
BINS := $(BIN_DIR)/$(OS)/$(ARCH)/ingest
PLUGINS := $(addprefix $(PLUGIN_DIR)/$(OS)/$(ARCH)/,s3 drive)
PROJECT := ingest
PKG := github.com/connylabs/$(PROJECT)

TAG := $(shell git describe --abbrev=0 --tags HEAD 2>/dev/null)
COMMIT := $(shell git rev-parse HEAD)
VERSION := $(COMMIT)
ifneq ($(TAG),)
    ifeq ($(COMMIT), $(shell git rev-list -n1 $(TAG)))
        VERSION := $(TAG)
    endif
endif
DIRTY := $(shell test -z "$$(git diff --shortstat 2>/dev/null)" || echo -dirty)
VERSION := $(VERSION)$(DIRTY)
LD_FLAGS := -ldflags "-s -w -X $(PKG)/version.Version=$(VERSION)"
SRC := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
GO_FILES ?= $$(find . -name '*.go' -not -path './vendor/*')
GO_PKGS ?= $$(go list ./... | grep -v "$(PKG)/vendor")

EMBEDMD_BINARY := $(shell pwd)/$(BIN_DIR)/embedmd
GOLINT_BINARY := $(shell pwd)/$(BIN_DIR)/golint
MOCKERY_BINARY := $(shell pwd)/$(BIN_DIR)/mockery
NATS_BINARY := $(shell pwd)/$(BIN_DIR)/nats
MINIO_CLIENT_BINARY := $(shell pwd)/$(BIN_DIR)/mc

BUILD_IMAGE ?= ghcr.io/goreleaser/goreleaser-cross:v1.18.1
CC_amd64 ?= gcc
CC_arm ?= arm-linux-gnueabihf-gcc
CC_arm64 ?= aarch64-linux-gnu-gcc
CONTAINERIZE_BUILD ?= true
E2E ?= true
BUILD_PREFIX :=
BUILD_SUFIX :=
ifeq ($(CONTAINERIZE_BUILD), true)
	BUILD_PREFIX := docker run --rm \
	    -u $$(id -u):$$(id -g) \
	    -v $$(pwd):/$(PROJECT) \
	    -w /$(PROJECT) \
	    -e CC=$(CC_$(ARCH)) \
	    --entrypoint '' \
	    $(BUILD_IMAGE) \
	    /bin/sh -c '
	BUILD_SUFIX := '
endif

build: $(BINS) $(PLUGINS)

build-%:
	@$(MAKE) --no-print-directory OS=$(word 1,$(subst -, ,$*)) ARCH=$(word 2,$(subst -, ,$*)) build

all-build: $(addprefix build-$(OS)-, $(ALL_ARCH))

$(BINS): $(SRC) go.mod
	@mkdir -p $(BIN_DIR)/$(word 2,$(subst /, ,$@))/$(word 3,$(subst /, ,$@))
	@echo "building: $@"
	@$(BUILD_PREFIX) \
	        GOARCH=$(word 3,$(subst /, ,$@)) \
	        GOOS=$(word 2,$(subst /, ,$@)) \
	        GOCACHE=$$(pwd)/.cache \
		CGO_ENABLED=1 \
		go build -mod=vendor -o $@ \
		    $(LD_FLAGS) \
		    ./cmd/$(@F) \
	$(BUILD_SUFIX)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(PLUGINS): $(SRC) go.mod
	@mkdir -p $(PLUGIN_DIR)/$(word 3,$(subst /, ,$@))/$(word 4,$(subst /, ,$@))
	@echo "building: $@"
	@$(BUILD_PREFIX) \
	        GOARCH=$(word 4,$(subst /, ,$@)) \
	        GOOS=$(word 3,$(subst /, ,$@)) \
	        GOCACHE=$$(pwd)/.cache \
		CGO_ENABLED=1 \
		go build -buildmode=plugin -mod=vendor -o $@ \
		    $(LD_FLAGS) \
		    ./plugins/$(@F) \
	$(BUILD_SUFIX)

$(PLUGIN_DIR):
	mkdir -p $(PLUGIN_DIR)

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

mocks/client.go: ingest.go $(MOCKERY_BINARY)
	rm -f $@
	$(MOCKERY_BINARY) --filename $(@F) --name="Client"

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


test: $(PLUGINS)
	E2E=$(E2E) go test ./...

vendor:
	go mod tidy
	go mod vendor

$(GOLINT_BINARY): | $(BIN_DIR)
	go build -mod=vendor -o $@ golang.org/x/lint/golint

$(NATS_BINARY): | $(BIN_DIR)
	go build -mod=vendor -o $@ github.com/nats-io/natscli/nats

$(MINIO_CLIENT_BINARY): | $(BIN_DIR)
	go build -mod=vendor -o $@ github.com/minio/mc

$(MOCKERY_BINARY): | $(BIN_DIR)
	go build -o $@ github.com/vektra/mockery/v2

$(EMBEDMD_BINARY): | $(BIN_DIR)
	go build -o $@ github.com/campoy/embedmd
