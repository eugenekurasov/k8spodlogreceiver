# Developer entry points. The CI workflows call these same targets, so a
# green `make check` locally is the check a pull request gets.

GO ?= go

# Extra flags for the test targets, e.g. `make test GOTESTFLAGS="-run TestFoo"`.
GOTESTFLAGS ?=

# Tool versions. Each is pinned so every machine and CI run agree; the
# `renovate:` comments are what keeps them current.

# renovate: datasource=go depName=go.opentelemetry.io/collector/cmd/builder
OCB_VERSION ?= v0.161.0
# renovate: datasource=go depName=go.opentelemetry.io/collector/cmd/mdatagen
MDATAGEN_VERSION ?= v0.161.0
# renovate: datasource=go depName=github.com/golangci/golangci-lint/v2
GOLANGCI_LINT_VERSION ?= v2.14.0
# renovate: datasource=go depName=github.com/google/addlicense
ADDLICENSE_VERSION ?= v1.2.0

# The integration cluster. Patch tags are bumped by Renovate; which minor
# to develop against is a maintainer decision, as in the CI matrix.
KIND_CLUSTER ?= k8spodlog-test
# renovate: datasource=docker depName=kindest/node
KIND_NODE_IMAGE ?= kindest/node:v1.37.0

# Tools are installed under ./bin (gitignored) rather than $GOPATH/bin, so
# they cannot collide with a differently versioned copy already on $PATH.
# The version is part of the path, which is what makes a version bump
# reinstall rather than silently reuse the old binary.
TOOLS_DIR := $(CURDIR)/bin
GOLANGCI_LINT := $(TOOLS_DIR)/$(GOLANGCI_LINT_VERSION)/golangci-lint
ADDLICENSE := $(TOOLS_DIR)/$(ADDLICENSE_VERSION)/addlicense
OCB := $(TOOLS_DIR)/$(OCB_VERSION)/builder
MDATAGEN := $(TOOLS_DIR)/$(MDATAGEN_VERSION)/mdatagen

# `go install pkg@version` refuses to run in vendor mode, and a populated
# vendor/ turns that on implicitly, so GOFLAGS is cleared for tool installs.
GOINSTALL = GOFLAGS= GOBIN=$(dir $@) $(GO) install

# Hand-written Go files. mdatagen's output carries no SPDX header upstream
# either, and regenerating would drop one, so it is excluded.
HANDWRITTEN_GO_FILES = $$(find . -name '*.go' \
	-not -path './bin/*' \
	-not -path './build/*' \
	-not -path './vendor/*' \
	-not -name 'generated_*.go')

.DEFAULT_GOAL := help

##@ General

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next } \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo

.PHONY: check
check: fmt-check lint license-check test

##@ Development

.PHONY: fmt
fmt: ## Format the Go sources
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## Fail if any Go source is unformatted
	@unformatted=$$(gofmt -l $(HANDWRITTEN_GO_FILES)); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run golangci-lint
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: $(GOLANGCI_LINT) ## Run golangci-lint and apply the fixes it can make
	$(GOLANGCI_LINT) run --fix

.PHONY: license-check
license-check: $(ADDLICENSE) ## Check SPDX headers on hand-written Go files
	@$(ADDLICENSE) -check $(HANDWRITTEN_GO_FILES)

.PHONY: license-fix
license-fix: $(ADDLICENSE) ## Add missing SPDX headers
	@$(ADDLICENSE) -c "Yevhenii Kurasov" -l apache -s=only $(HANDWRITTEN_GO_FILES)

.PHONY: generate
generate: $(MDATAGEN) ## Regenerate the mdatagen files from metadata.yaml
	$(MDATAGEN) metadata.yaml

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	$(GO) mod tidy

.PHONY: vendor
vendor: ## Populate vendor/
	$(GO) mod vendor

##@ Tests

.PHONY: test
test: ## Run the unit tests
	$(GO) test $(GOTESTFLAGS) ./...

.PHONY: test-integration
test-integration: ## Run the integration tests against the current kubectl context
	$(GO) test $(GOTESTFLAGS) -tags integration -timeout 180s ./...

.PHONY: kind-up
kind-up: ## Create the kind cluster the integration tests use
	kind create cluster --name $(KIND_CLUSTER) --image $(KIND_NODE_IMAGE)

.PHONY: kind-down
kind-down: ## Delete the kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

##@ Build

.PHONY: build
build: $(OCB) ## Build a local collector containing this receiver
	$(OCB) --config builder-config.yaml

.PHONY: clean
clean: ## Remove the built collector and the installed tools
	rm -rf build $(TOOLS_DIR)

##@ Tools

.PHONY: tools
tools: $(GOLANGCI_LINT) $(ADDLICENSE) $(OCB) $(MDATAGEN) ## Install the pinned tools into ./bin

$(GOLANGCI_LINT):
	$(GOINSTALL) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(ADDLICENSE):
	$(GOINSTALL) github.com/google/addlicense@$(ADDLICENSE_VERSION)

$(OCB):
	$(GOINSTALL) go.opentelemetry.io/collector/cmd/builder@$(OCB_VERSION)

# mdatagen's own go.mod carries replace directives, which `go install
# pkg@version` refuses to honour. Building it as a dependency of a throwaway
# module sidesteps that: replace directives in a dependency are ignored.
$(MDATAGEN):
	@mkdir -p $(dir $@)
	@tmp=$$(mktemp -d) && cd $$tmp \
		&& GOFLAGS= $(GO) mod init mdatagentool >/dev/null \
		&& GOFLAGS= $(GO) get go.opentelemetry.io/collector/cmd/mdatagen@$(MDATAGEN_VERSION) \
		&& GOFLAGS= $(GO) build -o $@ go.opentelemetry.io/collector/cmd/mdatagen \
		&& rm -rf $$tmp
