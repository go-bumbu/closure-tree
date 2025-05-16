COMMIT_SHA_SHORT ?= $(shell git rev-parse --short=12 HEAD)
PWD_DIR := ${CURDIR}

default: help

#==========================================================================================
##@ Testing
#==========================================================================================
test: ## run fast go tests
	@go test ./...

test-full: ## run go full tests (uses test containers)
	@go test ./... -alldbs

test-race: ## run go full tests with race test (uses test containers)
	@go test ./... -race -count 2

lint: ## run go linter
	@# depends on https://github.com/golangci/golangci-lint
	@golangci-lint run

license-check: ## check for invalid licenses
	@# depends on : https://github.com/elastic/go-licence-detector
	@go list -m -mod=readonly  -json all  | go-licence-detector -includeIndirect -validate -rules allowedLicenses.json

benchmark: ## run go benchmarks
	@go test -run=^$$ -bench=. ./...

.PHONY: verify
verify: lint test-full license-check benchmark coverage ## run all tests

# Default coverage threshold is 80
COVERAGE_THRESHOLD ?= 80

.PHONY: coverage
coverage: ## check code coverage numbers
	@go test -coverprofile=coverage.out -covermode=atomic ./ > /dev/null; \
	if [ -f coverage.out ]; then \
		coverage=$$(go tool cover -func=coverage.out | grep total: | awk '{print $$3}' | sed 's/%//'); \
		if [ $$(echo "$$coverage < $(COVERAGE_THRESHOLD)" | bc -l) -eq 1 ]; then \
			echo "❌ Test coverage is below $(COVERAGE_THRESHOLD)%! Actual: $$coverage%"; \
			exit 1; \
		else \
			echo "✅ Test coverage is $$coverage%"; \
		fi; \
		rm -f coverage.out; \
	else \
		echo "⚠️ No test coverage data found"; \
		exit 1; \
	fi

cover-report: ## generate a coverage report
	go test -covermode=count -coverpkg=./... -coverprofile cover.out  ./...
	go tool cover -html cover.out -o cover.html
	open cover.html



clean:  ## delete test generated data
	@rm -f *.sqlite

#==========================================================================================
##@ Release
#==========================================================================================

.PHONY: check-git-clean
check-git-clean: # check if git repo is clen
	@git diff --quiet

.PHONY: check-branch
check-branch:
	@current_branch=$$(git symbolic-ref --short HEAD) && \
	if [ "$$current_branch" != "main" ]; then \
		echo "Error: You are on branch '$$current_branch'. Please switch to 'main'."; \
		exit 1; \
	fi

check_env: # check for needed envs
	@[ "${version}" ] || ( echo ">> version is not set, usage: make tag version=\"v1.2.3\" "; exit 1 )


tag: check_env check-branch check-git-clean verify ## create a tag and push to git
	@git diff --quiet || ( echo 'git is in dirty state' ; exit 1 )
	@[ "${version}" ] || ( echo ">> version is not set, usage: make tag version=\"v1.2.3\" "; exit 1 )
	@git tag -d $(version) || true
	@git tag -a $(version) -m "Release version: $(version)"
	@git push --delete origin $(version) || true
	@git push origin $(version) || true



#==========================================================================================
#  Help
#==========================================================================================
.PHONY: help
help: # Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
