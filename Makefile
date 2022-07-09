all: build

.PHONY: build
build:
	go build

.PHONY: test
test:
	go test

.PHONY: tidy
tidy:
	go mod tidy
