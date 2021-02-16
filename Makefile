GO := go
TARGETS := linux/amd64 linux/386 freebsd/amd64 freebsd/386 \
	windows/amd64 windows/386 darwin/amd64

.PHONY: install
install:
	${GO} install ./...

.PHONY: native
native:
	${GO} build -o bin/ ./...

.PHONY: release
release: $(TARGETS)

%:
	lib/build $@

.PHONY: test
test:
	${GO} test ./...

.PHONY: lint
lint:
	golint ./...

.PHONY: vet
vet:
	${GO} vet ./...

.PHONY: clean
clean:
	rm -rf bin
