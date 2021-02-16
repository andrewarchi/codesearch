GO := go1.16rc1
TARGETS := linux/amd64 linux/386 freebsd/amd64 freebsd/386 \
	windows/amd64 windows/386 darwin/amd64

.PHONY: native
native:
	@mkdir -p bin
	${GO} build -o bin/ github.com/andrewarchi/codesearch/cmd/{cgrep,cindex,csearch}

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

.PHONY: install
install:
	${GO} install ./...

.PHONY: clean
clean:
	rm -rf bin
