TARGETS := linux/amd64 linux/386 freebsd/amd64 freebsd/386 \
	windows/amd64 windows/386 darwin/amd64

.PHONY: native
native:
	@mkdir -p bin
	go build -o bin/ github.com/andrewarchi/codesearch/cmd/{cgrep,cindex,csearch}

.PHONY: release
release: $(TARGETS)

%:
	lib/build $@

.PHONY: test
test:
	go test ./...

.PHONY: lint
lint:
	golint ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: install
install:
	go install ./...

.PHONY: clean
clean:
	rm -rf bin
