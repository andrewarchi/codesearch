TARGETS := linux/amd64 linux/386 freebsd/amd64 freebsd/386 \
	windows/amd64 windows/386 darwin/amd64

.PHONY: native
native:
	@mkdir -p bin
	go build -o bin/ github.com/andrewarchi/codesearch/cmd/{cgrep,cindex,csearch}

.PHONY: all
all: $(TARGETS)

%:
	lib/build $@
