compile:
	@if ! which protoc-gen-go > /dev/null; then \
		echo "Installing protoc-gen-go..."; \
		go install google.golang.org/protobuf/cmd/protoc-gen-go@latest; \
	fi
	export PATH="$PATH:$(go env GOPATH)/bin" && \
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.

test:
  go test -race ./...
