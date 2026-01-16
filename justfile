test:
  go test -race ./...

[script]
compile:
  #!/usr/bin/env bash
  export PATH="$PATH:$(go env GOPATH)/bin"

  if test -z $(which protoc-gen-go); then
  	echo "Installing protoc-gen-go..."
  	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
  fi

  if test -z $(which protoc-gen-go-grpc); then
    echo "Installing grpc package..."
    go get google.golang.org/grpc@v1.78.0
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.0
  fi

  protoc api/v1/*.proto \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    --proto_path=.
