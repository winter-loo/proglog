export CONFIG_PATH := env_var_or_default('PROGLOG_HOME', env_var('HOME') + '/.proglog')

test: acl-config
  go test -race ./...

acl-config:
  mkdir -p $CONFIG_PATH
  cp test/acl-model.conf $CONFIG_PATH/acl-model.conf
  cp test/acl-policy.csv $CONFIG_PATH/acl-policy.csv

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

[script]
gencert:
  #!/usr/bin/env bash
  export PATH="$PATH:$(go env GOPATH)/bin"

  mkdir -p $CONFIG_PATH

  if test -z $(which cfssl); then
    echo "Installing cfssl..."
    go install github.com/cloudflare/cfssl/cmd/cfssl@v1.6.5
  fi

  if test -z $(which cfssljson); then
    echo "Installing cfssljson..."
    go install github.com/cloudflare/cfssl/cmd/cfssljson@v1.6.5
  fi

  # https://blog.cloudflare.com/how-to-build-your-own-public-key-infrastructure
  
  # generate certificate for CA
  cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca

  # generate certificate for our server
  cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=test/ca-config.json \
    -profile=server \
    test/server-csr.json | cfssljson -bare server

  # generate certificate for our client
  cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=test/ca-config.json \
    -profile=client \
    -cn="root" \
    test/client-csr.json | cfssljson -bare root-client

  cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=test/ca-config.json \
    -profile=client \
    -cn="nobody" \
    test/client-csr.json | cfssljson -bare nobody-client

  mv *.pem *.csr ${CONFIG_PATH}
