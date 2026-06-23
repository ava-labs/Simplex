module github.com/ava-labs/simplex/jepsen/node

go 1.25.0

require (
	github.com/ava-labs/simplex v0.0.0
	github.com/supranational/blst v0.3.16
	go.etcd.io/bbolt v1.3.11
	go.uber.org/zap v1.28.0
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/ava-labs/simplex => ../../
