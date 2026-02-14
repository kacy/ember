.PHONY: proto-gen test

proto-gen:
	protoc --go_out=. --go_opt=module=github.com/kacy/ember-go \
		--go-grpc_out=. --go-grpc_opt=module=github.com/kacy/ember-go \
		--proto_path=../../proto \
		ember/v1/ember.proto

test:
	go test -v ./...
