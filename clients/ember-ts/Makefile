.PHONY: proto-gen test clean

proto-gen:
	npx --yes --package=@grpc/proto-loader proto-loader-gen-types \
		--proto_path=../../proto \
		--outDir=src/generated \
		../../proto/ember/v1/ember.proto

test:
	npm test

clean:
	rm -rf src/generated node_modules
