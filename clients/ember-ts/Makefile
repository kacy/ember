.PHONY: proto-gen build test clean

proto-gen:
	npx --yes --package=@grpc/proto-loader proto-loader-gen-types \
		--proto_path=../../proto \
		--outDir=src/generated \
		../../proto/ember/v1/ember.proto
	mkdir -p proto/ember/v1
	cp ../../proto/ember/v1/ember.proto proto/ember/v1/ember.proto

build:
	npm run build

test:
	npm test

clean:
	rm -rf dist src/generated node_modules
