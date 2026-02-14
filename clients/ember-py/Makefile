.PHONY: proto-gen test install-dev clean

proto-gen:
	python -m grpc_tools.protoc \
		--python_out=ember/proto \
		--grpc_python_out=ember/proto \
		--proto_path=../../proto \
		ember/v1/ember.proto
	# fix generated import path: ember.v1 -> ember.proto.ember.v1
	sed -i '' 's/from ember\.v1 import/from ember.proto.ember.v1 import/' \
		ember/proto/ember/v1/ember_pb2_grpc.py

test:
	pytest -v

install-dev:
	pip install -e ".[dev]"

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
