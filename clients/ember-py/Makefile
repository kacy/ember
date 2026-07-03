.PHONY: proto-gen test install-dev clean

proto-gen:
	python3 -m grpc_tools.protoc \
		--python_out=ember/proto \
		--grpc_python_out=ember/proto \
		--proto_path=../../proto \
		ember/v1/ember.proto
	# fix generated import path: ember.v1 -> ember.proto.ember.v1
	# (sed -i.bak works with both GNU and BSD sed; bare -i '' is macOS-only)
	sed -i.bak 's/from ember\.v1 import/from ember.proto.ember.v1 import/' \
		ember/proto/ember/v1/ember_pb2_grpc.py
	rm -f ember/proto/ember/v1/ember_pb2_grpc.py.bak

test:
	pytest -v

install-dev:
	pip install -e ".[dev]"

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
