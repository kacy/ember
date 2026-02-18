#!/usr/bin/env python3
"""compile user_profile.proto into python bindings.

run this once before main.py. requires grpcio-tools (included in requirements.txt).
"""

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent


def main() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"--proto_path={HERE}",
            f"--python_out={HERE}",
            "user_profile.proto",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    print("generated user_profile_pb2.py")


if __name__ == "__main__":
    main()
