"""Custom build script to compile proto files during installation."""

import subprocess
import sys
from pathlib import Path
from setuptools.command.build_py import build_py as _build_py


def compile_protos():
    """Compile all .proto files in the project."""
    # Collect proto files from both dblib and microbench directories
    proto_files = (
        list(Path("dblib").glob("*.proto"))
        + list(Path("microbench").glob("*.proto"))
        + list(Path("macrobench").glob("*.proto"))
    )

    if not proto_files:
        print("No .proto files found to compile")
        return True

    for proto_file in proto_files:
        print(f"Compiling {proto_file}...")
        try:
            subprocess.run(
                [sys.executable, "-m", "grpc_tools.protoc", "-I.",
                 "--python_out=.", str(proto_file)],
                check=True,
                capture_output=True,
                text=True,
            )
            print(f"Successfully compiled {proto_file}")
        except subprocess.CalledProcessError as e:
            print(f"Error compiling {proto_file}: {e.stderr}", file=sys.stderr)
            return False
    return True


class BuildWithProtos(_build_py):
    """Custom build command that compiles proto files before building."""

    def run(self):
        """Run proto compilation then proceed with normal build."""
        if not compile_protos():
            raise RuntimeError("protobuf compilation failed")

        # Call the parent class's run method to do the actual build
        super().run()


if __name__ == "__main__":
    sys.exit(0 if compile_protos() else 1)
