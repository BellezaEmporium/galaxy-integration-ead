import sys
from pathlib import Path

# The generated protobuf modules use sibling imports like `import common_pb2`.
# When this package is imported as `generated_protos`, Python will not
# automatically search the package directory for those top-level names.
# Prepend the package directory to sys.path so those generated modules load
# correctly in both tests and the installed plugin runtime.
_PACKAGE_DIR = Path(__file__).resolve().parent
if str(_PACKAGE_DIR) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_DIR))
