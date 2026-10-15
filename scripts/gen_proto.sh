#!/usr/bin/env bash
#
# Regenera los stubs gRPC de DFSha dentro del paquete common/proto/.
#
# Uso:
#   ./scripts/gen_proto.sh            # usa .venv/bin/python si existe
#   PYTHON=python3 ./scripts/gen_proto.sh
#
# Los .proto fuente viven en proto/. Los stubs generados quedan en
# common/proto/ con imports de estilo paquete (common.proto.<x>_pb2).
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON=".venv/bin/python"
  else
    PYTHON="python3"
  fi
fi

OUT="common/proto"
mkdir -p "$OUT"
touch "$OUT/__init__.py"

# Los .proto fuente viven en common/proto/. Compilamos con el raiz del
# repo como include-path (-I.) para que los stubs generados usen imports
# de estilo paquete (from common.proto import <x>_pb2).
"$PYTHON" -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  common/proto/dfsha.proto common/proto/ring.proto

echo "Stubs regenerados en $OUT/"
