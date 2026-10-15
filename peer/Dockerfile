# Imagen del peer DFSha. Se construye desde la RAIZ del repo:
#   docker build -f peer/Dockerfile -t dfsha-peer .
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# codigo: paquete common (incluye los stubs gRPC en common/proto) y peer
COPY common ./common
COPY peer ./peer

ENV PYTHONPATH=/app
ENV DATA_DIR=/data
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 50051

CMD ["python", "-m", "peer.main"]
