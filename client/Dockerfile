# Imagen del cliente DFSha. Se construye desde la RAIZ del repo:
#   docker build -f client/Dockerfile -t dfsha-client .
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# el cliente necesita el paquete common (stubs gRPC + config) y client
COPY common ./common
COPY client ./client

ENV PYTHONPATH=/app
WORKDIR /work

ENTRYPOINT ["python", "/app/client/client.py"]
CMD ["--host", "peer1", "--port", "50051"]
