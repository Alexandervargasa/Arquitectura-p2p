#!/usr/bin/env bash
#
# EC2 user-data: instala Docker y arranca un peer de DFSha al bootear.
#
# COMO USARLO
# -----------
# Este script se pega en el campo "User data" al lanzar la instancia EC2
# (Amazon Linux 2023 o Ubuntu). Antes de usarlo, EDITA las variables de
# abajo (SELF_ADDRESS, SEED_PEERS, ...). Como user-data no conoce la IP
# publica de antemano, por defecto la descubrimos desde el metadata de EC2.
#
# OJO: user-data corre como root en el primer arranque.
set -euxo pipefail

# ----- configuracion del peer (EDITAR) -----
REPO_URL="https://github.com/Alexandervargasa/Arquitectura-p2p.git"
REPO_DIR="/opt/dfsha"
SEED_PEERS=""          # vacio en el primer peer; en los demas: IP_SEED:50051
REPLICATION_FACTOR="2"
CHUNK_SIZE_MB="8"
PEER_WEIGHT="1"
PORT="50051"

# ----- instalar git (nombre de paquete universal en AL y Ubuntu) -----
if command -v dnf >/dev/null 2>&1; then
  dnf install -y git
elif command -v apt-get >/dev/null 2>&1; then
  apt-get update -y
  apt-get install -y git ca-certificates curl
fi

# ----- instalar Docker (incluye el plugin "docker compose") -----
# Se usa el script de instalacion oficial de Docker en vez de los paquetes
# docker.io/docker-compose-plugin de los repos de cada distro: en algunas
# versiones de Ubuntu recientes "docker-compose-plugin" no existe en sus
# repos oficiales (solo en el repo propio de Docker), lo que hace fallar
# "apt-get install" completo y deja la instancia sin Docker. get.docker.com
# funciona igual en Amazon Linux y Ubuntu y siempre trae el plugin compose.
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker

# ----- descubrir IP publica desde el metadata de EC2 (IMDSv2) -----
TOKEN=$(curl -sS -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
PUBLIC_IP=$(curl -sS -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/public-ipv4)
SELF_ADDRESS="${PUBLIC_IP}:${PORT}"

# ----- traer el codigo y arrancar el peer -----
git clone "$REPO_URL" "$REPO_DIR" || (cd "$REPO_DIR" && git pull)
cd "$REPO_DIR/deploy/aws"

cat > .env.peer <<EOF
SELF_ADDRESS=${SELF_ADDRESS}
SEED_PEERS=${SEED_PEERS}
REPLICATION_FACTOR=${REPLICATION_FACTOR}
CHUNK_SIZE_MB=${CHUNK_SIZE_MB}
PEER_WEIGHT=${PEER_WEIGHT}
EOF

docker compose -f docker-compose.peer.yml --env-file .env.peer up --build -d
