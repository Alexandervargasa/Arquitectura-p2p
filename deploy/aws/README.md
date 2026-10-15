# Despliegue de DFSha en AWS (una instancia EC2 por peer)

Esta guía despliega el anillo DFSha en AWS con **una instancia EC2 por
peer**, cada peer en un contenedor Docker, comunicándose por Internet
mediante gossip sobre direcciones reales `IP:puerto`. Escala de 4 a 10
peers agregando instancias.

> **Importante:** todos los pasos que crean o modifican recursos en tu
> cuenta de AWS (lanzar instancias, grupos de seguridad) los ejecutas
> **tú**. Esta guía te dice qué hacer; no toca tu cuenta por sí sola.

## Arquitectura del despliegue

```
   Internet
      │
      ├── EC2 peer1 (seed)   IP1:50051  ─┐
      ├── EC2 peer2          IP2:50051   │  gossip / health-check
      ├── EC2 peer3          IP3:50051   │  + StoreBlob/GetBlob (chunks)
      └── EC2 peerN          IPN:50051  ─┘
```

- Cada peer corre `docker-compose.peer.yml` (un solo servicio `peer`).
- Se identifica con `SELF_ADDRESS = IP_PUBLICA:50051`.
- Un peer nuevo solo necesita conocer **una** semilla (`SEED_PEERS`)
  para integrarse; el gossip propaga el resto.
- Al entrar/salir peers, el **rebalanceo** mueve los chunks a sus nuevos
  dueños sin perder datos.

## Requisitos previos

- Cuenta AWS (free tier sirve).
- Un par de llaves EC2 (`.pem`) para SSH.
- El repositorio accesible por `git clone` (ajusta `REPO_URL` en
  `user-data.sh`).

## 1. Grupo de seguridad (Security Group)

Crea un Security Group para los peers con estas reglas de entrada:

| Tipo       | Puerto | Origen                                  |
|------------|--------|-----------------------------------------|
| SSH (TCP)  | 22     | Tu IP (para administrar)                |
| Custom TCP | 50051  | El propio Security Group (peer ↔ peer)  |
| Custom TCP | 50051  | Tu IP (para que el cliente se conecte)  |

Apuntar el origen del puerto 50051 al **propio** Security Group permite
que las instancias se hablen entre sí sin abrir el puerto a todo
Internet. Abre 50051 a tu IP solo si vas a correr el cliente desde tu
máquina.

> **Importante:** la regla "origen = el propio Security Group" solo
> aplica a tráfico que viaja por **IP privada** dentro de la VPC. Si los
> peers se configuran con su IP pública (`SELF_ADDRESS`/`SEED_PEERS`),
> AWS no reconoce ese tráfico como interno y lo bloquea con un timeout
> silencioso, aunque la regla se vea bien en la consola. Por eso en los
> pasos 2 y 3 usamos la **IP privada** de cada instancia para el gossip
> peer-a-peer (el cliente externo sigue usando la IP pública sin
> problema, ya que esa va por la otra regla).

## 2. Lanzar el primer peer (semilla)

1. Lanza una instancia EC2 (ej. `t3.micro`, Amazon Linux 2023 o Ubuntu).
2. Asígnale el Security Group del paso 1.
3. En **User data**, pega `user-data.sh`. Antes:
   - Ajusta `REPO_URL`.
   - Deja `SEED_PEERS=""` (es la semilla).
4. Lanza. El script instala Docker, clona el repo y arranca el peer. Usa
   la **IP privada** de la instancia para `SELF_ADDRESS` (ver nota del
   paso 1 sobre por qué debe ser privada y no pública).

Alternativa manual (sin user-data), ya dentro de la instancia:

```bash
# instalar docker + git (Amazon Linux 2023)
sudo dnf install -y docker git && sudo systemctl enable --now docker
git clone <REPO_URL> /opt/dfsha && cd /opt/dfsha/deploy/aws
cp .env.peer.example .env.peer
# editar .env.peer: SELF_ADDRESS=<IP_privada>:50051, SEED_PEERS= (vacio)
# la IP privada se ve con: hostname -I
sudo docker compose -f docker-compose.peer.yml --env-file .env.peer up --build -d
```

## 3. Lanzar peers adicionales (2, 3, 4 ...)

Igual que el primero, pero en `.env.peer` (o en las variables del
`user-data.sh`):

- `SELF_ADDRESS = <IP_privada_de_esta_instancia>:50051`
- `SEED_PEERS = <IP_privada_del_peer1>:50051` (basta una semilla viva)

Usa siempre **IP privada** para estas dos variables (ver nota del paso
1). El cliente (`verify_cluster.py` o tu propio cliente) sigue
conectándose a cualquier peer por su **IP pública**; eso no cambia.

Cada peer nuevo se integra al anillo por gossip sin reiniciar a los
demás. Verás en los logs del contenedor:

```bash
sudo docker logs -f dfsha-peer
# ... "ring activo cambio: [... todas las IPs ...]"
# ... "rebalanceo: N claves migradas"   (cuando corresponde)
```

## 4. Escalar de 4 a 10 peers

Repite el paso 3 hasta tener 10 instancias. Cada vez que entra un peer:

1. El gossip lo propaga a todo el anillo (segundos).
2. Cada peer recalcula los dueños de sus claves locales.
3. Las claves que ya no le corresponden se **empujan** a sus nuevos
   dueños y se borran localmente solo si el empuje tuvo éxito.

Resultado: los archivos subidos con 4 peers siguen accesibles con 10.

## 5. Verificar el cluster

Desde tu máquina (con acceso al puerto 50051 de algún peer) o desde una
de las instancias:

```bash
# desde la raiz del repo
PYTHONPATH=. python deploy/aws/verify_cluster.py \
    --host <IP_PUBLICA_DE_UN_PEER> --port 50051 --size-mb 50
```

Hace `ls /`, `send` de un archivo de 50 MB, `recv`, y verifica que el
sha256 coincide. Útil también para el video del entregable.

## 6. Probar tolerancia a fallos

```bash
# en una instancia que NO sea la unica dueña de todo:
sudo docker stop dfsha-peer
# vuelve a verificar: ls y recv deben seguir funcionando desde los demas
```

Con `REPLICATION_FACTOR=2`, el cluster tolera la caída de un peer sin
perder acceso a los datos (los chunks tienen una réplica en otro peer).

## 7. Benchmark de tamaño de chunk

Para comparar tamaños de chunk, redeploya los peers con distinto
`CHUNK_SIZE_MB` (8, 10, 16, ...) y mide el tiempo de `send`/`recv` del
mismo archivo. El cliente también lee `CHUNK_SIZE_MB` de su entorno.

## 8. Costos y apagado

- `t3.micro` entra en free tier (750 h/mes). Con 10 instancias
  consumirás esa cuota rápido: **apaga las instancias** (`stop`) cuando
  no las uses, o termínalas (`terminate`) al acabar.
- El almacenamiento es un volumen Docker local a cada instancia; al
  terminar la instancia se pierde (coherente con un cluster de prueba).

## Variables de entorno del peer (referencia)

| Variable             | Default | Descripción                                      |
|----------------------|---------|--------------------------------------------------|
| `SELF_ADDRESS`       | —       | `IP_publica:puerto` de este peer (obligatorio).  |
| `SEED_PEERS`         | vacío   | Semillas para gossip (al menos una, salvo peer1).|
| `REPLICATION_FACTOR` | `2`     | Copias de cada chunk/metadato.                   |
| `CHUNK_SIZE_MB`      | `8`     | Tamaño de chunk en MB.                           |
| `PEER_WEIGHT`        | `1`     | Peso de capacidad (nodos virtuales ponderados).  |
| `LISTEN_PORT`        | `50051` | Puerto en el que escucha el peer.                |
