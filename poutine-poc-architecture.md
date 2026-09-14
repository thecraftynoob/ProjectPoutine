# Poutine PoC Cluster — Architecture & Setup Reference

**Purpose:** k3s proof-of-concept cluster to validate application architecture and code (STT/address parsing pipeline, presumably feeding into or reading from Kafka/Postgres/Redis) before deploying to production infrastructure. Low/no real traffic expected — sizing decisions here prioritize "boots reliably" over "handles load."

Last updated: 2026-09-13

---

## Physical Host

- Single ESXi host, **16GB total RAM**
- ~5GB configured across other unrelated VMs (not part of this cluster)
- ~11GB available for the poutine-poc VMs, ~1GB kept as slack

## Cluster Nodes

All VMs run **Debian GNU/Linux 13 (trixie)**, kernel `6.12.107+deb13-amd64`, k3s `v1.36.4+k3s1`, containerd `2.3.4-k3s1.36`.

| Node | Role | RAM (post-increase) | k8s Role | Node Label |
|---|---|---|---|---|
| `poutine-poc-master` | k3s control plane + FreeSWITCH | 3GB | control-plane | — |
| `poutine-poc-1` | Kafka | 5GB | worker (agent) | `role=kafka` |
| `poutine-poc-2` | Postgres + Redis | 2GB | worker (agent) | `role=data` |

**Important:** `kubectl` only works from `poutine-poc-master`. poc1 and poc2 are k3s **agents** — they have no local kubeconfig/API server access (`/etc/rancher/k3s/k3s.yaml` doesn't exist on agents). All `kubectl label`, `kubectl apply`, and `kubectl get` commands must be run from master; the `nodeSelector` fields in the manifests are what actually place pods on the correct worker.

Original spec (all 3 nodes, before RAM increase): 2 CPU, 1.9GB RAM each, 100GB disk (`sda1`, 98G), VMware virtual platform, AMD Phenom II X6 1055T (virtualized).

## Namespaces & Workloads

### `kafka` namespace — on `poutine-poc-1`

- **Deployment:** `kafka` (1 replica)
- **Image:** `apache/kafka:3.9.0`
- **Mode:** KRaft (no ZooKeeper) — single node acts as both `broker` and `controller`
- **Heap:** `-Xmx1536m -Xms1536m`
- **Resources:** requests `250m CPU / 1.5Gi RAM`, limits `1 CPU / 2.5Gi RAM`
- **Storage:** PVC `kafka-pvc`, 10Gi, mounted at `/var/lib/kafka/data`
- **Ports:** 9092 (PLAINTEXT client), 9093 (CONTROLLER, internal)
- **Service:** `kafka.kafka.svc.cluster.local:9092`
- **Manifest file:** `kafka.yaml` (on master)

Verify broker health:
```bash
kubectl -n kafka exec -it deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### `data` namespace — on `poutine-poc-2`

**Postgres**
- **Deployment:** `postgres` (1 replica)
- **Image:** `postgres:17-alpine`
- **Env:** `POSTGRES_PASSWORD=changeme` (placeholder — rotate before anything beyond PoC), `PGDATA=/var/lib/postgresql/data/pgdata`
- **Resources:** requests `100m CPU / 128Mi RAM`, limits `500m CPU / 384Mi RAM`
- **Storage:** PVC `postgres-pvc`, 5Gi
- **Service:** `postgres.data.svc.cluster.local:5432`

**Redis**
- **Deployment:** `redis` (1 replica)
- **Image:** `redis:7-alpine`
- **Resources:** requests `50m CPU / 32Mi RAM`, limits `200m CPU / 128Mi RAM`
- **No persistent storage configured** (in-memory only, PoC-appropriate)
- **Service:** `redis.data.svc.cluster.local:6379`

**Manifest file:** `data-services.yaml` (on master)

Verify:
```bash
kubectl -n data exec -it deploy/postgres -- psql -U postgres -c '\l'
kubectl -n data exec -it deploy/redis -- redis-cli ping
```

### `freeswitch` namespace — on `poutine-poc-master`

Pre-existing, not part of this setup pass, but shares the host:
- `freeswitch-0` (StatefulSet-style pod)
- `freeswitch-gui` deployment, NodePort service (`8080:30880`), pulls image from private registry `192.168.4.20:5000/freeswitch-gui:latest`
- Uses hostNetwork, `nodeSelector: freeswitch=primary`
- PVC `freeswitch-pvc`, 10Gi, Retain reclaim policy

### System / networking add-ons (k3s defaults, on master)

- CoreDNS
- Traefik (ingress) — has `svclb-traefik` service-lb pods spread across all 3 nodes
- metrics-server
- local-path-provisioner (used for all PVCs above)

Private container registry in use elsewhere on this network: `192.168.4.20:5000`

## Connection Strings for Application Code

Use these cluster-internal DNS names so code doesn't hardcode IPs and carries over cleanly to production:

- Kafka: `kafka.kafka.svc.cluster.local:9092`
- Postgres: `postgres.data.svc.cluster.local:5432`
- Redis: `redis.data.svc.cluster.local:6379`

## Design Decisions & Rationale

- **Kafka chosen over Redpanda** deliberately — Kafka is the intended end-state technology, so the PoC should exercise the real Kafka wire protocol/behavior even though Redpanda would have been lighter on RAM.
- **KRaft mode (no ZooKeeper)** used to avoid running a second JVM just for coordination — reduces RAM footprint since this is a single-broker PoC.
- **Kafka isolated to its own node (poc1)** primarily because its JVM has a high memory floor (~1–2Gi) regardless of traffic volume — idle Kafka doesn't shrink the way Redis/Postgres do. Given all nodes share one physical ESXi host, this isn't about CPU/network isolation, just giving the JVM enough headroom to boot reliably without starving the k3s control plane or FreeSWITCH on master.
- **Postgres + Redis co-located on poc2** since both are lightweight at idle/low-traffic and don't need dedicated nodes.
- RAM was increased on all 3 VMs (via ESXi `vim-cmd`/`.vmx` edit while powered off) specifically to give Kafka's JVM room; poc2 and master increases are comfort margin, not hard requirements.

## Operational Notes

- Shutting down VMs for maintenance: graceful `sudo shutdown -h now` inside each guest (workers first, master last), or `vim-cmd vmsvc/power.shutdown <vmid>` from the ESXi host if guest tools are responsive; `vim-cmd vmsvc/power.off <vmid>` only as a last-resort hard stop.
- RAM changes require the VM powered off first, then either `vim-cmd vmsvc/setconfig.memory <vmid> <MB>` or direct `.vmx` edit (`memSize` field) + `vim-cmd vmsvc/reload <vmid>`.
- Default Postgres password (`changeme`) and lack of auth/TLS anywhere in this stack are PoC-only shortcuts — do not carry these forward to production without hardening.
