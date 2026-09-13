# deploy/k8s

One manifest set per service (`deployment.yaml` + `service.yaml`), plus
`namespace.yaml` creating the `ccaas-dev` namespace (architecture doc
Section 1.4 — one namespace per environment, not per tenant).

## Conventions

- **Naming:** `{service-name}-{deployment|svc|cm}` per architecture doc
  Section 5.
- **Images:** placeholder `ccaas/{service-name}:local`, `imagePullPolicy:
  IfNotPresent` per Section 1.4 (build directly into Docker Desktop's local
  daemon; no separate registry for the single-node dev loop).
- **Probes:** liveness and readiness both use an **exec probe** running
  [`grpc_health_probe`](https://github.com/grpc-ecosystem/grpc_health_probe)
  against each service's configured gRPC port. This is the chosen approach
  (rather than a native gRPC probe type) applied consistently across every
  service. Every service's Dockerfile installs a pinned `grpc_health_probe`
  binary (`v0.4.57`) into the final distroless image via a small dedicated
  build stage — see any service's Dockerfile for the pattern.
- **Infra connectivity (local dev):** `infra-config.yaml` is a ConfigMap
  (`ccaas-infra-config`) holding non-secret Redis/NATS/Postgres connection
  settings, pointed at `host.docker.internal` so pods in Docker Desktop's
  K8s can reach the docker-compose-hosted infra on the host (architecture
  doc Section 1.4's dev-loop split: infra in docker-compose, services in
  K8s). `infra-secret.example.yaml` is the matching example for the
  Postgres password — copy it to `infra-secret.local.yaml` (git-ignored),
  adjust the password to match your docker-compose.yml, and `kubectl
  apply` it yourself; it is not applied automatically and the example
  file's value should never be treated as a real secret. Task Router and
  Agent & Presence Service (the two services currently wired to real
  infra) consume both via `envFrom`/`valueFrom` in their `deployment.yaml`;
  `POSTGRES_DSN` is composed from the ConfigMap + Secret parts using
  Kubernetes' `$(VAR)` env-var interpolation rather than duplicated as a
  whole connection string. A real (non-local) deployment replaces the
  ConfigMap's values and the Secret's source with real cluster-hosted or
  managed infra addresses/credentials — no application code changes
  either way, per Section 1.4's Secrets guidance applied to this config
  too.

## Out of scope for this pass

`ingress-nginx` Helm values and `mkcert` TLS setup (architecture doc
Section 1.4) are interactive local-cluster tooling, not scaffolded YAML —
configure those by hand when actually standing up the Docker Desktop K8s
cluster, per the architecture doc.
