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
  Postgres password(s) — copy it to `infra-secret.local.yaml`
  (git-ignored), adjust the password(s) to match your docker-compose.yml,
  and `kubectl apply` it yourself; it is not applied automatically and the
  example file's values should never be treated as real secrets. Every
  service that touches Postgres (task-router, tenant-identity,
  historical-reporting, background-worker-pool) consumes both via
  `valueFrom` in its `deployment.yaml` and composes TWO DSNs from the
  ConfigMap + Secret parts using Kubernetes' `$(VAR)` env-var
  interpolation rather than duplicating either as a whole connection
  string: `POSTGRES_DSN` (the Postgres superuser, "ccaas" — used only to
  run that service's Migrate() at startup) and `RUNTIME_POSTGRES_DSN` (a
  shared, non-superuser, NOBYPASSRLS runtime role, "ccaas_app" — used for
  that service's real, ongoing queries, so Row-Level Security is actually
  enforced rather than silently bypassed by a superuser connection). See
  ARCHITECTURE_FLOW.md §5 and GAPS.md's "Closed gaps" section for the full
  writeup of why this two-identity split exists. A real (non-local)
  deployment replaces the ConfigMap's values and the Secret's source with
  real cluster-hosted or managed infra addresses/credentials — no
  application code changes either way, per Section 1.4's Secrets guidance
  applied to this config too.

## Rebuilding and redeploying a service's image (local dev)

The standard loop is: `docker build -f services/{name}/Dockerfile -t
ccaas/{name}:local .` (build context is the repo ROOT, not the service
directory) then `kubectl rollout restart deployment/{name}-deployment -n
ccaas-dev`.

**Observed caveat (this environment, Docker Desktop Kubernetes):** a
`docker build` that retags an EXISTING `:local` tag with new content was
observed, in this milestone's live verification, to not always be picked
up by Kubernetes/containerd's own image store even after `kubectl
rollout restart` or deleting the pod outright — `kubectl get pod -o
jsonpath='{.items[0].status.containerStatuses[0].imageID}'` kept
resolving to the OLD image's digest under the `:local` tag, while `docker
images`/`docker image inspect ccaas/{name}:local` on the same host
correctly showed the new one. This looks like a Docker Desktop
dockerd-vs-containerd image store synchronization quirk for a **reused**
tag, not anything wrong with the build or the manifests.
Two things that reliably resolved it when this happened:
1. Build with `--provenance=false --load` (avoids a multi-platform
   attestation manifest list, which may be part of what confuses the
   digest resolution for a reused tag).
2. If the stale digest persists even after that, build under a **new,
   unique tag** (e.g. `:v2`) and `kubectl set image
   deployment/{name}-deployment {name}=ccaas/{name}:v2 -n ccaas-dev` —
   a genuinely new tag always forces correct resolution. Retag back to
   `:local` afterward for the next dev cycle if desired, understanding
   the same quirk can recur on the NEXT reused-tag rebuild.
If a rollout restart's pod comes up successfully but is unexpectedly
still running old code (e.g. a `grpc.Unimplemented` for an RPC you just
added), check `imageID` first before assuming a code or manifest bug.

## Out of scope for this pass

`ingress-nginx` Helm values and `mkcert` TLS setup (architecture doc
Section 1.4) are interactive local-cluster tooling, not scaffolded YAML —
configure those by hand when actually standing up the Docker Desktop K8s
cluster, per the architecture doc.
