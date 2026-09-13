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
  service; it assumes a `grpc_health_probe` binary is present in the
  container image — the scaffold Dockerfiles do not currently install it,
  so wiring that in (or switching to Kubernetes' native `grpc` probe type,
  stable since 1.24+) is a follow-up before these manifests are exercised
  against a real cluster.

## Out of scope for this pass

`ingress-nginx` Helm values and `mkcert` TLS setup (architecture doc
Section 1.4) are interactive local-cluster tooling, not scaffolded YAML —
configure those by hand when actually standing up the Docker Desktop K8s
cluster, per the architecture doc.
