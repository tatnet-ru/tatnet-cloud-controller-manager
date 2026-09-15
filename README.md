# tatnet-cloud-controller-manager

[tatnet.ru](https://tatnet.ru) · [панель](https://min.tatnet.ru) ·
[документация](https://docs.tatnet.ru) ·
[справочник API](https://api.tatnet.ru/v1/docs)

Kubernetes [cloud-controller-manager](https://kubernetes.io/docs/concepts/architecture/cloud-controller/)
for **managed TatNet Kubernetes**. It gives a `Service` of `type=LoadBalancer`
an `EXTERNAL-IP` by provisioning a managed TatNet load balancer (phase B of the
managed-LB product) in front of the cluster's NodePorts.

## Scope (deliberately minimal)

The provider implements **only** `cloudprovider.LoadBalancer` and the CCM runs
**only** the service-lb controller (`--controllers=service-lb-controller`).
Consequences:

- No `InstancesV2` → nodes are **not** tainted `node.cloudprovider.kubernetes.io/uninitialized`,
  so kubelet stays on its default (no `--cloud-provider=external` needed).
- The node set is delivered to `EnsureLoadBalancer(..., nodes)` by the
  controller, so the provider needs no node/instance API.

## How a Service maps to a TatNet LB (L7 contract, CCM >=0.2.0)

The CCM writes one declarative document per reconcile via
`PUT /v1/load-balancers/{id}/managed-config` — the endpoint replaces **only**
objects with `managed_by='ccm'`, so listeners/target-groups the user added to
the same LB by hand (e.g. an https listener) always survive. The old flat
`forwarding_rules`/`PUT /backends` model is removed from the API (old bodies
get a 422).

| Kubernetes | TatNet managed-config document |
|---|---|
| Service (type=LoadBalancer) | one managed LB, keyed by `cloudprovider.DefaultLoadBalancerName(service)` |
| each `spec.ports[]` port `p` | listener `{port: p.port, protocol: tcp, default_action: forward → "svc-<p.port>"}` |
| — | target group `svc-<p.port>` `{target_type: ip, protocol: tcp, port: p.nodePort, health_check: {mode: tcp}}` |
| ready node `InternalIP`s | `targets: [{target_ip: ...}]` of every ccm TG |
| `status.loadBalancer.ingress[].ip` | the LB VIP |

Reconcile: `EnsureLoadBalancer` = find-by-name → create (config in the create
body) or `PUT managed-config`; `UpdateLoadBalancer` re-pushes the document on
node changes — but **never with an empty node set** (that would blackhole the
VIP; it logs + events and keeps the previous targets);
`EnsureLoadBalancerDeleted` tears the LB down (idempotent).

## Service annotations (`tatnet.ru/`)

Unknown annotation **values** raise a Warning event on the Service and are
ignored — a typo never wedges the reconcile.

| Annotation | Values | Effect |
|---|---|---|
| `tatnet.ru/lb-node-count` | int `1..3` | LB haproxy node count (create; later via `PATCH` — only when the annotation is set) |
| `tatnet.ru/lb-proxy-protocol` | `v1` \| `v2` | `send_proxy` on every ccm target group |
| `tatnet.ru/lb-health-check-path` | `/path` matching `^/[A-Za-z0-9._~/=&?-]*$` (mirrors the api schema) | `health_check: {mode: http, path}` (httpchk inside the tcp backend) |
| `tatnet.ru/lb-stickiness` | `source_ip` | `stickiness: {type: source_ip}` |

## Port 80 and HTTP-01 certificates

A Service port `80` maps to a `tcp:80` listener and is **legal** — the old
unconditional "port 80 is reserved for ACME" rule is gone from the api. The
conditional rule remains: while an **HTTP-01 certificate is attached to the
LB** (a user can do that by hand on the k8s-mode LB), the api keeps port 80
reserved for ACME renewals and answers `422` to any CCM document carrying a
`:80` listener. The CCM surfaces that as a `ConfigRejected` **Warning event on
the Service** (visible in `kubectl describe svc`) and returns the error, so
the service controller keeps retrying with backoff — but a retry alone cannot
succeed: detach the HTTP-01 certificate (or switch it to DNS-01), or move the
Service off port 80.

## Configuration (env, from the injected Secret)

| Env | Meaning |
|---|---|
| `TATNET_API_BASE` | e.g. `https://api.tatnet.ru` |
| `TATNET_PROJECT_ID` | the cluster's project |
| `TATNET_K8S_CLUSTER_ID` | the managed cluster id (k8s-mode LB target) |
| `TATNET_API_KEY` | project-scoped `tn_live_` key minted for this CCM |
| `TATNET_LB_NODE_COUNT` | default haproxy nodes per LB (1..3, default 2; the `tatnet.ru/lb-node-count` annotation overrides per-Service) |
| `TATNET_LB_READY_TIMEOUT` | how long one reconcile waits for the region to finish building before handing the retry back (default `30s`, `0` = never wait) |
| `TATNET_LB_EXISTENCE_SYNC_PERIOD` | how often to re-check that every published load balancer still exists (default `5m`, `0` disables) |

### Existence: a published address that stops being true is retracted

The service controller is **edge-triggered on the Service**. Its informer
re-delivers every Service on resync, but the handler passes `old == new` to
`needsUpdate`, which compares spec and metadata only — never status — so an
unchanged Service is never re-enqueued and `EnsureLoadBalancer` never runs
again. Cloud state can change while the Service does not: delete the load
balancer out of band and the Service publishes a dead address indefinitely:
nothing re-enqueues the Service, so no recreate is attempted and nothing is
logged.

The existence reconciler is the level-triggered half. Every
`TATNET_LB_EXISTENCE_SYNC_PERIOD` it lists Services that carry an address and
checks the invariant that actually matters — **the address we published is
still the truth**, not merely that some balancer exists. A Service is repaired
when its load balancer is gone, has no address, or answers at a different VIP
than the one published. For each:

1. **clears `status.loadBalancer`** — the truthful half. "No address yet" is a
   state every caller already handles; a dead address is a black hole, since
   connections time out rather than being refused.
2. **stamps `tatnet.ru/lb-resynced-at`** — the wake-up. `needsUpdate` compares
   annotations, and that is the only handle it offers on an object the CCM may
   touch (status is not compared, spec belongs to the owner). The write
   re-queues the Service and the service controller — still the single writer
   of `status.loadBalancer` — rebuilds the balancer and publishes the **new**
   address itself.

A lookup that *failed* is never treated as a balancer that is *missing*:
otherwise one transient API error would retract every live address in the
cluster. Services with no address yet (still building) and Services being
deleted (the finalizer owns those) are skipped.

Two things this deliberately does not add. **A finalizer** — the framework
already sets `service.kubernetes.io/load-balancer-cleanup` itself, so cloud
teardown cannot be outrun by the Service's deletion. **A leader lock** —
`Initialize` is reached only from `OnStartedLeading`, so a standby replica
never starts the loop, and losing the lease cancels it.

⚠ The stamp is a write to the owner's Service, left behind on purpose as the
record that a rebuild happened (the address after one is a new address). A
GitOps controller that reconciles annotations will show drift on it.

### Readiness: the address is published only once it serves

A Service gets `status.loadBalancer.ingress` only after the region reports the
LB as `active` or `degraded` — i.e. once at least one haproxy node is in the
VIP's target set. The VIP itself is allocated synchronously at create, well
before anything answers on it, and publishing it early hands out an
address whose connections **time out rather than being refused** — which reads
downstream as a broken backend, not a balancer that is still coming up.

Until then the reconcile returns an error, so the Service simply carries no
address and the controller retries. `TATNET_LB_READY_TIMEOUT` only trades how
long a single reconcile blocks (the service controller syncs one Service at a
time by default) against how late a ready LB gets published — the address is
withheld across retries either way.

## Deployment

Delivered into the cluster by the **k8s-orchestrator** as a Talos inline
manifest at bootstrap (`deploy/ccm.yaml.tmpl`, with `__PLACEHOLDER__` tokens
substituted per-cluster). Not applied by hand.

## Build

```
go build ./...
go test ./...
docker build -t <registry>/tatnet-cloud-controller-manager:<tag> .
```

## Лицензия

[Apache-2.0](LICENSE).
