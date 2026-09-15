package tatnet

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

// Service annotations (namespace tatnet.ru/) understood by the CCM. An unknown
// VALUE is surfaced as a Warning event on the Service and ignored — a typo must
// not wedge the reconcile loop.
const (
	annNodeCount       = "tatnet.ru/lb-node-count"        // int 1..3 → PATCH LB node_count
	annProxyProtocol   = "tatnet.ru/lb-proxy-protocol"    // v1|v2 → send_proxy on every ccm TG
	annHealthCheckPath = "tatnet.ru/lb-health-check-path" // → health_check {mode:"http", path} (httpchk in a tcp backend — canonical k8s check)
	annStickiness      = "tatnet.ru/lb-stickiness"        // source_ip
)

// publicStatus values of a TatNet LB (api §5.5, projected from lb_state.phase).
const (
	statusProvisioning = "provisioning"
	statusActive       = "active"
	statusDegraded     = "degraded"
	statusError        = "error"
)

// defaultReadyPoll is how often awaitServing re-reads the LB while it builds.
const defaultReadyPoll = 2 * time.Second

// hcPathRe mirrors the api-schema validator for health-check paths exactly —
// the CCM must not accept a value the api will 422. Go's default ^/$ anchor
// to the whole text (\A/\z semantics, no trailing-newline pedal), so this is
// byte-for-byte the api contract.
var hcPathRe = regexp.MustCompile(`^/[A-Za-z0-9._~/=&?-]*$`)

// loadBalancer implements cloudprovider.LoadBalancer over the TatNet /v1 API.
// One Service of type=LoadBalancer maps to one managed TatNet LB, keyed by the
// controller-derived name (stable across reconciles). Each Service port maps to
// a tcp listener + target group "svc-<port>" whose targets are the cluster node
// InternalIPs at the port's NodePort; the whole document is written via
// PUT .../managed-config (only managed_by='ccm' objects are replaced, so
// user-added listeners/TGs on the same LB survive).
type loadBalancer struct {
	client       *Client
	k8sClusterID string
	nodeCount    int                  // default when the node-count annotation is absent
	recorder     record.EventRecorder // set in provider.Initialize; may be nil (log-only)
	// How long one EnsureLoadBalancer call waits for the region to finish
	// building before handing the retry back to the controller, and how often
	// it re-reads while waiting. Zero readyWait = check once (the test
	// default); production wires it in newProvider.
	readyWait time.Duration
	readyPoll time.Duration
}

// GetLoadBalancerName is the stable Service<->LB key. TatNet LB names accept the
// controller's default ("a"+uid, <=32 chars), so reuse it verbatim.
func (l *loadBalancer) GetLoadBalancerName(_ context.Context, clusterName string, service *v1.Service) string {
	return cloudprovider.DefaultLoadBalancerName(service)
}

func (l *loadBalancer) GetLoadBalancer(ctx context.Context, clusterName string, service *v1.Service) (*v1.LoadBalancerStatus, bool, error) {
	name := l.GetLoadBalancerName(ctx, clusterName, service)
	lb, err := l.client.FindByName(ctx, name)
	if err != nil {
		return nil, false, err
	}
	if lb == nil {
		return nil, false, nil
	}
	return statusFor(lb), true, nil
}

func (l *loadBalancer) EnsureLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (*v1.LoadBalancerStatus, error) {
	name := l.GetLoadBalancerName(ctx, clusterName, service)
	ips := nodeIPs(nodes)
	if len(ips) == 0 {
		return nil, fmt.Errorf("no schedulable node with an InternalIP for service %s/%s", service.Namespace, service.Name)
	}
	ann := l.parseAnnotations(service)
	cfg, err := configFor(service, ips, ann)
	if err != nil {
		return nil, err
	}

	lb, err := l.client.FindByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if lb == nil {
		nodeCount := l.nodeCount
		if ann.nodeCount != 0 {
			nodeCount = ann.nodeCount
		}
		lb, err = l.client.Create(ctx, name, nodeCount, cfg, l.k8sClusterID)
		if err != nil {
			l.eventOnRejected(service, err)
			return nil, fmt.Errorf("create LB: %w", err)
		}
	} else {
		if err := l.client.PutManagedConfig(ctx, lb.ID, cfg); err != nil {
			l.eventOnRejected(service, err)
			return nil, fmt.Errorf("put managed config: %w", err)
		}
		// node_count is only touched when the annotation asks for it — the
		// CCM default must not fight a count set from the panel.
		if ann.nodeCount != 0 && ann.nodeCount != lb.NodeCount {
			if err := l.client.PatchNodeCount(ctx, lb.ID, ann.nodeCount); err != nil {
				return nil, fmt.Errorf("patch node_count: %w", err)
			}
		}
	}

	// Publishing the VIP is what makes the Service read as ready — and
	// everything downstream believes it: CI waits, ingress controllers,
	// cert-manager's HTTP-01. The VIP is allocated synchronously at create,
	// but nothing listens on it until the region has built the haproxy nodes,
	// so publishing at allocation time hands out an address that BLACK-HOLES:
	// connections time out rather than being refused, which reads as a broken
	// backend, not a missing balancer. Measured on a fresh cluster: VIP at
	// 2 s, first byte served at 98 s — 96 seconds of "ready" that lied.
	fresh, err := l.awaitServing(ctx, lb.ID)
	if err != nil {
		return nil, err
	}
	return statusFor(fresh), nil
}

// serving reports whether the region says the VIP can take traffic.
//
// lb_state.phase is the region's own answer to "can a node serve yet":
// 'provisioning' is set exactly while no haproxy is in the VIP's target set.
// 'degraded' publishes too — it is a serving LB with a failed peer or a
// skipped rule, and withholding the address there would turn a partial
// outage into a total one.
func serving(lb *LB) bool {
	return lb.VIP != "" && (lb.Status == statusActive || lb.Status == statusDegraded)
}

// awaitServing polls until the LB actually serves, and returns an error rather
// than a status while it does not — the service controller only patches the
// Service from what EnsureLoadBalancer returns, so an error keeps the address
// unpublished instead of publishing a hole.
//
// The wait is bounded on both sides deliberately. Without any wait, the
// controller's retry ladder is exponential (5s, 10s, 20s, 40s, 80s…), so a
// ~90 s build publishes minutes after it was actually reachable. Without a
// bound, we would hold the worker for the whole build — the service controller
// syncs one Service at a time by default, so every other Service would queue
// behind this one. Waiting a little and then handing the retry back keeps both
// costs small.
func (l *loadBalancer) awaitServing(ctx context.Context, id string) (*LB, error) {
	poll := l.readyPoll
	if poll <= 0 {
		poll = defaultReadyPoll
	}
	deadline := time.Now().Add(l.readyWait)
	for {
		fresh, err := l.client.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if serving(fresh) {
			return fresh, nil
		}
		if fresh.Status == statusError {
			// Not retried away by waiting: surface the region's own reason.
			detail := fresh.StatusDetail
			if detail == "" {
				detail = "no detail reported"
			}
			return nil, fmt.Errorf("LB %s failed to build: %s", id, detail)
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf(
				"LB %s is not serving yet (phase %q); not publishing its address until it is",
				id, fresh.Status)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func (l *loadBalancer) UpdateLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) error {
	name := l.GetLoadBalancerName(ctx, clusterName, service)
	ips := nodeIPs(nodes)
	if len(ips) == 0 {
		// Guard: an empty node set would wipe every ccm target and blackhole
		// the VIP (this exact hole shipped with the flat-model CCM). Keep the
		// last good target set; the controller re-syncs once nodes are back.
		l.warnf(service, "EmptyNodeSet",
			"refusing to push an empty backend set to LB %q; keeping previous targets", name)
		return nil
	}
	ann := l.parseAnnotations(service)
	cfg, err := configFor(service, ips, ann)
	if err != nil {
		return err
	}
	lb, err := l.client.FindByName(ctx, name)
	if err != nil {
		return err
	}
	if lb == nil {
		return fmt.Errorf("load balancer %q not found for update", name)
	}
	if err := l.client.PutManagedConfig(ctx, lb.ID, cfg); err != nil {
		l.eventOnRejected(service, err)
		return err
	}
	return nil
}

func (l *loadBalancer) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *v1.Service) error {
	name := l.GetLoadBalancerName(ctx, clusterName, service)
	lb, err := l.client.FindByName(ctx, name)
	if err != nil {
		return err
	}
	if lb == nil {
		return nil
	}
	return l.client.Delete(ctx, lb.ID)
}

// lbAnnotations is the validated view of the tatnet.ru/* Service annotations.
// Zero values mean "annotation absent (or invalid and ignored)".
type lbAnnotations struct {
	nodeCount  int    // 0 = absent
	sendProxy  string // "", "v1", "v2"
	hcPath     string // "" = plain tcp health check
	stickiness string // "", "source_ip"
}

// parseAnnotations validates the Service annotations. Invalid values raise a
// Warning event and are ignored (the reconcile proceeds with defaults) —
// failing here would wedge the Service on a typo.
func (l *loadBalancer) parseAnnotations(service *v1.Service) lbAnnotations {
	var a lbAnnotations
	if raw, ok := service.Annotations[annNodeCount]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 3 {
			l.warnf(service, "InvalidAnnotation", "%s=%q: must be an integer 1..3; ignoring", annNodeCount, raw)
		} else {
			a.nodeCount = n
		}
	}
	if raw, ok := service.Annotations[annProxyProtocol]; ok {
		switch raw {
		case "v1", "v2":
			a.sendProxy = raw
		default:
			l.warnf(service, "InvalidAnnotation", "%s=%q: must be v1 or v2; ignoring", annProxyProtocol, raw)
		}
	}
	if raw, ok := service.Annotations[annHealthCheckPath]; ok {
		if !hcPathRe.MatchString(raw) {
			l.warnf(service, "InvalidAnnotation", "%s=%q: must match %s (absolute path, api schema); ignoring", annHealthCheckPath, raw, hcPathRe)
		} else {
			a.hcPath = raw
		}
	}
	if raw, ok := service.Annotations[annStickiness]; ok {
		if raw == "source_ip" {
			a.stickiness = raw
		} else {
			l.warnf(service, "InvalidAnnotation", "%s=%q: only source_ip is supported; ignoring", annStickiness, raw)
		}
	}
	return a
}

// eventOnRejected surfaces a 422 as a Warning event on the Service, on top of
// the returned error. A 422 means the api refused the desired config, so a
// retry alone cannot fix it — the owner has to act. The canonical case: the
// Service asks for port 80 while the user manually attached an HTTP-01
// certificate to this k8s LB (tcp:80 is otherwise legal; the api reserves it
// for ACME renewals only while such certs are bound). A bare error log in the
// CCM pod is invisible to the Service owner; the event is not.
func (l *loadBalancer) eventOnRejected(service *v1.Service, err error) {
	if IsUnprocessable(err) {
		l.warnf(service, "ConfigRejected",
			"tatnet api rejected the LB config (e.g. port 80 while an HTTP-01 certificate is attached to the LB; a retry alone cannot succeed): %v", err)
	}
}

// warnf logs and (when a recorder is wired) emits a Warning event on the Service.
func (l *loadBalancer) warnf(service *v1.Service, reason, format string, args ...any) {
	klog.Warningf("service %s/%s: "+format, append([]any{service.Namespace, service.Name}, args...)...)
	if l.recorder != nil {
		l.recorder.Eventf(service, v1.EventTypeWarning, reason, format, args...)
	}
}

// configFor maps a Service + node IPs to the managed-config document (design
// §5.7): each Service port p → tcp listener :p.Port forwarding to target group
// "svc-<p.Port>" (target_type ip, port p.NodePort, targets = node IPs).
func configFor(service *v1.Service, ips []string, ann lbAnnotations) (LBConfig, error) {
	cfg := LBConfig{TargetGroups: []TargetGroup{}, Listeners: []Listener{}}
	for _, p := range service.Spec.Ports {
		if p.NodePort == 0 {
			// Not yet allocated — skip; the controller re-syncs.
			continue
		}
		if p.Protocol != "" && p.Protocol != v1.ProtocolTCP {
			return LBConfig{}, fmt.Errorf("service %s/%s port %d: only TCP is supported (got %s)",
				service.Namespace, service.Name, p.Port, p.Protocol)
		}
		tgName := fmt.Sprintf("svc-%d", p.Port)
		hc := &HealthCheck{Mode: "tcp"}
		if ann.hcPath != "" {
			hc = &HealthCheck{Mode: "http", Path: ann.hcPath}
		}
		tg := TargetGroup{
			Name:        tgName,
			TargetType:  "ip",
			Protocol:    "tcp",
			Port:        int(p.NodePort),
			HealthCheck: hc,
			SendProxy:   ann.sendProxy,
			Targets:     make([]Target, 0, len(ips)),
		}
		if ann.stickiness != "" {
			tg.Stickiness = &Stickiness{Type: ann.stickiness}
		}
		for _, ip := range ips {
			tg.Targets = append(tg.Targets, Target{TargetIP: ip})
		}
		cfg.TargetGroups = append(cfg.TargetGroups, tg)
		cfg.Listeners = append(cfg.Listeners, Listener{
			Port:     int(p.Port),
			Protocol: "tcp",
			HTTP2:    false, // explicit: tcp listeners must not carry http2 (§5.4.5)
			DefaultAction: RuleAction{
				Type:        "forward",
				TargetGroup: tgName,
			},
		})
	}
	if len(cfg.Listeners) == 0 {
		return LBConfig{}, fmt.Errorf("service %s/%s has no TCP port with a NodePort", service.Namespace, service.Name)
	}
	return cfg, nil
}

// nodeIPs collects each node's InternalIP, sorted for a stable reconcile.
func nodeIPs(nodes []*v1.Node) []string {
	var ips []string
	for _, n := range nodes {
		for _, a := range n.Status.Addresses {
			if a.Type == v1.NodeInternalIP && a.Address != "" {
				ips = append(ips, a.Address)
				break
			}
		}
	}
	sort.Strings(ips)
	return ips
}

func statusFor(lb *LB) *v1.LoadBalancerStatus {
	return &v1.LoadBalancerStatus{
		Ingress: []v1.LoadBalancerIngress{{IP: lb.VIP}},
	}
}
