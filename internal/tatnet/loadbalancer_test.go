package tatnet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func svc(ports ...v1.ServicePort) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web"},
		Spec:       v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer, Ports: ports},
	}
}

func annotated(s *v1.Service, kv map[string]string) *v1.Service {
	s.Annotations = kv
	return s
}

func node(ip string) *v1.Node {
	return &v1.Node{Status: v1.NodeStatus{Addresses: []v1.NodeAddress{
		{Type: v1.NodeHostName, Address: "h"},
		{Type: v1.NodeInternalIP, Address: ip},
	}}}
}

// ---------------------------------------------------------------------------
// Service → document mapping
// ---------------------------------------------------------------------------

func TestConfigFor_MapsPortsToDocument(t *testing.T) {
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP},
		v1.ServicePort{Port: 443, NodePort: 31001})
	cfg, err := configFor(s, []string{"10.7.0.2", "10.7.0.9"}, lbAnnotations{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Listeners) != 2 || len(cfg.TargetGroups) != 2 {
		t.Fatalf("want 2 listeners + 2 TGs, got %+v", cfg)
	}
	ln := cfg.Listeners[0]
	if ln.Port != 80 || ln.Protocol != "tcp" || ln.HTTP2 ||
		ln.DefaultAction.Type != "forward" || ln.DefaultAction.TargetGroup != "svc-80" {
		t.Fatalf("bad listener: %+v", ln)
	}
	if cfg.Listeners[1].DefaultAction.TargetGroup != "svc-443" {
		t.Fatalf("bad second listener: %+v", cfg.Listeners[1])
	}
	tg := cfg.TargetGroups[0]
	if tg.Name != "svc-80" || tg.TargetType != "ip" || tg.Protocol != "tcp" || tg.Port != 31000 {
		t.Fatalf("bad TG: %+v", tg)
	}
	if tg.HealthCheck == nil || tg.HealthCheck.Mode != "tcp" || tg.HealthCheck.Path != "" {
		t.Fatalf("default health check must be plain tcp: %+v", tg.HealthCheck)
	}
	if tg.SendProxy != "" || tg.Stickiness != nil {
		t.Fatalf("no annotations → no send_proxy/stickiness: %+v", tg)
	}
	want := []Target{{TargetIP: "10.7.0.2"}, {TargetIP: "10.7.0.9"}}
	if !reflect.DeepEqual(tg.Targets, want) {
		t.Fatalf("targets: got %+v want %+v", tg.Targets, want)
	}
	if cfg.TargetGroups[1].Port != 31001 {
		t.Fatalf("second TG must use its own NodePort: %+v", cfg.TargetGroups[1])
	}
}

func TestConfigFor_SkipsUnallocatedNodePort(t *testing.T) {
	// All ports unallocated → error (nothing to program yet).
	s := svc(v1.ServicePort{Port: 80, NodePort: 0})
	if _, err := configFor(s, []string{"10.7.0.2"}, lbAnnotations{}); err == nil {
		t.Fatal("expected error when no NodePort is allocated")
	}
	// A mix: the unallocated port is skipped, the allocated one is kept.
	s = svc(v1.ServicePort{Port: 80, NodePort: 0}, v1.ServicePort{Port: 443, NodePort: 31001})
	cfg, err := configFor(s, []string{"10.7.0.2"}, lbAnnotations{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Listeners) != 1 || cfg.Listeners[0].Port != 443 {
		t.Fatalf("want only the allocated port, got %+v", cfg.Listeners)
	}
}

func TestConfigFor_RejectsUDP(t *testing.T) {
	s := svc(v1.ServicePort{Port: 53, NodePort: 31000, Protocol: v1.ProtocolUDP})
	if _, err := configFor(s, []string{"10.7.0.2"}, lbAnnotations{}); err == nil {
		t.Fatal("expected UDP to be rejected")
	}
}

func TestConfigFor_Annotations(t *testing.T) {
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP},
		v1.ServicePort{Port: 443, NodePort: 31001, Protocol: v1.ProtocolTCP})
	cfg, err := configFor(s, []string{"10.7.0.2"}, lbAnnotations{
		sendProxy: "v2", hcPath: "/healthz", stickiness: "source_ip",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tg := range cfg.TargetGroups {
		if tg.SendProxy != "v2" {
			t.Fatalf("proxy-protocol annotation must set send_proxy on every ccm TG: %+v", tg)
		}
		if tg.HealthCheck == nil || tg.HealthCheck.Mode != "http" || tg.HealthCheck.Path != "/healthz" {
			t.Fatalf("hc path annotation must switch the check to http: %+v", tg.HealthCheck)
		}
		if tg.Stickiness == nil || tg.Stickiness.Type != "source_ip" {
			t.Fatalf("stickiness annotation must set source_ip: %+v", tg.Stickiness)
		}
	}
}

func TestParseAnnotations_Valid(t *testing.T) {
	l := &loadBalancer{}
	s := annotated(svc(), map[string]string{
		annNodeCount:       "3",
		annProxyProtocol:   "v1",
		annHealthCheckPath: "/healthz",
		annStickiness:      "source_ip",
	})
	got := l.parseAnnotations(s)
	want := lbAnnotations{nodeCount: 3, sendProxy: "v1", hcPath: "/healthz", stickiness: "source_ip"}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestParseAnnotations_InvalidValuesEventAndIgnore(t *testing.T) {
	rec := record.NewFakeRecorder(16)
	l := &loadBalancer{recorder: rec}
	s := annotated(svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP}),
		map[string]string{
			annNodeCount:       "7",       // out of 1..3
			annProxyProtocol:   "v3",      // unknown version
			annHealthCheckPath: "healthz", // not absolute
			annStickiness:      "cookie",  // unsupported
		})
	got := l.parseAnnotations(s)
	if got != (lbAnnotations{}) {
		t.Fatalf("invalid values must be ignored, got %+v", got)
	}
	if n := len(rec.Events); n != 4 {
		t.Fatalf("want 4 warning events, got %d", n)
	}
	for i := 0; i < 4; i++ {
		e := <-rec.Events
		if !strings.Contains(e, "InvalidAnnotation") || !strings.Contains(e, "Warning") {
			t.Fatalf("bad event: %q", e)
		}
	}
	// And the reconcile still proceeds: the document builds with defaults.
	if _, err := configFor(s, []string{"10.7.0.2"}, got); err != nil {
		t.Fatalf("invalid annotations must not fail the document build: %v", err)
	}
}

// TestDocumentJSON pins the wire form of the managed-config document — the api
// is implemented against exactly this contract (snake_case, §5.1/§5.7).
func TestDocumentJSON(t *testing.T) {
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	cfg, err := configFor(s, []string{"10.7.0.5"}, lbAnnotations{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"target_groups":[{"name":"svc-80","target_type":"ip","protocol":"tcp","port":31000,` +
		`"health_check":{"mode":"tcp"},"targets":[{"target_ip":"10.7.0.5"}]}],` +
		`"listeners":[{"port":80,"protocol":"tcp","http2":false,` +
		`"default_action":{"type":"forward","target_group":"svc-80"}}]}`
	if string(b) != want {
		t.Fatalf("document wire form drifted:\n got %s\nwant %s", b, want)
	}
}

func TestNodeIPs_SortedInternalOnly(t *testing.T) {
	got := nodeIPs([]*v1.Node{node("10.7.0.9"), node("10.7.0.2"), {}})
	want := []string{"10.7.0.2", "10.7.0.9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// fakeAPI is a minimal in-memory /v1 LB backend for the flow tests: paginated
// list, create-with-config, managed-config PUT, node_count PATCH, delete.
// ---------------------------------------------------------------------------

type fakeAPI struct {
	mu             sync.Mutex
	lbs            map[string]*LB
	configs        map[string]LBConfig
	nextID         int
	listCalls      int
	putConfigCalls int
	patchedCounts  []int
	reject422      string // non-empty → POST create / PUT managed-config answer 422 with this body
	getCalls       int
	// The region builds asynchronously: a fresh LB answers 'provisioning'
	// until a haproxy node can serve. serveAfterGets is how many GETs still
	// report that (0 = serving on the first read); forceStatus overrides the
	// phase outright, for the degraded/error arms.
	serveAfterGets    int
	forceStatus       string
	forceStatusDetail string
	failList          bool // GET list answers 500 — an unreadable API, not an empty one
}

func (f *fakeAPI) sorted() []LB {
	items := make([]LB, 0, len(f.lbs))
	for _, lb := range f.lbs {
		items = append(items, *lb)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items
}

func newFakeAPI() (*httptest.Server, *fakeAPI) {
	f := &fakeAPI{lbs: map[string]*LB{}, configs: map[string]LBConfig{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/p/load-balancers", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			if f.failList {
				w.WriteHeader(500)
				return
			}
			f.listCalls++
			all := f.sorted()
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if limit <= 0 || limit > 200 {
				limit = 200 // the real /v1 caps the page size
			}
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			pageEnd := min(offset+limit, len(all))
			items := []LB{}
			if offset < len(all) {
				items = all[offset:pageEnd]
			}
			// The real /v1 paginated envelope uses "data" (not "items") — the
			// fake must match so FindByName is actually exercised. And the
			// real "count" is the SIZE OF THE PAGE (len(data), api/_common.py),
			// NOT the collection total: the fake mirrors that so a
			// count-based termination bug cannot pass the tests again.
			wired := make([]map[string]any, 0, len(items))
			for _, it := range items {
				it := it
				wired = append(wired, wire(&it))
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": wired, "count": len(wired)})
		case http.MethodPost:
			if f.reject422 != "" {
				w.WriteHeader(422)
				_, _ = w.Write([]byte(f.reject422))
				return
			}
			var req createReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			f.nextID++
			id := fmt.Sprintf("lb-%d", f.nextID)
			lb := &LB{ID: id, Name: req.Name, Mode: "k8s",
				NodeCount: req.NodeCount, VIP: "185.1.1.1", Status: "provisioning"}
			f.lbs[id] = lb
			f.configs[id] = req.Config
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(201)
			_ = json.NewEncoder(w).Encode(wire(lb))
		}
	})
	mux.HandleFunc("/v1/projects/p/load-balancers/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		// /v1/projects/p/load-balancers/{id}[/managed-config]
		rest := strings.TrimPrefix(r.URL.Path, "/v1/projects/p/load-balancers/")
		id, sub, _ := strings.Cut(rest, "/")
		lb, ok := f.lbs[id]
		if !ok {
			w.WriteHeader(404)
			return
		}
		switch {
		case sub == "managed-config" && r.Method == http.MethodPut:
			if f.reject422 != "" {
				w.WriteHeader(422)
				_, _ = w.Write([]byte(f.reject422))
				return
			}
			var cfg LBConfig
			_ = json.NewDecoder(r.Body).Decode(&cfg)
			f.configs[id] = cfg
			f.putConfigCalls++
			w.WriteHeader(200)
		case sub == "" && r.Method == http.MethodGet:
			f.getCalls++
			if lb.Status == statusProvisioning && f.getCalls > f.serveAfterGets {
				lb.Status = statusActive
			}
			if f.forceStatus != "" {
				lb.Status, lb.StatusDetail = f.forceStatus, f.forceStatusDetail
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(wire(lb))
		case sub == "" && r.Method == http.MethodPatch:
			var req struct {
				NodeCount *int `json:"node_count"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.NodeCount != nil {
				lb.NodeCount = *req.NodeCount
				f.patchedCounts = append(f.patchedCounts, *req.NodeCount)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(wire(lb))
		case sub == "" && r.Method == http.MethodDelete:
			delete(f.lbs, id)
			delete(f.configs, id)
			w.WriteHeader(202)
		default:
			w.WriteHeader(405)
		}
	})
	return httptest.NewServer(mux), f
}

// TestFindByName_Paginates: an LB beyond the first page MUST be found. The
// fake serves the REAL envelope semantics (count = page size, not total), so
// any count-based termination — which is always true after a full first page —
// fails this test instead of slipping through against an idealized fake.
func TestFindByName_Paginates(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	// 250 LBs > the 200-per-page cap; the target sorts onto the second page.
	for i := 0; i < 250; i++ {
		name := fmt.Sprintf("lb-%03d", i)
		api.lbs[name] = &LB{ID: name, Name: name}
	}
	c := mustClient(t, srv.URL)
	lb, err := c.FindByName(context.Background(), "lb-249")
	if err != nil {
		t.Fatal(err)
	}
	if lb == nil || lb.ID != "lb-249" {
		t.Fatalf("expected to find lb-249 on the second page, got %+v", lb)
	}
	if api.listCalls < 2 {
		t.Fatalf("expected FindByName to page (>=2 list calls), got %d", api.listCalls)
	}
	// Miss: pages through everything and returns nil without error.
	api.listCalls = 0
	lb, err = c.FindByName(context.Background(), "nope")
	if err != nil || lb != nil {
		t.Fatalf("miss must be (nil, nil), got %+v %v", lb, err)
	}
	if api.listCalls != 2 {
		t.Fatalf("miss must still page through all %d LBs (2 calls), got %d", len(api.lbs), api.listCalls)
	}
}

// Exact multiple of the page cap: every page is full, so the loop only learns
// the collection is over from the trailing empty page — and must terminate on
// it rather than spin.
func TestFindByName_ExactPageMultipleTerminates(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	for i := 0; i < 2*pageLimit; i++ {
		name := fmt.Sprintf("lb-%03d", i)
		api.lbs[name] = &LB{ID: name, Name: name}
	}
	c := mustClient(t, srv.URL)
	// Hit on the very last item of the last full page.
	last := fmt.Sprintf("lb-%03d", 2*pageLimit-1)
	lb, err := c.FindByName(context.Background(), last)
	if err != nil || lb == nil || lb.ID != last {
		t.Fatalf("expected to find %s, got %+v %v", last, lb, err)
	}
	if api.listCalls != 2 {
		t.Fatalf("hit on page 2 must take exactly 2 list calls, got %d", api.listCalls)
	}
	// Miss: 2 full pages + 1 empty page, then stop.
	api.listCalls = 0
	lb, err = c.FindByName(context.Background(), "nope")
	if err != nil || lb != nil {
		t.Fatalf("miss must be (nil, nil), got %+v %v", lb, err)
	}
	if api.listCalls != 3 {
		t.Fatalf("miss over an exact multiple must take 3 list calls (last one empty), got %d", api.listCalls)
	}
}

func TestEnsureAndDeleteFlow(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2}
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	ctx := context.Background()

	// First ensure creates the LB (config in the create body) and returns the VIP.
	st, err := lb.EnsureLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.5")})
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Ingress) != 1 || st.Ingress[0].IP != "185.1.1.1" {
		t.Fatalf("bad status: %+v", st)
	}
	if api.putConfigCalls != 0 {
		t.Fatalf("create path must not also PUT managed-config, got %d PUTs", api.putConfigCalls)
	}
	cfg := api.configs["lb-1"]
	if len(cfg.Listeners) != 1 || cfg.Listeners[0].Port != 80 ||
		len(cfg.TargetGroups) != 1 || cfg.TargetGroups[0].Port != 31000 {
		t.Fatalf("create body must carry the document: %+v", cfg)
	}
	// GetLoadBalancer now finds it.
	_, exists, err := lb.GetLoadBalancer(ctx, "cl", s)
	if err != nil || !exists {
		t.Fatalf("expected LB to exist, err=%v", err)
	}
	// Second ensure must find the existing LB (FindByName over the real "data"
	// envelope), NOT create a duplicate, and refresh via PUT managed-config.
	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.5"), node("10.7.0.6")}); err != nil {
		t.Fatal(err)
	}
	if len(api.lbs) != 1 {
		t.Fatalf("re-ensure created a duplicate: %d LBs in store", len(api.lbs))
	}
	if api.putConfigCalls != 1 {
		t.Fatalf("re-ensure must go through PUT managed-config, got %d PUTs", api.putConfigCalls)
	}
	if got := api.configs["lb-1"].TargetGroups[0].Targets; len(got) != 2 {
		t.Fatalf("re-ensure must push both node IPs: %+v", got)
	}
	// No node-count annotation → node_count untouched.
	if len(api.patchedCounts) != 0 {
		t.Fatalf("node_count must not be patched without the annotation: %v", api.patchedCounts)
	}
	// Delete actually removes it from the backend (not just a no-op).
	if err := lb.EnsureLoadBalancerDeleted(ctx, "cl", s); err != nil {
		t.Fatal(err)
	}
	if len(api.lbs) != 0 {
		t.Fatalf("delete leaked the LB: %d still in store", len(api.lbs))
	}
	// A second delete is a no-op (nothing to find).
	if err := lb.EnsureLoadBalancerDeleted(ctx, "cl", s); err != nil {
		t.Fatalf("delete must be idempotent: %v", err)
	}
}

func TestEnsure_NodeCountAnnotationPatches(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2}
	ctx := context.Background()
	nodes := []*v1.Node{node("10.7.0.5")}

	// Create honors the annotation.
	s := annotated(svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP}),
		map[string]string{annNodeCount: "3"})
	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, nodes); err != nil {
		t.Fatal(err)
	}
	if api.lbs["lb-1"].NodeCount != 3 {
		t.Fatalf("create must honor the node-count annotation: %+v", api.lbs["lb-1"])
	}
	// Same value again → no PATCH.
	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, nodes); err != nil {
		t.Fatal(err)
	}
	if len(api.patchedCounts) != 0 {
		t.Fatalf("unchanged annotation must not PATCH: %v", api.patchedCounts)
	}
	// Changed value → exactly one PATCH.
	s.Annotations[annNodeCount] = "1"
	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, nodes); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(api.patchedCounts, []int{1}) {
		t.Fatalf("changed annotation must PATCH once: %v", api.patchedCounts)
	}
}

func TestUpdate_EmptyNodeSetGuard(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	rec := record.NewFakeRecorder(4)
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2, recorder: rec}
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	ctx := context.Background()

	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.5")}); err != nil {
		t.Fatal(err)
	}
	before := api.configs["lb-1"]

	// An empty node set must NOT be pushed (it would blackhole the VIP):
	// log + event + succeed, keeping the previous targets.
	if err := lb.UpdateLoadBalancer(ctx, "cl", s, nil); err != nil {
		t.Fatalf("empty node set must not fail the sync: %v", err)
	}
	if api.putConfigCalls != 0 {
		t.Fatalf("empty node set must not reach the API, got %d PUTs", api.putConfigCalls)
	}
	if !reflect.DeepEqual(api.configs["lb-1"], before) {
		t.Fatal("stored config changed on empty node set")
	}
	if len(rec.Events) != 1 || !strings.Contains(<-rec.Events, "EmptyNodeSet") {
		t.Fatal("expected an EmptyNodeSet warning event")
	}

	// A real node change goes through as a managed-config PUT.
	if err := lb.UpdateLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.6")}); err != nil {
		t.Fatal(err)
	}
	if api.putConfigCalls != 1 {
		t.Fatalf("node change must PUT managed-config, got %d", api.putConfigCalls)
	}
	if got := api.configs["lb-1"].TargetGroups[0].Targets; len(got) != 1 || got[0].TargetIP != "10.7.0.6" {
		t.Fatalf("update must push the new node set: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// 422 (config rejected) → Warning event on the Service. tcp:80 is legal in
// general — the api answers 422 only while an HTTP-01 certificate is attached
// to the LB, and a bare error log in the CCM pod is invisible to the Service
// owner.
// ---------------------------------------------------------------------------

const reject80Body = `{"detail":"port 80 is reserved for ACME HTTP-01 renewals while a certificate is attached to this load balancer"}`

func expectConfigRejectedEvent(t *testing.T, rec *record.FakeRecorder) {
	t.Helper()
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "Warning") || !strings.Contains(e, "ConfigRejected") || !strings.Contains(e, "port 80") {
			t.Fatalf("bad event: %q", e)
		}
	default:
		t.Fatal("expected a ConfigRejected warning event on the Service")
	}
}

func TestEnsure_Port80IsLegal(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2}
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	if _, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")}); err != nil {
		t.Fatalf("tcp:80 must map and be accepted when no HTTP-01 cert is attached: %v", err)
	}
	if got := api.configs["lb-1"].Listeners; len(got) != 1 || got[0].Port != 80 || got[0].Protocol != "tcp" {
		t.Fatalf("Service port 80 must map to a tcp:80 listener: %+v", got)
	}
}

func TestEnsure_422OnPutEmitsWarningEvent(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	rec := record.NewFakeRecorder(8)
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2, recorder: rec}
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	ctx := context.Background()

	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.5")}); err != nil {
		t.Fatal(err)
	}
	// The user attaches an HTTP-01 cert to the LB by hand → the api starts
	// rejecting the :80 document with 422.
	api.reject422 = reject80Body
	_, err := lb.EnsureLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.5")})
	if err == nil {
		t.Fatal("422 must still surface as an error (the controller owns retry/backoff)")
	}
	if !IsUnprocessable(err) {
		t.Fatalf("wrapped 422 must be detectable via IsUnprocessable: %v", err)
	}
	expectConfigRejectedEvent(t, rec)
}

func TestEnsure_422OnCreateEmitsWarningEvent(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	rec := record.NewFakeRecorder(8)
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2, recorder: rec}
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})

	api.reject422 = reject80Body
	if _, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")}); err == nil {
		t.Fatal("422 on create must surface as an error")
	}
	expectConfigRejectedEvent(t, rec)
}

func TestUpdate_422EmitsWarningEvent(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	rec := record.NewFakeRecorder(8)
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2, recorder: rec}
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	ctx := context.Background()

	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.5")}); err != nil {
		t.Fatal(err)
	}
	api.reject422 = reject80Body
	if err := lb.UpdateLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.6")}); err == nil {
		t.Fatal("422 on update must surface as an error")
	}
	expectConfigRejectedEvent(t, rec)
}

// Non-422 errors must NOT produce the ConfigRejected event — it is reserved
// for "the api refused this config, act on it".
func TestEnsure_Non422ErrorNoRejectEvent(t *testing.T) {
	rec := record.NewFakeRecorder(8)
	lb := &loadBalancer{client: mustClient(t, "http://127.0.0.1:0"), k8sClusterID: "c", nodeCount: 2, recorder: rec}
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	if _, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")}); err == nil {
		t.Fatal("expected a transport error")
	}
	select {
	case e := <-rec.Events:
		t.Fatalf("no event expected on a transport error, got %q", e)
	default:
	}
}

// ---------------------------------------------------------------------------
// hc-path annotation mirrors the api-schema regex ^/[A-Za-z0-9._~/=&?-]*$
// (Go ^/$ = \A/\z: no trailing-newline leniency). The CCM must not accept a
// value the api will 422, and must not reject one the api allows.
// ---------------------------------------------------------------------------

func TestParseAnnotations_HealthCheckPathMirrorsAPISchema(t *testing.T) {
	valid := []string{
		"/",
		"/healthz",
		"/api/v1/health",
		"/deep-check_v2.x~y",
		"/healthz?deep=1&scope=all",
	}
	for _, p := range valid {
		rec := record.NewFakeRecorder(4)
		l := &loadBalancer{recorder: rec}
		got := l.parseAnnotations(annotated(svc(), map[string]string{annHealthCheckPath: p}))
		if got.hcPath != p {
			t.Errorf("valid path %q rejected (got %+v)", p, got)
		}
		select {
		case e := <-rec.Events:
			t.Errorf("valid path %q raised event %q", p, e)
		default:
		}
	}
	invalid := []string{
		"",              // empty
		"healthz",       // not absolute
		"/health check", // space
		"/healthz\n",    // trailing newline must NOT slip through the anchors
		"/здоровье",     // non-ASCII
		"/health%20z",   // % is not in the api character class
		"/heal#z",       // fragment char
	}
	for _, p := range invalid {
		rec := record.NewFakeRecorder(4)
		l := &loadBalancer{recorder: rec}
		got := l.parseAnnotations(annotated(svc(), map[string]string{annHealthCheckPath: p}))
		if got.hcPath != "" {
			t.Errorf("invalid path %q accepted as %q", p, got.hcPath)
		}
		select {
		case e := <-rec.Events:
			if !strings.Contains(e, "InvalidAnnotation") || !strings.Contains(e, "Warning") {
				t.Errorf("invalid path %q: bad event %q", p, e)
			}
		default:
			t.Errorf("invalid path %q must raise a warning event", p)
		}
	}
}

// ---------------------------------------------------------------------------
// Readiness gate: the address is published only once the region serves it
//
// Publishing at VIP-allocation time handed out an address that black-holed for
// the whole build (measured: VIP at 2 s, first byte at 98 s). The Service must
// carry no address at all until the region says a node can serve — "not ready
// yet" is a state callers handle; "ready, but every connection times out" is
// not.
// ---------------------------------------------------------------------------

func ensureLB(t *testing.T, srv *httptest.Server) *loadBalancer {
	t.Helper()
	return &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2}
}

func TestEnsure_WithholdsAddressWhileProvisioning(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	api.serveAfterGets = 1 << 30 // the region never finishes within this test
	lb := ensureLB(t, srv)
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})

	st, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")})
	if err == nil {
		t.Fatal("a provisioning LB must not publish its address")
	}
	if st != nil {
		t.Fatalf("nothing may be published while provisioning, got %+v", st)
	}
	if !strings.Contains(err.Error(), statusProvisioning) {
		t.Fatalf("the error must name the phase the caller is waiting on: %v", err)
	}
	// The LB itself was still created — withholding the address is not
	// withholding the build.
	if len(api.lbs) != 1 {
		t.Fatalf("the LB must exist even though its address is unpublished: %d", len(api.lbs))
	}
}

func TestEnsure_PublishesOnceRegionServes(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	api.serveAfterGets = 2 // two reads still building, the third serves
	lb := ensureLB(t, srv)
	lb.readyWait, lb.readyPoll = 2*time.Second, time.Millisecond
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})

	st, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")})
	if err != nil {
		t.Fatalf("the wait must ride out a build that finishes inside it: %v", err)
	}
	if len(st.Ingress) != 1 || st.Ingress[0].IP != "185.1.1.1" {
		t.Fatalf("bad status: %+v", st)
	}
	if api.getCalls < 3 {
		t.Fatalf("want at least 3 reads (it polled), got %d", api.getCalls)
	}
}

func TestEnsure_DegradedStillPublishes(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	api.forceStatus = statusDegraded
	lb := ensureLB(t, srv)
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})

	st, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")})
	if err != nil {
		t.Fatalf("degraded is a serving LB — withholding its address would widen the outage: %v", err)
	}
	if len(st.Ingress) != 1 || st.Ingress[0].IP != "185.1.1.1" {
		t.Fatalf("bad status: %+v", st)
	}
}

func TestEnsure_ErrorPhaseFailsFastWithDetail(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	api.forceStatus, api.forceStatusDetail = statusError, "no lb-net address left"
	lb := ensureLB(t, srv)
	// A wait long enough that sitting it out would be obvious in the timing.
	lb.readyWait, lb.readyPoll = 30*time.Second, 10*time.Second
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})

	start := time.Now()
	_, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")})
	if err == nil {
		t.Fatal("a failed build must surface, not publish")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("a failed build is not fixed by waiting; took %s", elapsed)
	}
	if !strings.Contains(err.Error(), api.forceStatusDetail) {
		t.Fatalf("the region's own reason must reach the Service event: %v", err)
	}
}

func TestEnsure_ContextCancelStopsTheWait(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	api.serveAfterGets = 1 << 30
	lb := ensureLB(t, srv)
	lb.readyWait, lb.readyPoll = time.Minute, 10*time.Millisecond
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := lb.EnsureLoadBalancer(ctx, "cl", s, []*v1.Node{node("10.7.0.5")}); err == nil {
		t.Fatal("expected the cancelled context to end the wait")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("the wait ignored cancellation; took %s", elapsed)
	}
}

// dropAll removes every LB, standing in for a delete that happened behind the
// controller's back (a panel/API delete, a region teardown, an operator).
func (f *fakeAPI) dropAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lbs = map[string]*LB{}
	f.configs = map[string]LBConfig{}
}

// wire — проводная форма ответа /v1. Внутренний LB больше не несёт json-тегов
// (он заполняется из сгенерированных моделей), поэтому двойник описывает
// контракт явно — и это правильнее: тест держит форму ответа, а не
// внутренности драйвера.
func wire(lb *LB) map[string]any {
	return map[string]any{
		"id": lb.ID, "name": lb.Name, "mode": lb.Mode,
		"node_count": lb.NodeCount, "vip": lb.VIP,
		"status": lb.Status, "status_detail": lb.StatusDetail,
		"project_id": "p", "region_id": "r", "vpc_id": "v",
	}
}

// mustClient — конструктор теперь может отказать (негодный адрес).
func mustClient(t *testing.T, base string) *Client {
	t.Helper()
	c, err := NewClient(base, "p", "k")
	if err != nil {
		t.Fatalf("клиент tatnet: %v", err)
	}
	return c
}
