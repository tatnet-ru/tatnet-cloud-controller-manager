package tatnet

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

// ---------------------------------------------------------------------------
// Existence reconciliation
//
// The service controller is edge-triggered on the Service: its informer
// re-delivers every object on resync, but needsUpdate compares spec and
// metadata only, so an unchanged Service is never re-enqueued and
// EnsureLoadBalancer never runs again. Delete the LB out of band and the
// Service publishes a dead address forever (measured on prod: ten minutes, no
// recreate, no log line). These tests pin the level-triggered half.
// ---------------------------------------------------------------------------

func lbSvc() *v1.Service {
	s := svc(v1.ServicePort{Port: 80, NodePort: 31000, Protocol: v1.ProtocolTCP})
	s.UID = types.UID("11111111-2222-3333-4444-555555555555")
	return s
}

// sweepFixture: an LB really created through EnsureLoadBalancer, and a Service
// in the fake cluster carrying the address that Ensure published.
func sweepFixture(t *testing.T) (*existenceReconciler, *fakeAPI, *record.FakeRecorder, *httptest.Server) {
	t.Helper()
	srv, api := newFakeAPI()
	rec := record.NewFakeRecorder(8)
	lb := &loadBalancer{client: mustClient(t, srv.URL), k8sClusterID: "c", nodeCount: 2, recorder: rec}
	s := lbSvc()
	st, err := lb.EnsureLoadBalancer(context.Background(), "cl", s, []*v1.Node{node("10.7.0.5")})
	if err != nil {
		srv.Close()
		t.Fatalf("fixture: %v", err)
	}
	s.Status.LoadBalancer = *st
	r := &existenceReconciler{lb: lb, kube: fake.NewSimpleClientset(s), period: time.Minute}
	return r, api, rec, srv
}

func currentSvc(t *testing.T, r *existenceReconciler) *v1.Service {
	t.Helper()
	got, err := r.kube.CoreV1().Services("default").Get(context.Background(), "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	return got
}

func TestSweep_RetractsAndRequeuesWhenTheLoadBalancerIsGone(t *testing.T) {
	r, api, rec, srv := sweepFixture(t)
	defer srv.Close()
	api.dropAll() // someone deleted it behind the controller's back

	res, err := r.sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (sweepResult{checked: 1, stale: 1, repaired: 1}) {
		t.Fatalf("unexpected sweep result: %+v", res)
	}
	got := currentSvc(t, r)
	if len(got.Status.LoadBalancer.Ingress) != 0 {
		t.Fatalf("the dead address must be retracted, got %+v", got.Status.LoadBalancer.Ingress)
	}
	stamp, ok := got.Annotations[annResyncedAt]
	if !ok {
		t.Fatal("the wake-up annotation is the only handle needsUpdate offers; without it nothing re-ensures")
	}
	if _, err := time.Parse(time.RFC3339, stamp); err != nil {
		t.Fatalf("the value must change every rebuild, so it is a timestamp: %q", stamp)
	}
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "LoadBalancerMissing") {
			t.Fatalf("unexpected event: %q", e)
		}
	default:
		t.Fatal("the owner learns about a rebuild from an event or not at all")
	}
}

func TestSweep_LeavesALiveLoadBalancerAlone(t *testing.T) {
	r, _, _, srv := sweepFixture(t)
	defer srv.Close()

	res, err := r.sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (sweepResult{checked: 1}) {
		t.Fatalf("a healthy LB must not be touched: %+v", res)
	}
	got := currentSvc(t, r)
	if len(got.Status.LoadBalancer.Ingress) != 1 || got.Status.LoadBalancer.Ingress[0].IP != "185.1.1.1" {
		t.Fatalf("address must survive: %+v", got.Status.LoadBalancer.Ingress)
	}
	if _, ok := got.Annotations[annResyncedAt]; ok {
		t.Fatal("no rebuild happened, so nothing may be stamped on the owner's Service")
	}
}

func TestSweep_ALookupErrorIsNotAMissingLoadBalancer(t *testing.T) {
	// The failure mode that would make this sweep the outage it prevents: one
	// transient API error retracting every live address in the cluster.
	r, api, _, srv := sweepFixture(t)
	defer srv.Close()
	api.failList = true

	res, err := r.sweep(context.Background())
	if err != nil {
		t.Fatalf("one unreadable Service must not abort the sweep: %v", err)
	}
	if res.stale != 0 || res.repaired != 0 {
		t.Fatalf("an error is not an absence: %+v", res)
	}
	got := currentSvc(t, r)
	if len(got.Status.LoadBalancer.Ingress) != 1 {
		t.Fatalf("a live address must survive an unreadable API: %+v", got.Status.LoadBalancer.Ingress)
	}
}

func TestSweep_SkipsServicesWithNothingPublishedYet(t *testing.T) {
	// Ensure withholds the address until the region serves it, so a Service
	// without one is mid-build — not a Service whose LB vanished.
	r, api, _, srv := sweepFixture(t)
	defer srv.Close()
	api.dropAll()
	cur := currentSvc(t, r)
	cur.Status.LoadBalancer = v1.LoadBalancerStatus{}
	if _, err := r.kube.CoreV1().Services("default").UpdateStatus(
		context.Background(), cur, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := r.sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (sweepResult{}) {
		t.Fatalf("nothing was published, so nothing can be stale: %+v", res)
	}
}

func TestSweep_SkipsTerminatingServices(t *testing.T) {
	// Its LB is *supposed* to be gone — the finalizer path is deleting it.
	// Rebuilding here would resurrect what teardown just removed.
	r, api, _, srv := sweepFixture(t)
	defer srv.Close()
	api.dropAll()
	cur := currentSvc(t, r)
	now := metav1.Now()
	cur.DeletionTimestamp = &now
	cur.Finalizers = []string{"service.kubernetes.io/load-balancer-cleanup"}
	if _, err := r.kube.CoreV1().Services("default").Update(
		context.Background(), cur, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := r.sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (sweepResult{}) {
		t.Fatalf("a Service being deleted is the finalizer's business: %+v", res)
	}
}

func TestSweep_IgnoresNonLoadBalancerServices(t *testing.T) {
	r, api, _, srv := sweepFixture(t)
	defer srv.Close()
	api.dropAll()
	cur := currentSvc(t, r)
	cur.Spec.Type = v1.ServiceTypeClusterIP
	if _, err := r.kube.CoreV1().Services("default").Update(
		context.Background(), cur, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := r.sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (sweepResult{}) {
		t.Fatalf("not ours: %+v", res)
	}
}

func TestRun_ZeroPeriodDisablesTheLoop(t *testing.T) {
	r, api, _, srv := sweepFixture(t)
	defer srv.Close()
	api.dropAll()
	r.period = 0

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	select {
	case <-done: // returned immediately, without waiting out the context
	case <-time.After(2 * time.Second):
		t.Fatal("period 0 must disable the loop, not spin it")
	}
	if len(currentSvc(t, r).Status.LoadBalancer.Ingress) != 1 {
		t.Fatal("a disabled reconciler must not touch anything")
	}
}

func TestSweep_RepairsAnAddressThatMoved(t *testing.T) {
	// The weaker invariant ("the LB exists") would leave the Service pointing
	// at a VIP the balancer no longer answers on.
	r, api, rec, srv := sweepFixture(t)
	defer srv.Close()
	api.mu.Lock()
	for _, lb := range api.lbs {
		lb.VIP = "185.1.1.99"
	}
	api.mu.Unlock()

	res, err := r.sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (sweepResult{checked: 1, stale: 1, repaired: 1}) {
		t.Fatalf("a moved address is stale too: %+v", res)
	}
	if len(currentSvc(t, r).Status.LoadBalancer.Ingress) != 0 {
		t.Fatal("the old address must be retracted")
	}
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "LoadBalancerAddressChanged") {
			t.Fatalf("the event must say WHICH way it went stale: %q", e)
		}
	default:
		t.Fatal("expected an event")
	}
}
