package tatnet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

const testReservation = "8c10620b-6af6-4ae2-b4bd-e69eef8fa0e8"

func TestInvalidReservedIPFailsBeforeAnyAPIRequest(t *testing.T) {
	for _, annotations := range []map[string]string{
		{annFloatingIPID: ""}, {annFloatingIPID: "185.152.80.101"},
		{annFloatingIPID: "typo"}, {annFloatingIPID: testReservation + "\n"},
		{annFloatingIPID: " " + testReservation},
		{annFloatingIPID: testReservation, annScheme: "internal"},
		{annFloatingIPID: testReservation, annAWSScheme: "internal"},
	} {
		t.Run(annotations[annFloatingIPID]+annotations[annScheme]+annotations[annAWSScheme], func(t *testing.T) {
			rec := record.NewFakeRecorder(4)
			l := &loadBalancer{recorder: rec} // nil client: validation must precede all API work
			s := annotated(svc(v1.ServicePort{Port: 443, NodePort: 30443}), annotations)
			if status, err := l.EnsureLoadBalancer(context.Background(), "c", s, []*v1.Node{node("10.7.0.2")}); err == nil || status != nil {
				t.Fatal("invalid reservation silently accepted")
			}
			select {
			case event := <-rec.Events:
				if !strings.Contains(event, "InvalidAnnotation") {
					t.Fatal(event)
				}
			default:
				t.Fatal("missing warning event")
			}
			if err := l.UpdateLoadBalancer(context.Background(), "c", s, []*v1.Node{node("10.7.0.2")}); err == nil {
				t.Fatal("update accepted invalid reservation")
			}
		})
	}
}

func TestReservedIPCreateReconcileAndRecreate(t *testing.T) {
	srv, api := newFakeAPI()
	defer srv.Close()
	l := ensureLB(t, srv)
	s := annotated(svc(v1.ServicePort{Port: 443, NodePort: 30443}), map[string]string{annFloatingIPID: strings.ToUpper(testReservation)})
	nodes := []*v1.Node{node("10.7.0.2")}
	first, err := l.EnsureLoadBalancer(context.Background(), "c", s, nodes)
	if err != nil {
		t.Fatal(err)
	}
	name := l.GetLoadBalancerName(context.Background(), "c", s)
	lb, err := l.client.FindByName(context.Background(), name)
	if err != nil || lb == nil || lb.FloatingIPID != testReservation {
		t.Fatalf("reservation lost in list: %+v %v", lb, err)
	}
	if _, err := l.EnsureLoadBalancer(context.Background(), "c", s, nodes); err != nil {
		t.Fatal(err)
	}
	if err := l.UpdateLoadBalancer(context.Background(), "c", s, nodes); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := l.GetLoadBalancer(context.Background(), "c", s); err != nil || !exists {
		t.Fatalf("get: exists=%v err=%v", exists, err)
	}
	if len(api.lbs) != 1 || api.nextID != 1 {
		t.Fatal("reconcile created duplicate LB")
	}
	if err := l.EnsureLoadBalancerDeleted(context.Background(), "c", s); err != nil {
		t.Fatal(err)
	}
	second, err := l.EnsureLoadBalancer(context.Background(), "c", s, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if first.Ingress[0].IP != second.Ingress[0].IP || api.nextID != 2 {
		t.Fatal("replacement lost stable VIP")
	}
}

func TestReservedIPCannotBeChangedOrRemovedOnExistingLB(t *testing.T) {
	for _, desired := range []string{"", "ad6d163e-7178-4dc8-bc01-f7447625d1e2"} {
		t.Run(desired, func(t *testing.T) {
			srv, api := newFakeAPI()
			defer srv.Close()
			l := ensureLB(t, srv)
			l.recorder = record.NewFakeRecorder(12)
			s := annotated(svc(v1.ServicePort{Port: 443, NodePort: 30443}), map[string]string{annFloatingIPID: testReservation})
			nodes := []*v1.Node{node("10.7.0.2")}
			if _, err := l.EnsureLoadBalancer(context.Background(), "c", s, nodes); err != nil {
				t.Fatal(err)
			}
			if desired == "" {
				delete(s.Annotations, annFloatingIPID)
			} else {
				s.Annotations[annFloatingIPID] = desired
			}
			if got, err := l.EnsureLoadBalancer(context.Background(), "c", s, nodes); err == nil || got != nil {
				t.Fatal("ensure published mismatched reservation")
			}
			if err := l.UpdateLoadBalancer(context.Background(), "c", s, nodes); err == nil {
				t.Fatal("update accepted reservation change")
			}
			if got, _, err := l.GetLoadBalancer(context.Background(), "c", s); err == nil || got != nil {
				t.Fatal("get published mismatched reservation")
			}
			if api.putConfigCalls != 0 || len(api.patchedCounts) != 0 || len(api.lbs) != 1 {
				t.Fatal("existing LB was mutated on invalid reservation change")
			}
			// A broken annotation must never prevent explicit Service teardown.
			s.Annotations[annFloatingIPID] = "invalid"
			if err := l.EnsureLoadBalancerDeleted(context.Background(), "c", s); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReservedIPAPIContractMustBeAcknowledged(t *testing.T) {
	for _, tc := range []struct {
		name         string
		createID     string
		getID        string
		deleteStatus int
	}{
		{"old-api", "", "", 202},
		{"different-allocation", "ad6d163e-7178-4dc8-bc01-f7447625d1e2", "", 202},
		{"cleanup-fails", "", "", 500},
		{"changed-while-building", testReservation, "", 202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleted := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case http.MethodPost:
					var req createReq
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if req.FloatingIPID != testReservation {
						t.Errorf("create lost desired reservation: %+v", req)
					}
					w.WriteHeader(201)
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "lb", "name": "web", "vip": "185.152.80.101", "floating_ip_id": tc.createID, "status": "active", "config": req.Config})
				case http.MethodDelete:
					deleted = true
					w.WriteHeader(tc.deleteStatus)
				default:
					if strings.HasSuffix(r.URL.Path, "/lb") {
						_ = json.NewEncoder(w).Encode(map[string]any{"id": "lb", "name": "web", "vip": "185.152.80.101", "floating_ip_id": tc.getID, "status": "active"})
					} else {
						_, _ = w.Write([]byte(`{"data":[],"count":0}`))
					}
				}
			}))
			defer srv.Close()
			l := ensureLB(t, srv)
			s := annotated(svc(v1.ServicePort{Port: 443, NodePort: 30443}), map[string]string{annFloatingIPID: testReservation})
			got, err := l.EnsureLoadBalancer(context.Background(), "c", s, []*v1.Node{node("10.7.0.2")})
			if err == nil || got != nil {
				t.Fatal("published an unacknowledged reservation")
			}
			if tc.name == "changed-while-building" {
				if deleted {
					t.Fatal("must not delete an already acknowledged LB on a later read mismatch")
				}
			} else if !deleted {
				t.Fatal("unintended newly created LB was not cleaned up")
			}
			if tc.deleteStatus == 500 && !strings.Contains(err.Error(), "cleanup") {
				t.Fatalf("cleanup failure hidden: %v", err)
			}
		})
	}
}

func TestReservedIPAPIRejectionHasNoAutomaticAddressFallback(t *testing.T) {
	for _, code := range []int{403, 409, 422} {
		calls := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodPost {
				calls++
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{"error":{"message":"reserved IP unavailable"}}`))
			} else {
				_, _ = w.Write([]byte(`{"data":[],"count":0}`))
			}
		}))
		l := ensureLB(t, srv)
		rec := record.NewFakeRecorder(4)
		l.recorder = rec
		s := annotated(svc(v1.ServicePort{Port: 443, NodePort: 30443}), map[string]string{annFloatingIPID: testReservation})
		got, err := l.EnsureLoadBalancer(context.Background(), "c", s, []*v1.Node{node("10.7.0.2")})
		srv.Close()
		if err == nil || got != nil || calls != 1 {
			t.Fatalf("fallback after API %d: calls=%d status=%+v err=%v", code, calls, got, err)
		}
		select {
		case event := <-rec.Events:
			if !strings.Contains(event, "ReservedIPRejected") {
				t.Fatal(event)
			}
		default:
			t.Fatal("missing reservation rejection event")
		}
	}
}
