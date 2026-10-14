package tatnet

import (
	"context"
	"encoding/json"
	v1 "k8s.io/api/core/v1"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSchemeInvalidAndConflictingFailBeforeAPI(t *testing.T) {
	for _, annotations := range []map[string]string{
		{annScheme: "interanl"},
		{annScheme: ""},
		{annAWSScheme: ""},
		{annScheme: "internal", annAWSScheme: "internet-facing"},
	} {
		l := &loadBalancer{}
		s := annotated(svc(v1.ServicePort{Port: 80, NodePort: 31000}), annotations)
		if _, err := l.EnsureLoadBalancer(context.Background(), "c", s, []*v1.Node{node("10.7.0.2")}); err == nil {
			t.Fatal("invalid scheme must never fall back to public")
		}
	}
}

func TestInternalSchemeFromAWSAnnotationIsProvisionedAndPublished(t *testing.T) {
	created := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			var req createReq
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatal(err)
			}
			if req.Scheme != "internal" {
				t.Fatalf("create scheme=%q", req.Scheme)
			}
			created = true
			w.WriteHeader(201)
			w.Write([]byte(`{"id":"lb","name":"web","scheme":"internal","vip":"10.7.0.252","node_count":2,"status":"active"}`))
		} else if strings.HasSuffix(r.URL.Path, "/lb") {
			w.Write([]byte(`{"id":"lb","name":"web","scheme":"internal","vip":"10.7.0.252","node_count":2,"status":"active"}`))
		} else {
			w.Write([]byte(`{"data":[],"count":0,"limit":50,"offset":0}`))
		}
	}))
	defer srv.Close()
	l := ensureLB(t, srv)
	s := annotated(svc(v1.ServicePort{Port: 80, NodePort: 31000}), map[string]string{annAWSScheme: "internal"})
	got, err := l.EnsureLoadBalancer(context.Background(), "c", s, []*v1.Node{node("10.7.0.2")})
	if err != nil {
		t.Fatal(err)
	}
	if !created || got.Ingress[0].IP != "10.7.0.252" {
		t.Fatalf("got=%+v created=%v", got, created)
	}
}
