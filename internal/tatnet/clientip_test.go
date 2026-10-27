package tatnet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
)

func TestClientIPDefaultsAndAWSAttributes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		native      bool
		proxy       string
		bad         bool
	}{
		{"default", nil, true, "", false},
		{"native-off", map[string]string{annPreserveClientIP: "false"}, false, "", false},
		{"proxy", map[string]string{annProxyProtocol: "v2"}, false, "v2", false},
		{"aws-proxy", map[string]string{annAWSProxyProtocol: "*"}, false, "v2", false},
		{"aws-native", map[string]string{annAWSTargetGroupAttributes: "preserve_client_ip.enabled=true,proxy_protocol_v2.enabled=false"}, true, "none", false},
		{"aws-proxy-attributes", map[string]string{annAWSTargetGroupAttributes: "preserve_client_ip.enabled=false,proxy_protocol_v2.enabled=true"}, false, "v2", false},
		{"typo", map[string]string{annProxyProtocol: "v3"}, false, "", true},
		{"conflict", map[string]string{annProxyProtocol: "v2", annPreserveClientIP: "true"}, false, "", true},
		{"duplicate", map[string]string{annAWSTargetGroupAttributes: "preserve_client_ip.enabled=true,preserve_client_ip.enabled=false"}, false, "", true},
		{"invalid-bool", map[string]string{annPreserveClientIP: "TRUE"}, false, "", true},
		{"conflicting-proxy", map[string]string{annProxyProtocol: "none", annAWSProxyProtocol: "*"}, false, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := (&loadBalancer{}).parseAnnotations(annotated(svc(), tc.annotations))
			if tc.bad {
				if a.clientIPError == "" {
					t.Fatal("unsafe configuration was accepted")
				}
				return
			}
			if a.clientIPError != "" || nativeClientIP(a) != tc.native || a.sendProxy != tc.proxy {
				t.Fatalf("got %+v", a)
			}
		})
	}
}

func TestSourceRangesAreEnforcedOrRejected(t *testing.T) {
	s := svc(v1.ServicePort{Port: 443, NodePort: 30443})
	s.Spec.LoadBalancerSourceRanges = []string{"203.0.113.0/24"}
	cfg, err := configFor(s, []string{"10.1.0.2"}, lbAnnotations{})
	if err != nil || !cfg.TargetGroups[0].PreserveClientIP || len(cfg.TargetGroups[0].SourceCIDRs) != 1 {
		t.Fatalf("source range lost: %+v %v", cfg, err)
	}
	if _, err := configFor(s, []string{"10.1.0.2"}, lbAnnotations{sendProxy: "v2"}); err == nil {
		t.Fatal("source ranges must never be silently ignored by a TCP proxy")
	}
}

func TestOldAPICannotSilentlyDowngradeClientIP(t *testing.T) {
	deleted := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			deleted = true
			w.WriteHeader(202)
			return
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"id":"old","name":"web","node_count":2,"status":"active","config":{"target_groups":[{"name":"svc-443","protocol":"tcp","target_type":"ip","port":30443}]}}`))
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL, "p", "key")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Create(context.Background(), "web", 2, LBConfig{TargetGroups: []TargetGroup{{Name: "svc-443", PreserveClientIP: true}}}, "c", CreateOptions{})
	if err == nil || !strings.Contains(err.Error(), "did not acknowledge") || !deleted {
		t.Fatalf("old API left a wrongly advertised LB: deleted=%v err=%v", deleted, err)
	}
}

func TestNativeAcknowledgementRequiresEveryRequestedGroup(t *testing.T) {
	cfg := LBConfig{TargetGroups: []TargetGroup{{Name: "svc-80", PreserveClientIP: true}, {Name: "svc-443", PreserveClientIP: true}}}
	body, _ := json.Marshal(cfg)
	if err := verifyClientIPContract(body, cfg); err != nil {
		t.Fatal(err)
	}
	if err := verifyClientIPContract([]byte(`{"target_groups":[{"name":"svc-80","preserve_client_ip":true}]}`), cfg); err == nil {
		t.Fatal("partially acknowledged network contract accepted")
	}
}

func TestAPICannotAcknowledgeNativeWhileDroppingSourceRanges(t *testing.T) {
	cfg := LBConfig{TargetGroups: []TargetGroup{{Name: "svc-443", PreserveClientIP: true, SourceCIDRs: []string{"203.0.113.0/24"}}}}
	if err := verifyClientIPContract([]byte(`{"target_groups":[{"name":"svc-443","preserve_client_ip":true}]}`), cfg); err == nil {
		t.Fatal("source policy was silently dropped")
	}
}
