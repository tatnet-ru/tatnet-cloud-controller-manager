package tatnet

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
)

const (
	annPreserveClientIP         = "tatnet/lb-preserve-client-ip"
	annAWSProxyProtocol         = "service.beta.kubernetes.io/aws-load-balancer-proxy-protocol"
	annAWSTargetGroupAttributes = "service.beta.kubernetes.io/aws-load-balancer-target-group-attributes"
)

func nativeClientIP(a lbAnnotations) bool {
	if a.preserveClientIP != nil {
		return *a.preserveClientIP
	}
	return a.sendProxy == "" || a.sendProxy == "none"
}

// Native L4 is the default. PROXY protocol is opt-in, because prepending it
// to an arbitrary TCP/TLS application's bytes breaks that application.
func parseClientIPAnnotations(s *v1.Service, a lbAnnotations) lbAnnotations {
	fail := func(message string) lbAnnotations { a.clientIPError = message; return a }
	var awsProxy *bool
	if raw, ok := s.Annotations[annAWSProxyProtocol]; ok {
		if raw != "*" {
			return fail(annAWSProxyProtocol + " must be '*'")
		}
		v := true
		awsProxy = &v
	}
	var awsPreserve *bool
	if raw, ok := s.Annotations[annAWSTargetGroupAttributes]; ok {
		seen := map[string]bool{}
		for _, entry := range strings.Split(raw, ",") {
			key, value, found := strings.Cut(strings.TrimSpace(entry), "=")
			if !found || seen[key] || (value != "true" && value != "false") {
				return fail("invalid or duplicate target group attribute " + entry)
			}
			seen[key] = true
			v, _ := strconv.ParseBool(value)
			switch key {
			case "preserve_client_ip.enabled":
				awsPreserve = &v
			case "proxy_protocol_v2.enabled":
				if awsProxy != nil && *awsProxy != v {
					return fail("conflicting AWS PROXY protocol annotations")
				}
				awsProxy = &v
			default:
				return fail("unsupported target group attribute " + key)
			}
		}
	}
	if raw, ok := s.Annotations[annProxyProtocol]; ok {
		switch raw {
		case "v1", "v2", "none":
			a.sendProxy = raw
		default:
			return fail(annProxyProtocol + " must be none, v1 or v2")
		}
		if awsProxy != nil && ((raw == "v2") != *awsProxy || raw == "v1") {
			return fail("conflicting TatNet and AWS PROXY protocol annotations")
		}
	} else if awsProxy != nil {
		if *awsProxy {
			a.sendProxy = "v2"
		} else {
			a.sendProxy = "none"
		}
	}
	if raw, ok := s.Annotations[annPreserveClientIP]; ok {
		if raw != "true" && raw != "false" {
			return fail(annPreserveClientIP + " must be true or false")
		}
		v, _ := strconv.ParseBool(raw)
		a.preserveClientIP = &v
		if awsPreserve != nil && *awsPreserve != v {
			return fail("conflicting client IP preservation annotations")
		}
	} else {
		a.preserveClientIP = awsPreserve
	}
	if nativeClientIP(a) && a.sendProxy != "" && a.sendProxy != "none" {
		return fail("native client IP preservation and PROXY protocol cannot be enabled together")
	}
	return a
}

// An older API can silently discard unknown request fields. A successful HTTP
// response is insufficient: require its persisted config to acknowledge the
// requested network contract before publishing a VIP.
func verifyClientIPContract(body []byte, cfg LBConfig) error {
	want := map[string]TargetGroup{}
	for _, g := range cfg.TargetGroups {
		if g.PreserveClientIP {
			want[g.Name] = g
		}
	}
	if len(want) == 0 {
		return nil
	}
	var response struct {
		Config       *LBConfig     `json:"config"`
		TargetGroups []TargetGroup `json:"target_groups"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("decode client IP contract acknowledgement: %w", err)
	}
	groups := response.TargetGroups
	if response.Config != nil {
		groups = response.Config.TargetGroups
	}
	for _, g := range groups {
		if g.PreserveClientIP {
			expected, ok := want[g.Name]
			if !ok {
				return fmt.Errorf("TatNet API enabled client IP preservation on an unexpected target group %s", g.Name)
			}
			x := append([]string(nil), expected.SourceCIDRs...)
			y := append([]string(nil), g.SourceCIDRs...)
			slices.Sort(x)
			slices.Sort(y)
			if !slices.Equal(x, y) {
				return fmt.Errorf("TatNet API did not acknowledge loadBalancerSourceRanges for %s", g.Name)
			}
			delete(want, g.Name)
		}
	}
	if len(want) > 0 {
		return fmt.Errorf("TatNet API did not acknowledge native client IP preservation; upgrade the API before the CCM")
	}
	return nil
}
