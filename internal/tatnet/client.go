// Package tatnet implements a Kubernetes cloud provider whose only capability is
// LoadBalancer: it maps a Service of type=LoadBalancer to a managed TatNet load
// balancer via the public /v1 API, using a project-scoped tn_live_ key minted
// for this cluster's cloud-controller-manager.
//
// Transport and response models come from the GENERATED client
// github.com/tatnet-ru/tatnet-go, produced from the /v1 contract. What stays
// hand-written here is the managed-config DOCUMENT below, and that split is
// deliberate rather than lazy: drift on the READ side is silent (a renamed
// field decodes to zero — that is how "data" vs "items" shipped in v0.1.3 and
// made FindByName return nothing), while drift on the WRITE side is loud —
// the API answers 422 and the Service says so. The silent half is now bound to
// the contract by the compiler; the loud half keeps the narrow, CCM-shaped
// document that §5.7 prescribes.
package tatnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	api "github.com/tatnet-ru/tatnet-go/tatnet"
)

// Client is a thin /v1 load-balancer client scoped to one project by the key.
type Client struct {
	projectID string
	gen       *api.ClientWithResponses
}

// NewClient builds a client. base has no trailing slash; key is a tn_live_ token.
// It now returns an error because the generated constructor can refuse a bad
// address — previously there was nothing to refuse, the request was assembled
// by hand on every call.
func NewClient(base, projectID, key string) (*Client, error) {
	gen, err := api.NewClientWithResponses(
		strings.TrimRight(base, "/")+"/v1",
		api.WithAPIKey(key),
		api.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	)
	if err != nil {
		return nil, fmt.Errorf("tatnet client: %w", err)
	}
	return &Client{projectID: projectID, gen: gen}, nil
}

// LB carries the fields the CCM needs, filled from the generated models.
//
// k8s_cluster_id used to be declared here and never arrived: neither the list
// nor the detail response carries it, so it silently decoded to "" — and
// nothing in the CCM read it. Generation made that visible; the field is gone.
type LB struct {
	ID        string
	Name      string
	Mode      string
	NodeCount int
	VIP       string
	Status    string
	// Why the region says what it says — surfaced verbatim when the build
	// fails, so the Service event names the cause instead of just "error".
	StatusDetail string
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// ---------------------------------------------------------------------------
// Managed-config document (LBConfigSpec, design §5.1). JSON is snake_case and
// must match the pydantic schemas exactly. Zero fields are omitted so server
// defaults apply; the CCM populates only what §5.7 prescribes.
// ---------------------------------------------------------------------------

// Target mirrors LBTargetSpec. CCM documents carry target_ip members only.
type Target struct {
	VMID     string `json:"vm_id,omitempty"`
	TargetIP string `json:"target_ip,omitempty"`
	AppID    string `json:"app_id,omitempty"`
	Port     int    `json:"port,omitempty"`
	Weight   *int   `json:"weight,omitempty"` // pointer: 0 (drain) is a valid value; nil → server default 1
}

// HealthCheck mirrors LBHealthCheckSpec.
type HealthCheck struct {
	Mode               string `json:"mode,omitempty"` // none|tcp|http
	Port               int    `json:"port,omitempty"`
	Path               string `json:"path,omitempty"`
	Host               string `json:"host,omitempty"`
	ExpectStatus       string `json:"expect_status,omitempty"`
	IntervalSeconds    int    `json:"interval_seconds,omitempty"`
	TimeoutSeconds     int    `json:"timeout_seconds,omitempty"`
	HealthyThreshold   int    `json:"healthy_threshold,omitempty"`
	UnhealthyThreshold int    `json:"unhealthy_threshold,omitempty"`
}

// Stickiness mirrors LBStickinessSpec.
type Stickiness struct {
	Type            string `json:"type,omitempty"` // none|cookie|source_ip
	CookieName      string `json:"cookie_name,omitempty"`
	DurationSeconds int    `json:"duration_seconds,omitempty"`
}

// TargetGroup mirrors LBTargetGroupSpec (the fields the CCM sets). TGs are
// addressed by client-visible name, which is what makes the declarative PUT
// idempotent.
type TargetGroup struct {
	Name        string       `json:"name"`
	TargetType  string       `json:"target_type"`    // vm|ip|app — CCM: "ip"
	Protocol    string       `json:"protocol"`       // http|https|tcp — CCM: "tcp"
	Port        int          `json:"port,omitempty"` // required unless target_type="app"
	Balance     string       `json:"balance,omitempty"`
	HealthCheck *HealthCheck `json:"health_check,omitempty"`
	Stickiness  *Stickiness  `json:"stickiness,omitempty"`
	SendProxy   string       `json:"send_proxy,omitempty"` // none|v1|v2
	Targets     []Target     `json:"targets"`
}

// RuleAction mirrors LBRuleAction (CCM documents use forward only).
type RuleAction struct {
	Type        string `json:"type"`                   // forward|redirect|fixed_response
	TargetGroup string `json:"target_group,omitempty"` // TG name within the document
}

// Listener mirrors LBListenerSpec (the fields the CCM sets). http2 has no
// omitempty on purpose: the server default is true but tcp listeners must
// carry http2=false (design §5.4.5), so it is always serialized explicitly.
type Listener struct {
	Port           int        `json:"port"`
	Protocol       string     `json:"protocol"` // http|https|tcp — CCM: "tcp"
	CertificateIDs []string   `json:"certificate_ids,omitempty"`
	AcceptProxy    bool       `json:"accept_proxy,omitempty"`
	HTTP2          bool       `json:"http2"`
	DefaultAction  RuleAction `json:"default_action"`
}

// LBConfig is the full managed-config document (LBConfigSpec).
type LBConfig struct {
	TargetGroups []TargetGroup `json:"target_groups"`
	Listeners    []Listener    `json:"listeners"`
}

// apiError carries the HTTP status so callers can distinguish 404.
type apiError struct {
	status int
	body   string
}

func (e *apiError) Error() string { return fmt.Sprintf("tatnet api %d: %s", e.status, e.body) }

// check turns a non-2xx into apiError. The generated client does not treat a
// status as failure by itself — it hands back the parsed body and the response
// and leaves that judgement to the caller, which is what lets Delete treat 404
// as success.
func check(resp *http.Response, body []byte) error {
	if resp == nil {
		return errors.New("tatnet api: empty response")
	}
	if resp.StatusCode >= 300 {
		return &apiError{status: resp.StatusCode, body: string(body)}
	}
	return nil
}

// IsNotFound reports whether err is a 404 from the API.
func IsNotFound(err error) bool { return statusIs(err, http.StatusNotFound) }

// IsUnprocessable reports whether err is a 422 from the API: the request was
// well-formed but the config was rejected by validation. The canonical case
// is a Service asking for port 80 while an HTTP-01 certificate is attached to
// the LB (tcp:80 is otherwise legal — the unconditional ACME reservation was
// lifted). Retrying without user action cannot succeed, so callers surface it
// to the Service owner instead of only logging.
func IsUnprocessable(err error) bool { return statusIs(err, http.StatusUnprocessableEntity) }

func statusIs(err error, code int) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.status == code
}

// pageLimit is the /v1 maximum page size.
const pageLimit = 200

// FindByName returns the project's LB with the given name, or nil if none.
// It pages through the whole collection: one ?limit=200 call silently misses
// LBs once the project exceeds the /v1 page cap.
func (c *Client) FindByName(ctx context.Context, name string) (*LB, error) {
	for offset := 0; ; offset += pageLimit {
		limit, off := pageLimit, offset
		resp, err := c.gen.LoadBalancersListLbsWithResponse(ctx, c.projectID,
			&api.LoadBalancersListLbsParams{Limit: &limit, Offset: &off})
		if err != nil {
			return nil, err
		}
		if err := check(resp.HTTPResponse, resp.Body); err != nil {
			return nil, err
		}
		if resp.JSON200 == nil {
			return nil, fmt.Errorf("tatnet api: %d returned no list body", resp.HTTPResponse.StatusCode)
		}
		for i := range resp.JSON200.Data {
			if resp.JSON200.Data[i].Name == name {
				return fromList(&resp.JSON200.Data[i]), nil
			}
		}
		// A short (or empty) page is the end of the collection. This is the
		// ONLY valid termination: the envelope's count is the page size, not
		// the total, so any count-based check stops after the first page
		// (that bug capped FindByName at 200 LBs — duplicate-create class).
		// When the total is an exact multiple of pageLimit this costs one
		// extra empty-page request; correctness over a saved roundtrip.
		if len(resp.JSON200.Data) < pageLimit {
			return nil, nil
		}
	}
}

func fromList(v *api.LoadBalancerResponse) *LB {
	return &LB{
		ID: v.Id, Name: v.Name, Mode: deref(v.Mode), NodeCount: v.NodeCount,
		VIP: deref(v.Vip), Status: v.Status, StatusDetail: deref(v.StatusDetail),
	}
}

func fromDetail(v *api.LoadBalancerDetailResponse) *LB {
	return &LB{
		ID: v.Id, Name: v.Name, Mode: deref(v.Mode), NodeCount: v.NodeCount,
		VIP: deref(v.Vip), Status: v.Status, StatusDetail: deref(v.StatusDetail),
	}
}

// Create makes a new k8s-mode LB with the full config document in the body —
// the shortest path stays a single call.
//
// The body goes as raw JSON on purpose: the config document is the CCM's own,
// narrow view of LBConfigSpec (§5.7), and mapping it into the generated
// wide type would put a second hand-written mirror where one was removed.
func (c *Client) Create(ctx context.Context, name string, nodeCount int, cfg LBConfig, k8sClusterID string) (*LB, error) {
	body, err := json.Marshal(createReq{
		Name: name, K8sClusterID: k8sClusterID, NodeCount: nodeCount, Config: cfg,
	})
	if err != nil {
		return nil, err
	}
	resp, err := c.gen.LoadBalancersCreateLbWithBodyWithResponse(ctx, c.projectID,
		"application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if err := check(resp.HTTPResponse, resp.Body); err != nil {
		return nil, err
	}
	if resp.JSON201 == nil {
		return nil, fmt.Errorf("tatnet api: %d returned no LB body", resp.HTTPResponse.StatusCode)
	}
	return fromDetail(resp.JSON201), nil
}

// Get fetches one LB by id.
func (c *Client) Get(ctx context.Context, id string) (*LB, error) {
	resp, err := c.gen.LoadBalancersGetLbWithResponse(ctx, c.projectID, id)
	if err != nil {
		return nil, err
	}
	if err := check(resp.HTTPResponse, resp.Body); err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("tatnet api: %d returned no LB body", resp.HTTPResponse.StatusCode)
	}
	return fromDetail(resp.JSON200), nil
}

// PutManagedConfig declaratively replaces the CCM-owned objects of the LB
// (managed_by='ccm') with the document. User-created listeners/target-groups
// on the same LB are untouched — that bounded blast radius is the whole point
// of the endpoint, and the CCM must never write through any other path.
func (c *Client) PutManagedConfig(ctx context.Context, id string, cfg LBConfig) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	resp, err := c.gen.LoadBalancersPutLbManagedConfigWithBodyWithResponse(ctx, c.projectID, id,
		"application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	return check(resp.HTTPResponse, resp.Body)
}

// PatchNodeCount changes the LB's haproxy node count (1..3).
func (c *Client) PatchNodeCount(ctx context.Context, id string, nodeCount int) error {
	resp, err := c.gen.LoadBalancersUpdateLbWithResponse(ctx, c.projectID, id,
		api.LoadBalancersUpdateLbJSONRequestBody{NodeCount: &nodeCount})
	if err != nil {
		return err
	}
	return check(resp.HTTPResponse, resp.Body)
}

// Delete marks the LB for teardown (idempotent — a missing LB is success).
func (c *Client) Delete(ctx context.Context, id string) error {
	resp, err := c.gen.LoadBalancersDeleteLbWithResponse(ctx, c.projectID, id)
	if err != nil {
		return err
	}
	if err := check(resp.HTTPResponse, resp.Body); IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

type createReq struct {
	Name         string   `json:"name"`
	K8sClusterID string   `json:"k8s_cluster_id"`
	NodeCount    int      `json:"node_count,omitempty"`
	Config       LBConfig `json:"config"`
}
