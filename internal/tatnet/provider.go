package tatnet

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"
)

// ProviderName is the --cloud-provider value and the provider-id scheme.
const ProviderName = "tatnet"

// provider implements cloudprovider.Interface with LoadBalancer as its only
// capability — the CCM runs the service controller alone (no node/route
// controllers), so Instances/Zones/Routes are intentionally absent.
type provider struct {
	lb *loadBalancer
	// How often the existence reconciler re-checks that every published load
	// balancer still exists (0 disables it). Not the service controller's job:
	// that one is edge-triggered on the Service and never re-asks.
	existencePeriod time.Duration
}

func init() {
	cloudprovider.RegisterCloudProvider(ProviderName, func(io.Reader) (cloudprovider.Interface, error) {
		return newProvider()
	})
}

func newProvider() (cloudprovider.Interface, error) {
	base := os.Getenv("TATNET_API_BASE")
	project := os.Getenv("TATNET_PROJECT_ID")
	key := os.Getenv("TATNET_API_KEY")
	clusterID := os.Getenv("TATNET_K8S_CLUSTER_ID")
	for k, v := range map[string]string{
		"TATNET_API_BASE": base, "TATNET_PROJECT_ID": project,
		"TATNET_API_KEY": key, "TATNET_K8S_CLUSTER_ID": clusterID,
	} {
		if v == "" {
			return nil, fmt.Errorf("%s is required", k)
		}
	}
	nodeCount := 2
	if s := os.Getenv("TATNET_LB_NODE_COUNT"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 3 {
			return nil, fmt.Errorf("TATNET_LB_NODE_COUNT must be 1..3")
		}
		nodeCount = n
	}
	// How long one Ensure waits for the region to finish building before
	// handing the retry back to the controller. Not "how long a build may
	// take": the address stays unpublished across retries either way, this
	// only trades worker-blocking against how late a ready LB gets published
	// (see awaitServing). 0 disables waiting entirely.
	readyWait := 30 * time.Second
	if s := os.Getenv("TATNET_LB_READY_TIMEOUT"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("TATNET_LB_READY_TIMEOUT must be a non-negative duration (e.g. 30s)")
		}
		readyWait = d
	}
	existencePeriod := defaultExistencePeriod
	if s := os.Getenv("TATNET_LB_EXISTENCE_SYNC_PERIOD"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("TATNET_LB_EXISTENCE_SYNC_PERIOD must be a non-negative duration (e.g. 5m)")
		}
		existencePeriod = d
	}
	client, err := NewClient(base, project, key)
	if err != nil {
		return nil, err
	}
	return &provider{
		lb: &loadBalancer{
			client: client, k8sClusterID: clusterID, nodeCount: nodeCount,
			readyWait: readyWait,
		},
		existencePeriod: existencePeriod,
	}, nil
}

// Initialize wires an event recorder so annotation problems land on the
// Service as Warning events (invalid values are ignored, not fatal — the
// events are the only way the user learns about the typo).
func (p *provider) Initialize(cb cloudprovider.ControllerClientBuilder, stop <-chan struct{}) {
	client := cb.ClientOrDie("tatnet-cloud-controller-manager")
	b := record.NewBroadcaster()
	b.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	p.lb.recorder = b.NewRecorder(scheme.Scheme, v1.EventSource{Component: "tatnet-cloud-controller-manager"})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stop
		cancel()
		b.Shutdown()
	}()
	// The level-triggered half of the contract: the service controller reacts
	// to Service changes, this notices when the CLOUD changed underneath one.
	// Runs once per cluster without a lock of its own: Initialize is reached
	// through startControllers, which the framework calls only from
	// OnStartedLeading — so a standby replica never gets here, and losing the
	// lease cancels ctx along with everything else.
	go (&existenceReconciler{lb: p.lb, kube: client, period: p.existencePeriod}).Run(ctx)
}
func (p *provider) LoadBalancer() (cloudprovider.LoadBalancer, bool)                  { return p.lb, true }
func (p *provider) Instances() (cloudprovider.Instances, bool)                        { return nil, false }
func (p *provider) InstancesV2() (cloudprovider.InstancesV2, bool)                    { return nil, false }
func (p *provider) Zones() (cloudprovider.Zones, bool)                                { return nil, false }
func (p *provider) Clusters() (cloudprovider.Clusters, bool)                          { return nil, false }
func (p *provider) Routes() (cloudprovider.Routes, bool)                              { return nil, false }
func (p *provider) ProviderName() string                                             { return ProviderName }
func (p *provider) HasClusterID() bool                                               { return true }
