package tatnet

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// annResyncedAt marks a Service whose load balancer this reconciler had to
// rebuild. It doubles as the wake-up signal — see repair.
const annResyncedAt = "tatnet.ru/lb-resynced-at"

// defaultExistencePeriod is how often the sweep runs when nothing overrides it.
const defaultExistencePeriod = 5 * time.Minute

// existenceReconciler asks the one question the service controller never asks
// twice: does the load balancer we published still exist?
//
// The controller is edge-triggered on the Service. Its informer re-delivers
// every Service on resync, but the handler hands old == new to needsUpdate,
// which compares spec and metadata only — so a re-delivery never re-enqueues,
// and EnsureLoadBalancer is not called again for a Service nobody edited.
// Cloud state, however, can change while the Service does not: delete the LB
// out of band and the Service goes on publishing its address forever.
// Measured on prod: ten minutes after such a delete, no recreate and not one
// line in the CCM log. This loop is the level-triggered half.
type existenceReconciler struct {
	lb     *loadBalancer
	kube   kubernetes.Interface
	period time.Duration
}

type sweepResult struct {
	checked  int // Services we published an address for
	stale    int // ...whose published address is no longer the truth
	repaired int // ...and that we retracted + re-queued
}

// Run sweeps until ctx is done. period <= 0 disables the loop entirely.
func (r *existenceReconciler) Run(ctx context.Context) {
	if r.period <= 0 {
		klog.Info("tatnet: load-balancer existence reconciler disabled")
		return
	}
	klog.Infof("tatnet: load-balancer existence reconciler every %s", r.period)
	t := time.NewTicker(r.period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			res, err := r.sweep(ctx)
			if err != nil {
				klog.Errorf("tatnet: existence sweep failed: %v", err)
				continue
			}
			if res.stale > 0 {
				klog.Warningf("tatnet: existence sweep: %d/%d published address(es) stale, %d re-queued",
					res.stale, res.checked, res.repaired)
			}
		}
	}
}

// watched: a Service we told Kubernetes was ready. One without an address has
// nothing to be wrong about — it is either still building (EnsureLoadBalancer
// withholds the address until the region serves it) or genuinely unclaimed —
// and one being deleted is the finalizer's business, not ours.
func watched(svc *v1.Service) bool {
	return svc.Spec.Type == v1.ServiceTypeLoadBalancer &&
		svc.DeletionTimestamp == nil &&
		len(svc.Status.LoadBalancer.Ingress) > 0
}

// publishedIP is the address we told Kubernetes to use. The CCM is the only
// writer of this field and always writes exactly one IP (statusFor).
func publishedIP(svc *v1.Service) string {
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return ""
	}
	return svc.Status.LoadBalancer.Ingress[0].IP
}

// staleness returns ("", "") when the published address is still correct, and
// otherwise an event reason plus a human-readable cause.
func staleness(name string, svc *v1.Service, found *LB) (string, string) {
	if found == nil {
		return "LoadBalancerMissing", fmt.Sprintf("load balancer %q no longer exists in TatNet", name)
	}
	if found.VIP == "" {
		return "LoadBalancerAddressLost", fmt.Sprintf("load balancer %q has no address", name)
	}
	if found.VIP != publishedIP(svc) {
		return "LoadBalancerAddressChanged", fmt.Sprintf(
			"load balancer %q now answers at %s, not the published %s", name, found.VIP, publishedIP(svc))
	}
	return "", ""
}

func (r *existenceReconciler) sweep(ctx context.Context) (sweepResult, error) {
	var res sweepResult
	list, err := r.kube.CoreV1().Services(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return res, fmt.Errorf("list services: %w", err)
	}
	for i := range list.Items {
		svc := &list.Items[i]
		if !watched(svc) {
			continue
		}
		res.checked++
		name := r.lb.GetLoadBalancerName(ctx, "", svc)
		found, err := r.lb.client.FindByName(ctx, name)
		if err != nil {
			// A lookup that FAILED is not a lookup that found nothing. Treating
			// the two alike would retract a live address on any transient API
			// error — the sweep would become the outage it exists to prevent.
			klog.Warningf("tatnet: existence check for service %s/%s failed, leaving it alone: %v",
				svc.Namespace, svc.Name, err)
			continue
		}
		// The invariant is not "the LB exists" but "the address we published is
		// still the truth" — the weaker one would keep advertising a stale VIP
		// for a balancer that got rebuilt at a different address.
		reason, msg := staleness(name, svc, found)
		if reason == "" {
			continue
		}
		res.stale++
		r.lb.warnf(svc, reason, "%s; retracting the address and rebuilding", msg)
		if err := r.repair(ctx, svc); err != nil {
			klog.Errorf("tatnet: repair of service %s/%s failed: %v", svc.Namespace, svc.Name, err)
			continue
		}
		res.repaired++
	}
	return res, nil
}

// repair retracts the dead address, then wakes the service controller.
//
// Both steps are needed, in this order:
//
//  1. Clearing status.loadBalancer is the truthful half. The address we handed
//     out no longer answers, and "no address yet" is a state every caller
//     already handles, while a dead one is a black hole — connections time out
//     instead of being refused. If step 2 never succeeds, this still stops the
//     lie.
//  2. The annotation is the wake-up. needsUpdate compares annotations, and
//     that is the only handle it offers on an object we may touch: status is
//     not compared (so step 1 on its own changes nothing), and spec belongs to
//     the owner. Writing it enqueues the Service, and the service controller —
//     which stays the SINGLE writer of status.loadBalancer on the happy path —
//     rebuilds the LB and publishes the new address itself. Rebuilding it here
//     instead would put a second writer on that field, which is the very shape
//     of bug this loop exists to clean up after.
//
// The annotation is left behind on purpose: it is the record that a rebuild
// happened, and the address after one is a NEW one. ⚠ It also means the sweep
// writes to the owner's Service — a GitOps controller that reconciles
// annotations will show drift here.
func (r *existenceReconciler) repair(ctx context.Context, svc *v1.Service) error {
	api := r.kube.CoreV1().Services(svc.Namespace)
	fresh, err := api.Get(ctx, svc.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get service: %w", err)
	}
	fresh.Status.LoadBalancer = v1.LoadBalancerStatus{}
	fresh, err = api.UpdateStatus(ctx, fresh, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("retract address: %w", err)
	}
	if fresh.Annotations == nil {
		fresh.Annotations = map[string]string{}
	}
	// A changing value is what makes this a change at all; a fixed marker
	// would wake the controller once and never again.
	fresh.Annotations[annResyncedAt] = time.Now().UTC().Format(time.RFC3339)
	if _, err := api.Update(ctx, fresh, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("re-queue service: %w", err)
	}
	return nil
}
