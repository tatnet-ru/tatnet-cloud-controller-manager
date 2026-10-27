package tatnet

import (
	"fmt"
	"regexp"
	"strings"

	v1 "k8s.io/api/core/v1"
)

var reservationIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func (l *loadBalancer) parseReservedIP(service *v1.Service, scheme string) (string, error) {
	raw, present := service.Annotations[annFloatingIPID]
	if !present {
		return "", nil
	}
	var err error
	switch {
	case !reservationIDPattern.MatchString(raw):
		err = fmt.Errorf("%s must be a canonical Floating IP allocation UUID", annFloatingIPID)
	case normalizedScheme(scheme) == "internal":
		err = fmt.Errorf("%s requires an internet-facing load balancer", annFloatingIPID)
	}
	if err != nil {
		l.warnf(service, "InvalidAnnotation", "%v; refusing to choose another address", err)
		return "", err
	}
	return strings.ToLower(raw), nil
}

func verifyReservedIP(lb *LB, want string) error {
	if lb.FloatingIPID != want {
		return fmt.Errorf("reserved IP mismatch (requested %q, actual %q); the reservation is immutable: restore the annotation or recreate the Service", want, lb.FloatingIPID)
	}
	return nil
}

func (l *loadBalancer) checkReservedIP(service *v1.Service, lb *LB, want string) error {
	if err := verifyReservedIP(lb, want); err != nil {
		l.warnf(service, "ReservedIPChangeRejected", "%v", err)
		return err
	}
	return nil
}
