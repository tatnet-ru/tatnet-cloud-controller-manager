// Command tatnet-cloud-controller-manager runs the Kubernetes
// cloud-controller-manager wired to the TatNet cloud provider. Only the
// service-lb controller is meant to run (selected via --controllers in the
// Deployment) — the provider implements LoadBalancer alone, so nodes are never
// tainted uninitialized and kubelet stays on the default (no --cloud-provider).
package main

import (
	"os"

	"k8s.io/apimachinery/pkg/util/wait"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app"
	"k8s.io/cloud-provider/app/config"
	"k8s.io/cloud-provider/names"
	"k8s.io/cloud-provider/options"
	"k8s.io/component-base/cli"
	cliflag "k8s.io/component-base/cli/flag"
	_ "k8s.io/component-base/logs/json/register"
	"k8s.io/klog/v2"

	// Registers the "tatnet" cloud provider via its init().
	_ "github.com/tatnet-ru/tatnet-cloud-controller-manager/internal/tatnet"
)

func main() {
	ccmOptions, err := options.NewCloudControllerManagerOptions()
	if err != nil {
		klog.Fatalf("unable to initialize command options: %v", err)
	}

	fss := cliflag.NamedFlagSets{}
	controllerInitializers := app.DefaultInitFuncConstructors
	controllerAliases := names.CCMControllerAliases()

	command := app.NewCloudControllerManagerCommand(
		ccmOptions, cloudInitializer, controllerInitializers, controllerAliases, fss, wait.NeverStop,
	)

	os.Exit(cli.Run(command))
}

func cloudInitializer(cfg *config.CompletedConfig) cloudprovider.Interface {
	shared := cfg.ComponentConfig.KubeCloudShared
	cloud, err := cloudprovider.InitCloudProvider(shared.CloudProvider.Name, shared.CloudProvider.CloudConfigFile)
	if err != nil {
		klog.Fatalf("cloud provider could not be initialized: %v", err)
	}
	if cloud == nil {
		klog.Fatalf("cloud provider is nil")
	}
	return cloud
}
