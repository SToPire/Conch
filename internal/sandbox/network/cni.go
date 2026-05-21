package network

import (
	"context"
	"fmt"
	"net"
	"strings"

	cni "github.com/containerd/go-cni"
)

const (
	defaultCNIIfName          = "eth0"
	defaultCNIInterfacePrefix = "eth"
	defaultCNIPluginConfDir   = "/etc/cni/net.d"
	defaultCNIPluginBinDir    = "/opt/cni/bin"
	defaultCNIPluginMaxConf   = 1
)

type NamespaceOpts = cni.NamespaceOpts

type CNIManagerConfig struct {
	PluginBinDirs   []string
	PluginConfDir   string
	PluginMaxConf   int
	InterfaceName   string
	SetupSerially   bool
	MinNetworkCount int
}

type CNIManager struct {
	plugin       cni.CNI
	config       CNIManagerConfig
	selectedConf string
}

type CNIResult struct {
	IP            string
	AdditionalIPs []string
	Interfaces    []CNIInterface
	Routes        []CNIRoute
	DNS           []CNIDNS
}

type CNIInterface struct {
	Name      string
	Mac       string
	Sandbox   string
	IPConfigs []CNIIPConfig
}

type CNIIPConfig struct {
	IP      string
	Gateway string
}

type CNIRoute struct {
	Dst string
	GW  string
}

type CNIDNS struct {
	Nameservers []string
	Domain      string
	Search      []string
	Options     []string
}

func NewCNIManager(cfg CNIManagerConfig) (*CNIManager, error) {
	cfg = normalizeCNIManagerConfig(cfg)
	plugin, err := cni.New(
		cni.WithMinNetworkCount(cfg.MinNetworkCount),
		cni.WithPluginConfDir(cfg.PluginConfDir),
		cni.WithPluginMaxConfNum(cfg.PluginMaxConf),
		cni.WithPluginDir(cfg.PluginBinDirs),
		cni.WithInterfacePrefix(interfacePrefix(cfg.InterfaceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cni: %w", err)
	}
	if err := plugin.Load(cni.WithLoNetwork, cni.WithDefaultConf); err != nil {
		return nil, fmt.Errorf("failed to load cni config: %w", err)
	}

	manager := &CNIManager{
		plugin: plugin,
		config: cfg,
	}
	manager.selectedConf = selectedCNIConfigName(plugin)
	return manager, nil
}

func normalizeCNIManagerConfig(cfg CNIManagerConfig) CNIManagerConfig {
	if len(cfg.PluginBinDirs) == 0 {
		cfg.PluginBinDirs = []string{defaultCNIPluginBinDir}
	}
	if cfg.PluginConfDir == "" {
		cfg.PluginConfDir = defaultCNIPluginConfDir
	}
	if cfg.PluginMaxConf == 0 {
		cfg.PluginMaxConf = defaultCNIPluginMaxConf
	}
	if cfg.InterfaceName == "" {
		cfg.InterfaceName = defaultCNIIfName
	}
	if cfg.MinNetworkCount == 0 {
		// loopback plus the outer sandbox network
		cfg.MinNetworkCount = 2
	}
	return cfg
}

func interfacePrefix(ifName string) string {
	if ifName == "" {
		return defaultCNIInterfacePrefix
	}
	prefix := strings.TrimRight(ifName, "0123456789")
	if prefix == "" {
		return defaultCNIInterfacePrefix
	}
	return prefix
}

func selectedCNIConfigName(plugin cni.CNI) string {
	config := plugin.GetConfig()
	if config == nil || len(config.Networks) == 0 || config.Networks[0] == nil || config.Networks[0].Config == nil {
		return ""
	}
	return config.Networks[0].Config.Name
}

func (m *CNIManager) SelectCNIPluginAndConfig(slot *Slot) (cni.CNI, string, error) {
	if m == nil || m.plugin == nil {
		return nil, "", fmt.Errorf("cni config not initialized")
	}
	if slot == nil {
		return nil, "", fmt.Errorf("slot is nil")
	}
	// Bridge sharding remains Conch policy. A deployment can map each shard to
	// a CNI config directory before constructing this manager; this exposes the
	// selected config name for callers that need to record that policy decision.
	return m.plugin, m.selectedConf, nil
}

func buildCNIOpts(slot *Slot, cniID, netnsPath string) ([]NamespaceOpts, error) {
	if slot == nil {
		return nil, fmt.Errorf("slot is nil")
	}
	if cniID == "" {
		return nil, fmt.Errorf("cniID is required")
	}
	if netnsPath == "" {
		return nil, fmt.Errorf("netnsPath is required")
	}
	return []NamespaceOpts{
		cni.WithLabels(map[string]string{
			"K8S_POD_NAMESPACE":          "conch",
			"K8S_POD_NAME":               cniID,
			"K8S_POD_INFRA_CONTAINER_ID": cniID,
			"CONCH_NETWORK_SLOT":         slot.Key,
			"CONCH_BRIDGE_SHARD":         slot.BridgeName(),
			"IgnoreUnknown":              "1",
		}),
	}, nil
}

func (m *CNIManager) SetupPodNetwork(ctx context.Context, cniID string, netnsPath string, opts ...NamespaceOpts) (*CNIResult, error) {
	if m == nil || m.plugin == nil {
		return nil, fmt.Errorf("cni config not initialized")
	}
	var (
		result *cni.Result
		err    error
	)
	if m.config.SetupSerially {
		result, err = m.plugin.SetupSerially(ctx, cniID, netnsPath, opts...)
	} else {
		result, err = m.plugin.Setup(ctx, cniID, netnsPath, opts...)
	}
	if err != nil {
		return nil, err
	}
	return convertCNIResult(result, m.config.InterfaceName)
}

func (m *CNIManager) TeardownPodNetwork(ctx context.Context, cniID string, netnsPath string, opts ...NamespaceOpts) error {
	if cniID == "" || netnsPath == "" {
		return nil
	}
	if m == nil || m.plugin == nil {
		return fmt.Errorf("cni config not initialized")
	}
	return m.plugin.Remove(ctx, cniID, netnsPath, opts...)
}

func convertCNIResult(result *cni.Result, defaultIfName string) (*CNIResult, error) {
	if result == nil {
		return nil, fmt.Errorf("cni returned nil result")
	}
	if defaultIfName == "" {
		defaultIfName = defaultCNIIfName
	}

	out := &CNIResult{
		Interfaces: make([]CNIInterface, 0, len(result.Interfaces)),
		Routes:     make([]CNIRoute, 0, len(result.Routes)),
		DNS:        make([]CNIDNS, 0, len(result.DNS)),
	}

	for name, iface := range result.Interfaces {
		if iface == nil {
			continue
		}
		converted := CNIInterface{
			Name:      name,
			Mac:       iface.Mac,
			Sandbox:   iface.Sandbox,
			IPConfigs: make([]CNIIPConfig, 0, len(iface.IPConfigs)),
		}
		for _, ipConfig := range iface.IPConfigs {
			if ipConfig == nil {
				continue
			}
			converted.IPConfigs = append(converted.IPConfigs, CNIIPConfig{
				IP:      ipConfig.IP.String(),
				Gateway: ipToString(ipConfig.Gateway),
			})
		}
		out.Interfaces = append(out.Interfaces, converted)
	}

	for _, route := range result.Routes {
		if route == nil {
			continue
		}
		out.Routes = append(out.Routes, CNIRoute{
			Dst: route.Dst.String(),
			GW:  ipToString(route.GW),
		})
	}
	for _, dns := range result.DNS {
		cniDNS := CNIDNS{
			Nameservers: dns.Nameservers,
			Domain:      dns.Domain,
			Search:      dns.Search,
			Options:     dns.Options,
		}
		out.DNS = append(out.DNS, cniDNS)
	}

	defaultIface := result.Interfaces[defaultIfName]
	if defaultIface == nil || len(defaultIface.IPConfigs) == 0 {
		for _, iface := range result.Interfaces {
			if iface != nil && len(iface.IPConfigs) > 0 {
				defaultIface = iface
				break
			}
		}
	}
	if defaultIface == nil || len(defaultIface.IPConfigs) == 0 {
		return nil, fmt.Errorf("failed to find network info for sandbox interface %q", defaultIfName)
	}

	for _, ipConfig := range defaultIface.IPConfigs {
		if ipConfig == nil {
			continue
		}
		ip := ipConfig.IP.String()
		if out.IP == "" && ipConfig.IP.To4() != nil {
			out.IP = ip
			continue
		}
		out.AdditionalIPs = append(out.AdditionalIPs, ip)
	}
	if out.IP == "" {
		out.IP = defaultIface.IPConfigs[0].IP.String()
		if len(out.AdditionalIPs) > 0 && out.AdditionalIPs[0] == out.IP {
			out.AdditionalIPs = out.AdditionalIPs[1:]
		}
	}

	return out, nil
}

func ipToString(ip net.IP) string {
	if ip == nil {
		return ""
	}
	return ip.String()
}
