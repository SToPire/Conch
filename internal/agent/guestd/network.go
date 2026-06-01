// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Description: Network configuration for conch-agent PID 1

package guestd

import (
	"net"
	"time"

	"github.com/openeuler/Conch/pkg/ulog"
)

func firstNonLoopbackInterfaceName(interfaces []net.Interface) string {
	for _, iface := range interfaces {
		if iface.Name == "" || iface.Name == "lo" || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		return iface.Name
	}
	return ""
}

func waitForNonLoopbackInterfaceName(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		interfaces, err := net.Interfaces()
		if err != nil {
			return "", err
		}
		if nicName := firstNonLoopbackInterfaceName(interfaces); nicName != "" {
			return nicName, nil
		}
		if time.Now().After(deadline) {
			return "", nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// setupNetwork brings up the loopback and first non-lo interface with a static IP.
func setupNetwork() {
	logger := ulog.GetLogger()
	// ip link set lo up
	if err := execIP("link", "set", "lo", "up").Run(); err != nil {
		logger.Warn("Failed to bring loopback up", ulog.F("error", err))
	}

	nicName, err := waitForNonLoopbackInterfaceName(2 * time.Second)
	if err != nil {
		logger.Error("Failed to get network interfaces", ulog.F("error", err))
		return
	}
	if nicName == "" {
		logger.Warn("No non-loopback network interface found")
		return
	}

	logger.Info("Configuring interface", ulog.F("name", nicName))

	// ip addr add 192.168.100.21/24 dev $NIC_NAME
	if err := execIP("addr", "add", "192.168.100.21/24", "dev", nicName).Run(); err != nil {
		logger.Warn("Failed to assign address", ulog.F("name", nicName), ulog.F("error", err))
	}
	// ip link set $NIC_NAME up
	if err := execIP("link", "set", nicName, "up").Run(); err != nil {
		logger.Warn("Failed to bring interface up", ulog.F("name", nicName), ulog.F("error", err))
	}
	// ip route add default via 192.168.100.2 dev $NIC_NAME
	if err := execIP("route", "add", "default", "via", "192.168.100.2", "dev", nicName).Run(); err != nil {
		logger.Warn("Failed to add default route", ulog.F("name", nicName), ulog.F("error", err))
	}

	// Add route for MMDS (169.254.169.254)
	if err := execIP("route", "add", "169.254.169.254/32", "dev", nicName).Run(); err != nil {
		logger.Warn("Failed to add MMDS route", ulog.F("name", nicName), ulog.F("error", err))
	}

	logger.Info("Network configured", ulog.F("name", nicName))
}
