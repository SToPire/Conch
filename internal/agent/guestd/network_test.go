// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Description: Network configuration tests for conch-agent PID 1

package guestd

import (
	"net"
	"testing"
)

func TestFirstNonLoopbackInterfaceName(t *testing.T) {
	tests := []struct {
		name       string
		interfaces []net.Interface
		want       string
	}{
		{
			name: "skips loopback name",
			interfaces: []net.Interface{
				{Name: "lo"},
				{Name: "eth0"},
			},
			want: "eth0",
		},
		{
			name: "skips loopback flag",
			interfaces: []net.Interface{
				{Name: "tap0", Flags: net.FlagLoopback},
				{Name: "ens3", Flags: net.FlagBroadcast},
			},
			want: "ens3",
		},
		{
			name: "returns empty when no usable interface exists",
			interfaces: []net.Interface{
				{Name: "lo", Flags: net.FlagLoopback},
				{Name: ""},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstNonLoopbackInterfaceName(tt.interfaces); got != tt.want {
				t.Fatalf("firstNonLoopbackInterfaceName() = %q, want %q", got, tt.want)
			}
		})
	}
}
