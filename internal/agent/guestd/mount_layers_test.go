package guestd

import (
	"reflect"
	"testing"
)

func TestPmemDeviceOrderPreservesMultilayerRootfs(t *testing.T) {
	got := orderedPmemDevices([]string{"/dev/pmem0", "/dev/pmem1", "/dev/pmem10", "/dev/pmem11", "/dev/pmem2", "/dev/pmem9", "/dev/pmem1p1", "/dev/pmemfoo"})
	want := []string{"/dev/pmem0", "/dev/pmem1", "/dev/pmem2", "/dev/pmem9", "/dev/pmem10", "/dev/pmem11"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("devices = %v, want %v", got, want)
	}
}
