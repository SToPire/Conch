// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Description: Kernel module loading for conch-agent PID 1

package guestd

import "github.com/openeuler/Conch/pkg/ulog"

func loadKernelModules(modules ...string) {
	logger := ulog.GetLogger()
	if !modprobeCommand.available() {
		logger.Warn("modprobe command is not available; skipping kernel module load")
		return
	}

	for _, module := range modules {
		if err := execModprobe(module).Run(); err != nil {
			logger.Warn("Failed to load kernel module", ulog.F("module", module), ulog.F("error", err))
			continue
		}
		logger.Info("Loaded kernel module", ulog.F("module", module))
	}
}
