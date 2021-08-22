package common

import (
	"net/http"
	"runtime"
	"runtime/debug"
	"time"

	"msgrouter/utils"
)

func Status(w http.ResponseWriter, req *http.Request) {
	status := map[string]interface{}{}

	status["os"] = runtime.GOOS
	status["cpu"] = runtime.NumCPU()
	status["goroutine"] = runtime.NumGoroutine()

	gcstats := &debug.GCStats{PauseQuantiles: make([]time.Duration, 100)}
	debug.ReadGCStats(gcstats)
	status["gcstats"] = gcstats

	utils.ResponseJson(w, 0, "success", status)
}
