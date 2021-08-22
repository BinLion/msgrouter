package consumer

import (
	"net/http"

	"msgrouter/queue"
	"msgrouter/utils"
)

func Workers(w http.ResponseWriter, req *http.Request) {
	var result []map[string]interface{}

	for k, v := range queue.WorkerPool.Counter {
		result = append(result, map[string]interface{}{
			"name":  k,
			"count": v,
		})
	}

	utils.ResponseJson(w, 0, "", result)
}
