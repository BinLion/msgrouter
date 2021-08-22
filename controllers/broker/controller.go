package broker

import (
	"net/http"

	"msgrouter/storage"
	"msgrouter/utils"
)

// 查看Controller
func Controller(w http.ResponseWriter, req *http.Request) {
	result := storage.GetController()

	utils.ResponseJson(w, 0, "", map[string]interface{}{
		"controller": result,
	})
}
