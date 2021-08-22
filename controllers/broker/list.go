package broker

import (
	"net/http"

	"msgrouter/storage"
	"msgrouter/utils"
)

// 查看Brokers列表
func List(w http.ResponseWriter, req *http.Request) {
	brokers := storage.GetBrokerList()

	utils.ResponseJson(w, 0, "", map[string]interface{}{
		"brokers": brokers,
	})
}
