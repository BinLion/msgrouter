package consumer

import (
	"net/http"
	"strconv"

	"msgrouter/storage"
	"msgrouter/utils"
)

// 创建消费者
func SetStatus(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.PostFormValue("key")

	// 状态
	statusString := req.PostFormValue("status")
	status, _ := strconv.Atoi(statusString)
	if status != 0 && status != 1 {
		utils.ResponseJson(w, -1, "status is invaild", nil)
		return
	}

	err := storage.SetConsumerStatus(key, status)
	if err != nil {
		utils.ResponseJson(w, -2, err.Error(), nil)
		return
	}

	utils.ResponseJson(w, 0, "success", nil)
	return
}
