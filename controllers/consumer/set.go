package consumer

import (
	"net/http"
	"strconv"
	"strings"

	"msgrouter/storage"
	"msgrouter/utils"
)

// 创建消费者
func Set(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.PostFormValue("key")

	// topic
	topic := req.PostFormValue("topic")
	if len(topic) < 1 {
		utils.ResponseJson(w, -1, "topic is invaild", nil)
		return
	}

	// 负责人
	owner := req.PostFormValue("owner")
	if len(owner) < 1 {
		utils.ResponseJson(w, -1, "owner is invaild", nil)
		return
	}

	// title
	title := req.PostFormValue("title")
	if len(topic) < 1 {
		utils.ResponseJson(w, -1, "title is invaild", nil)
		return
	}

	// 进程数
	countString := req.PostFormValue("count")
	count, _ := strconv.Atoi(countString)
	if count < 1 || count > 32 {
		utils.ResponseJson(w, -1, "count is invaild", nil)
		return
	}

	// 请求地址
	url := req.PostFormValue("url")
	if !strings.HasPrefix(url, "http") {
		utils.ResponseJson(w, -1, "url is invaild", nil)
		return
	}

	// offset初始化
	offsetsInitialS := req.PostFormValue("offsetsInitial")
	offsetsInitial, _ := strconv.Atoi(offsetsInitialS)
	if offsetsInitial != 0 && offsetsInitial != 1 {
		utils.ResponseJson(w, -1, "offsetsInitial is invaild", nil)
		return
	}

	// 是否重试
	retryS := req.PostFormValue("retry")
	retry, _ := strconv.Atoi(retryS)
	if retry != 0 && retry != 1 {
		utils.ResponseJson(w, -1, "retry is invaild", nil)
		return
	}

	// 状态
	statusS := req.PostFormValue("status")
	status, _ := strconv.Atoi(statusS)
	if status != 0 && status != 1 {
		utils.ResponseJson(w, -1, "status is invaild", nil)
		return
	}

	var err error

	if len(key) > 1 {
		// 修改
		err = storage.SetConsumer(key, topic, title, count, url, offsetsInitial, retry, status, owner)
	} else {
		// 创建
		err = storage.CreateConsumer(topic, title, count, url, offsetsInitial, retry, status, owner)
	}

	if err != nil {
		utils.ResponseJson(w, -2, err.Error(), nil)
		return
	}

	utils.ResponseJson(w, 0, "success", nil)
	return
}
