package topic

import (
	"net/http"

	"msgrouter/storage"
	"msgrouter/utils"
)

// 查询所有topic的详细信息
func Details(w http.ResponseWriter, req *http.Request) {
	topicDetails := storage.GetTopicDetails()
	utils.ResponseJson(w, 0, "success", topicDetails)
}
