package producer

import (
	"net/http"

	"msgrouter/queue"
	"msgrouter/utils"
)

// 生产消息
func Produce(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	// topic
	topic := req.PostFormValue("topic")
	if len(topic) < 1 {
		utils.ResponseJson(w, -1, "topic is invaild", nil)
		return
	}

	// message
	message := req.PostFormValue("message")
	if len(message) < 1 {
		utils.ResponseJson(w, -1, "message is invaild", nil)
		return
	}

	headersMap := map[string]string{}

	partition, offset, err := queue.Produce(topic, message, headersMap)
	if err != nil {
		utils.ResponseJson(w, -1, err.Error(), nil)
		return
	}

	utils.ResponseJson(w, 0, "", map[string]interface{}{
		"partition": partition,
		"offset":    offset,
	})
}
