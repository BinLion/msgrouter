package topic

import (
	"net/http"
	"strconv"

	"msgrouter/queue"
	"msgrouter/utils"
)

func Create(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	// topic
	topic := req.PostFormValue("topic")
	if len(topic) < 1 {
		utils.ResponseJson(w, -1, "topic is invaild", nil)
		return
	}

	// partitions
	partitionsString := req.PostFormValue("partitions")
	partitions, _ := strconv.Atoi(partitionsString)
	if partitions < 1 || partitions > 64 {
		utils.ResponseJson(w, -1, "partitions is invaild", nil)
		return
	}

	// replicationFactors
	replicationFactorsString := req.PostFormValue("replicationFactors")
	replicationFactors, _ := strconv.Atoi(replicationFactorsString)
	if replicationFactors < 1 || replicationFactors > 3 {
		utils.ResponseJson(w, -1, "replicationFactors is invaild", nil)
		return
	}

	err := queue.CreateTopic(topic, partitions, replicationFactors)

	if err != nil {
		utils.ResponseJson(w, -2, err.Error(), nil)
		return
	}

	utils.ResponseJson(w, 0, "", nil)
}
