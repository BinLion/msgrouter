package consumer

import (
	"net/http"

	"msgrouter/storage"
	"msgrouter/utils"
)

func Detail(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.FormValue("key")
	if len(key) < 1 {
		utils.ResponseJson(w, -1, "key is invaild", nil)
		return
	}

	detail, err := storage.GetConsumerDetail(key)
	if err != nil {
		utils.ResponseJson(w, -1, err.Error(), nil)
		return
	}

	utils.ResponseJson(w, 0, "", detail)
}
