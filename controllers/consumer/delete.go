package consumer

import (
	"net/http"

	"msgrouter/storage"
	"msgrouter/utils"
)

func Delete(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	key := req.FormValue("key")
	if len(key) < 1 {
		utils.ResponseJson(w, -1, "key is invaild", nil)
		return
	}

	err := storage.DeleteConsumer(key)
	if err != nil {
		utils.ResponseJson(w, -2, err.Error(), nil)
		return
	}

	utils.ResponseJson(w, 0, "success", nil)
	return
}
