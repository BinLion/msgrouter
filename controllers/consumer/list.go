package consumer

import (
	"net/http"

	"msgrouter/storage"
	"msgrouter/utils"
)

func List(w http.ResponseWriter, req *http.Request) {
	conf := storage.GetAllConsumers()

	utils.ResponseJson(w, 0, "", conf)
}
