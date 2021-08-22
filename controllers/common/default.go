package common

import (
	"net/http"
)

func Default(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("hello msgrouter"))
}
