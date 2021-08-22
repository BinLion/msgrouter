package utils

import (
	"log"
	"net/http"
	"net/url"
)

var alarmApi = "http://itil.firstp2p.com/api/alarm/push"

var maxChan = make(chan int, 10)

func AlarmSend(key string, title, content string) {
	go alarmSend(key, title, content)
}

func alarmSend(key string, title string, content string) {
	maxChan <- 1
	defer func() {
		<-maxChan
	}()

	params := url.Values{
		"type":    {key},
		"title":   {title},
		"content": {content},
	}

	resp, err := http.PostForm(alarmApi, params)
	if err != nil {
		log.Printf("alarm send error. key:%v, title:%v, content:%v, err:%v", key, title, content, err)
		return
	}
	defer resp.Body.Close()

	log.Printf("alarm send. key:%v, title:%v, content:%v, resp:%v, err:%v, chanlen:%v", key, title, content, resp.Body, err, len(maxChan))
}
