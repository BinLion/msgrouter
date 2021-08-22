package queue

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// 请求包
type ConsumerRequestPkg struct {
	Topic     string
	WorkerId  int
	Times     int
	Timestamp int64
	Message   string
}

// 响应包
type ConsumerResponsePkg struct {
	Code    int    `json:code`
	Message string `json:message`
}

// 发起请求
func ConsumerRequest(url string, topic string, times int, workerId int, msgValue []byte) bool {
	params := ConsumerRequestPkg{
		Topic:     topic,
		WorkerId:  workerId,
		Times:     times,
		Timestamp: time.Now().UnixNano(),
		Message:   string(msgValue),
	}
	paramsJson, _ := json.Marshal(params)

	var err error

	start := time.Now()
	resp, err := http.Post(url, "application/json;charset=utf-8", bytes.NewReader(paramsJson))
	cost := time.Now().Sub(start).Seconds()

	if err != nil {
		log.Printf("consumer request failed. err:%v, cost:%0.3f, url:%v, params:%s", err, cost, url, paramsJson)
		return false
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("consumer response read error. err:%v, status:%v, cost:%0.3f, url:%v, params:%s, resp:%v", err, resp.StatusCode, cost, url, paramsJson, resp.Body)
		return false
	}

	bodyShort := body
	if len(body) > 200 {
		bodyShort = body[:200]
	}

	// 解析返回值
	var response ConsumerResponsePkg

	// 防止对方没传code，默认认为是0
	response.Code = -1

	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Printf("consumer response parse error. err:%v, status:%v, cost:%0.3f, url:%v, params:%s, resp:%s", err, resp.StatusCode, cost, url, paramsJson, bodyShort)
		return false
	}

	if response.Code != 0 {
		log.Printf("consumer response code error. status:%v, cost:%0.3f, url:%v, params:%s, resp:%s", resp.StatusCode, cost, url, paramsJson, bodyShort)
		return false
	}

	// 返回成功
	log.Printf("consumer response success. status:%v, cost:%0.3f, url:%v, params:%s, resp:%s", resp.StatusCode, cost, url, paramsJson, bodyShort)
	return true
}
