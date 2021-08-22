package storage

import (
	"encoding/json"
	"fmt"
	"log"
)

const TopicsPath = "/brokers/topics"

type TopicDetail struct {
	Topic      string
	Version    int              `json:version`
	Partitions map[string][]int `json:partitions`
}

func GetTopicDetails() []TopicDetail {
	result, _, err := ZkConnection.Children(TopicsPath)
	var topicDetails []TopicDetail

	if err != nil {
		log.Printf("topic children get failed. err:[%v], path:%v", err, TopicsPath)
		return topicDetails
	}

	for _, topic := range result {
		path := fmt.Sprintf("%s/%s", TopicsPath, topic)
		r, _, err := ZkConnection.Get(path)
		if err != nil {
			log.Printf("topic detail get failed. err:%v, path:%v", err, path)
			continue
		}

		var topicDetail TopicDetail
		err = json.Unmarshal(r, &topicDetail)
		if err != nil {
			log.Printf("topic detail json unmarshal failed. err:%v, path:%v, result:%v", err, path, r)
			continue
		}

		topicDetail.Topic = topic
		topicDetails = append(topicDetails, topicDetail)
	}

	log.Printf("topicDetails get success. len:%v, topics:%v", len(topicDetails), topicDetails)
	return topicDetails
}
