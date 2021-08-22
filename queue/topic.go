package queue

import (
	"log"

	"github.com/Shopify/sarama"

	"msgrouter/storage"
)

// 创建一个topic
// 数据结构参考以下文档:
// https://godoc.org/github.com/Shopify/sarama#TopicDetail
func CreateTopic(topicName string, partitions int, replicationFactors int) error {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

	brokers := storage.GetBrokerHosts()

	request := &sarama.CreateTopicsRequest{}
	request.TopicDetails = make(map[string]*sarama.TopicDetail)
	request.TopicDetails[topicName] = &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactors),
	}

	broker := sarama.NewBroker(brokers[0])
	broker.Open(config)
	defer broker.Close()

	resp, err := broker.CreateTopics(request)
	if err != nil {
		log.Printf("create topic failed. err:%v", err)
		return err
	}
	log.Printf("create topic success. resp:%v", resp.TopicErrors)
	return nil
}
