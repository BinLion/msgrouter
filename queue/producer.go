package queue

import (
	"log"

	"github.com/Shopify/sarama"

	"msgrouter/storage"
)

var producer sarama.SyncProducer

// 最大可生产的消息体积 (128k)，设置为129是因为还有header占用体积
// kafka默认配置一般为1M
// kafka配置依赖socket.request.max.bytes和message.max.bytes中的最小值
var maxProduceMessageBytes = 129 * 1024

// 连接broker
func ProducerInit() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.MaxMessageBytes = maxProduceMessageBytes
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Version = sarama.V1_0_0_0
	config.ClientID = "msgrouter-producer"

	brokers := storage.GetBrokerHosts()

	var err error
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	//defer producer.Close()
}

// 控制并发
var maxChan = make(chan int, 100)

// 生产消息
func Produce(topic string, message string, headersMap map[string]string) (int32, int64, error) {
	maxChan <- 1
	defer func() {
		<-maxChan
	}()

	headers := []sarama.RecordHeader{}

	for k, v := range headersMap {
		headers = append(headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(message),
		Key:       sarama.StringEncoder("key"),
		Partition: int32(-1),
		Headers:   headers,
	}

	paritition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("produce message failed. topic:%v, err:%v, message:%v", topic, err, message)
		return -1, -1, err
	}

	log.Printf("produce message success. topic:%v, partition:%v, offset:%v, message:%v, header:%v", topic, paritition, offset, message, headersMap)
	return paritition, offset, nil
}
