package storage

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

var ConsumerConfPath = "/msgrouter/consumers"

// 消费者配置
type ConsumerConf struct {
	Key string
	// 负责人
	Owner string
	// 订阅的Topic
	Topic string
	// 说明
	Title string
	// 并发数量
	Count int
	// 消费Url
	Url string
	// 初始消费的offset配置 (0最新，1最老)
	OffsetsInitial int
	// 是否重试：0不重试，1重试
	Retry int
	// 消费者运行状态：0正常，1停止
	Status int

	Version int32
	Ctime   int64
	Mtime   int64
}

// 获取初始消费的offset配置
func (this *ConsumerConf) GetOffsetsInitial() int64 {
	if this.OffsetsInitial == 0 {
		return sarama.OffsetNewest
	}

	return sarama.OffsetOldest
}

// 新增消费者配置
func CreateConsumer(topic string, title string, count int, url string, offsetsInitial int, retry int, status int, owner string) error {
	path := getPath("")

	data, _ := json.Marshal(ConsumerConf{
		Topic:          topic,
		Owner:          owner,
		Title:          title,
		Count:          count,
		Url:            url,
		OffsetsInitial: offsetsInitial,
		Retry:          retry,
		Status:         status,
	})

	name, err := ZkConnection.Create(path, data, zk.FlagSequence, zk.WorldACL(zk.PermAll))

	log.Printf("consumer create. path:%v, name:%v, err:%v", path, name, err)
	if err != nil {
		return err
	}

	return nil
}

// 修改消费者配置
func SetConsumer(key string, topic string, title string, count int, url string, offsetsInitial int, retry int, status int, owner string) error {
	path := getPath(key)

	data, _ := json.Marshal(ConsumerConf{
		Topic:          topic,
		Owner:          owner,
		Title:          title,
		Count:          count,
		Url:            url,
		OffsetsInitial: offsetsInitial,
		Retry:          retry,
		Status:         status,
	})

	stat, err := ZkConnection.Set(path, data, -1)

	log.Printf("consumer set. path:%v, stat:%+v, err:%v", path, stat, err)
	if err != nil {
		return err
	}

	return nil
}

// 设置消费者状态
func SetConsumerStatus(key string, status int) error {
	path := getPath(key)

	result, err := GetConsumerDetail(key)
	if err != nil {
		return err
	}

	err = SetConsumer(key, result.Topic, result.Title, result.Count, result.Url, result.OffsetsInitial, result.Retry, status, result.Owner)
	log.Printf("consumer set status. path:%v, status:%v, err:%v", path, status, err)

	if err != nil {
		return err
	}

	return nil
}

// 是否存在
func ConsumerExists(key string) bool {
	path := getPath(key)

	result, stat, err := ZkConnection.Exists(path)

	log.Printf("consumer exists. path:%v, result:%v, stat:%+v, err:%v", path, result, stat, err)
	if err != nil {
		return false
	}

	return result
}

// 删除一个consumer
func DeleteConsumer(key string) error {
	path := getPath(key)

	err := ZkConnection.Delete(path, -1)
	if err != nil {
		return err
	}

	return nil
}

// 消费者详情
func GetConsumerDetail(key string) (ConsumerConf, error) {
	path := getPath(key)
	result, stat, err := ZkConnection.Get(path)

	var conf ConsumerConf

	if err != nil {
		log.Printf("consumer detail get failed. err:%v, path:%v", err, path)
		return conf, err
	}

	err = json.Unmarshal(result, &conf)
	if err != nil {
		log.Printf("consumer detail unmarshal failed. err:%v", err)
		return conf, err
	}

	conf.Ctime = stat.Ctime
	conf.Mtime = stat.Mtime
	conf.Version = stat.Version

	return conf, nil
}

// 获取可用的消费者
func GetUsableConsumers() []ConsumerConf {
	var result []ConsumerConf
	consumers := GetAllConsumers()
	for _, v := range consumers {
		if v.Status == 0 {
			result = append(result, v)
		}
	}

	return result
}

// 获取所有的配置的详情
func GetAllConsumers() []ConsumerConf {
	consumers := getConsumerConfList()

	var result []ConsumerConf

	for _, v := range consumers {
		detail, err := GetConsumerDetail(v)
		if err != nil {
			continue
		}

		detail.Key = v
		result = append(result, detail)
	}

	log.Printf("consumer conf get success. len:%v", len(result))

	return result
}

// 获取消费者配置列表
func getConsumerConfList() []string {
	result, _, err := ZkConnection.Children(ConsumerConfPath)

	// 配置读取失败
	if err != nil {
		log.Printf("consumer children get failed. err:[%v], path:%v", err, ConsumerConfPath)
		return []string{}
	}

	return result
}

// 拼接path
func getPath(key string) string {
	return fmt.Sprintf("%s/%s", ConsumerConfPath, key)
}
