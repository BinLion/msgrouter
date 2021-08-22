package queue

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"

	"msgrouter/storage"
	"msgrouter/utils"
)

var WorkerPool utils.WorkerPool

// 重试topic前缀
const RetryTopicPrefix = "retry_topic__"

// consumer配置定时加载时间间隔
const CONSUMER_CONF_LOOP_INTERVAL = 10

// 消费者Worker参数
type ConsumerWorkerArgs struct {
	Brokers   []string
	GroupName string
	Topics    []string
	//ConsumerConf storage.ConsumerConf
	Url         string
	Retry       int
	ClusterConf cluster.Config
}

func ConsumerLoopAsync() {
	go consumerLoop()
}

// consumer定时加载
func consumerLoop() {
	sarama.Logger = log.New(&utils.SaramaLoggerWriter, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)

	clusterConfig := cluster.NewConfig()
	clusterConfig.ClientID = "msgrouter-consumer"

	// 版本必须设置，低版本不支持Header
	clusterConfig.Version = sarama.V1_0_0_0
	//clusterConfig.Group.Return.Notifications = true

	//brokers := []string{"10.20.69.15:9092"}
	brokers := storage.GetBrokerHosts()

	WorkerPool = utils.NewWorkerPool()

	for {
		consumersConf := storage.GetUsableConsumers()
		serverCount, ServerIndex := storage.GetServerCountAndCurrentIndex()

		// 处理每个consumer group
		for _, conf := range consumersConf {
			groupName := fmt.Sprintf("msgrouter-group-%s", conf.Key)
			topics := []string{conf.Topic}
			clusterConfig.Consumer.Offsets.Initial = conf.GetOffsetsInitial()

			args := ConsumerWorkerArgs{
				Brokers:     brokers,
				GroupName:   groupName,
				Topics:      topics,
				Url:         conf.Url,
				Retry:       conf.Retry,
				ClusterConf: *clusterConfig,
			}

			// 计算要起的进程数
			// seed用来将顺序随机化
			seed, _ := strconv.Atoi(conf.Key)
			processCount := utils.CalcProcessCount(conf.Count, serverCount, (ServerIndex+seed)%serverCount)

			// 启动Worker
			WorkerPool.Start(conf.Key, processCount, ConsumerWorker, args)
		}

		// 关闭不使用的
		usingKeys := map[string]int{}
		for _, conf := range consumersConf {
			usingKeys[conf.Key] = 1
		}

		for k, _ := range WorkerPool.Counter {
			if _, ok := usingKeys[k]; ok {
				continue
			}

			WorkerPool.Stop(k)
			log.Printf("worker pool stop. key:%v", k)
		}

		time.Sleep(time.Second * CONSUMER_CONF_LOOP_INTERVAL)
	}
}

// 消费者worker，可以起多个
func ConsumerWorker(argsInput interface{}, workerId int, control <-chan int) {
	args, ok := argsInput.(ConsumerWorkerArgs)
	if !ok {
		log.Printf("consumer worker args input error.")
		return
	}

	log.Printf("consumer worker start. topic:%v, consumer:%v, worker:%v", args.Topics, args.GroupName, workerId)

	// 连接Brokers
	consumer, err := cluster.NewConsumer(args.Brokers, args.GroupName, args.Topics, &args.ClusterConf)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	for {
		select {
		case status := <-control:
			// 结束队列信号
			if status == 1 {
				return
			}

		case msg, ok := <-consumer.Messages():
			if !ok {
				log.Printf("consumer message read failed. topic:%v, consumer:%v, worker:%v", args.Topics, args.GroupName, workerId)
				continue
			}

			log.Printf("consumer message read. topic:%v, consumer:%v, worker:%v, partition:%v, offset:%v, msg:%s",
				args.Topics, args.GroupName, workerId, msg.Partition, msg.Offset, msg.Value)

			url := args.Url
			topic := msg.Topic

			// 处理header
			headerMap := GetHeaderMap(msg.Headers)

			// 如果是重试topic
			if headerMap["url"] != "" {
				url = headerMap["url"]
				topic = headerMap["topic"]
				nextTime, _ := strconv.Atoi(headerMap["nextTime"])

				// 未到达时间等待
				WaitToTime(int64(nextTime))
			}

			times, _ := strconv.Atoi(headerMap["times"])

			// 发起请求
			result := ConsumerRequest(url, topic, times, workerId, msg.Value)

			// 消费失败处理，重试
			if result != true {
				if args.Retry > 0 {
					headerMap["url"] = url
					headerMap["topic"] = topic
					ProduceRetryMessage(msg, headerMap)
				}
			}

			// 标记offset，确认消费完成
			consumer.MarkOffset(msg, "")
		}
	}
}

// 获取header map
func GetHeaderMap(headers []*sarama.RecordHeader) map[string]string {
	headerMap := map[string]string{
		//"times" : "",
		//"topic" : "",
		//"nextTime" : "",
		//"url" : "",
	}

	for _, v := range headers {
		key := string(v.Key)
		//if _, ok := headerMap[key]; ok {
		headerMap[key] = string(v.Value)
		//}
	}

	return headerMap
}

// 等待
func WaitToTime(nextTime int64) {
	waitTime := time.Unix(nextTime, 0).Sub(time.Now())

	if waitTime > 0 {
		log.Printf("sleep. time:%v", waitTime.Seconds())
		time.Sleep(waitTime)
	}

	// 默认sleep 1秒，可预防错误数据
	if nextTime == 0 {
		time.Sleep(time.Second)
	}
}

// 生产重试消息
func ProduceRetryMessage(msg *sarama.ConsumerMessage, headerMap map[string]string) {
	times, _ := strconv.Atoi(headerMap["times"])
	times = times + 1

	topic := fmt.Sprintf("%s%d", RetryTopicPrefix, times)

	// 计算下次执行时间
	nextTime := int(time.Now().Unix()) + int(math.Pow(2.0, float64(times)))*60

	// 必须写成功
	for {
		headerMap["times"] = strconv.Itoa(times)
		headerMap["nextTime"] = strconv.Itoa(nextTime)

		partition, offset, err := Produce(topic, string(msg.Value), headerMap)

		// 失败必须阻塞重试，否则只能跳过导致丢失。不能先放内存异步重试，因为可能丢失。
		if err != nil {
			log.Printf("produce retry message failed. topic:%v, err:%v", topic, err)
			time.Sleep(time.Second * 3)
			continue
		}

		log.Printf("produce retry message success. topic:%v, partition:%v, offset:%v", topic, partition, offset)
		return
	}
}
