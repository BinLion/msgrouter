package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
)

const BrokersPath = "/brokers/ids"

type BrokerNode struct {
	BrokerId  int
	Host      string `json:host`
	Port      int    `json:port`
	Timestamp string `json:timestamp`
	Version   int    `json:version`
}

type Controller struct {
	BrokerId  int    `json:brokerid`
	Timestamp string `json:timestamp`
	Version   int    `json:version`
}

// 获取可用的broker机器列表
func GetBrokerList() []BrokerNode {
	result, _, err := ZkConnection.Children(BrokersPath)

	// 配置读取失败
	if err != nil {
		log.Printf("brokers children get failed. err:[%v], path:%v", err, BrokersPath)
		return []BrokerNode{}
	}

	var brokers []BrokerNode

	for _, v := range result {
		path := fmt.Sprintf("%s/%s", BrokersPath, v)
		r, _, err := ZkConnection.Get(path)
		if err != nil {
			log.Printf("brokers detail get failed. err:%v, path:%v", err, path)
			continue
		}

		var brokerNode BrokerNode
		err = json.Unmarshal(r, &brokerNode)
		if err != nil {
			log.Printf("brokers detail json unmarshal failed. err:%v, path:%v, result:%v", err, path, r)
			continue
		}

		brokerNode.BrokerId, _ = strconv.Atoi(v)
		brokers = append(brokers, brokerNode)
	}

	log.Printf("brokers get success. len:%v, brokers:%v", len(brokers), brokers)
	return brokers
}

// 获取broker地址
func GetBrokerHosts() []string {
	brokers := GetBrokerList()

	var result []string
	for _, v := range brokers {
		result = append(result, fmt.Sprintf("%s:%d", v.Host, v.Port))
	}

	return result
}

// 获取控制结点信息
func GetController() Controller {
	var controller Controller

	path := "/controller"
	r, _, err := ZkConnection.Get(path)
	if err != nil {
		log.Printf("brokers controller get failed. err:%v, path:%v", err, path)
		return controller
	}

	err = json.Unmarshal(r, &controller)
	if err != nil {
		log.Printf("brokers controller json unmarshal failed. err:%v, path:%v, result:%v", err, path, r)
	}

	log.Printf("brokers controller get success. result:%v", r)
	return controller
}
