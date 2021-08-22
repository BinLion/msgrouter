package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/samuel/go-zookeeper/zk"

	"msgrouter/utils"
)

const ServersPath = "/msgrouter/servers"

type ServerNode struct {
	Key       string `json:"-"`
	Host      string
	Port      int
	Weight    int
	StartTime int64
}

// 当前服务的结点信息
var CurrentServerNode ServerNode

// 初始化当前服务器结点信息
func CurrentServerNodeInitAndRegister(host string, port int, weight int) error {
	CurrentServerNode = ServerNode{
		Host:      host,
		Port:      port,
		Weight:    weight,
		StartTime: time.Now().Unix(),
	}

	return CurrentServerNode.Register()
}

// 注册server
func (this *ServerNode) Register() error {
	// 获取本机ip
	if this.Host == "" {
		var err error
		this.Host, err = utils.GetLocalIp()
		if err != nil {
			return err
		}
	}

	path := getServerPath("")
	data, _ := json.Marshal(this)

	//name, err := ZkConnection.CreateProtectedEphemeralSequential(path, data, zk.WorldACL(zk.PermAll))
	name, err := ZkConnection.Create(path, data, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))

	log.Printf("register server start. path:%v, name:%v, err:%v", path, name, err)
	if err != nil {
		log.Printf("register server failed. err:%v", err)
		return err
	}

	this.Key = name
	log.Printf("register server success. host:%v, port:%v, key:%v", this.Host, this.Port, name)

	return nil
}

// 获取服务器列表
func getServerList() []string {
	result, _, err := ZkConnection.Children(ServersPath)

	// 配置读取失败
	if err != nil {
		log.Printf("server children get failed. err:[%v], path:%v", err, ServersPath)
		return []string{}
	}

	return result
}

// 获取服务器总数与当前机器的位置
func GetServerCountAndCurrentIndex() (int, int) {
	list := getServerList()

	var key string
	if len(CurrentServerNode.Key) > len(ServersPath)+1 {
		key = CurrentServerNode.Key[len(ServersPath)+1:]
	}

	log.Printf("GetServerCountAndCurrentIndex. serverlist:%v, key:%v", list, key)

	// 寻找本机的位置
	for k, v := range list {
		if v == key {
			return len(list), k
		}
	}

	// 如果在server list中没有找到，则尝试重新注册
	log.Printf("current server node is lost. will register retry. list:%v", list)
	CurrentServerNode.Register()

	return 0, 0
}

// 获取所有的server
func GetAllServers() []ServerNode {
	servers := getServerList()

	var result []ServerNode

	for _, v := range servers {
		detailJson, _, err := ZkConnection.Get(getServerPath(v))
		if err != nil {
			continue
		}

		var detail ServerNode
		err = json.Unmarshal([]byte(detailJson), &detail)
		if err != nil {
			continue
		}

		result = append(result, detail)
	}

	log.Printf("servers get success. len:%v", len(result))

	return result
}

// 拼接path
func getServerPath(key string) string {
	return fmt.Sprintf("%s/%s", ServersPath, key)
}
