package storage

import (
	"log"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var ZkConnection *zk.Conn

func ZkInit(host []string) {
	ZkConnect(host)
}

func ZkConnect(host []string) {
	var err error
	ZkConnection, _, err = zk.Connect(host, time.Second) //*10)
	if err != nil {
		panic(err)
	}

	log.Printf("zookeeper connect success. host:%v", host)
}
