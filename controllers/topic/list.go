package topic

import (
	"net/http"

	"github.com/Shopify/sarama"

	"msgrouter/storage"
	"msgrouter/utils"
)

// 查看Topics列表
func List(w http.ResponseWriter, req *http.Request) {
	brokers := storage.GetBrokerHosts()

	config := sarama.NewConfig()
	config.ClientID = "msgrouter-topics"
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		utils.ResponseJson(w, -2, err.Error(), nil)
		return
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		utils.ResponseJson(w, -2, err.Error(), nil)
		return
	}

	utils.ResponseJson(w, 0, "", map[string]interface{}{
		"topics": topics,
	})
}
