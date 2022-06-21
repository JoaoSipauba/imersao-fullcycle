package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"

	route2 "github.com/JoaoSipauba/imersaofsfc2-simulator/application/route"
	"github.com/JoaoSipauba/imersaofsfc2-simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

//{"clientId": "1", "routeId": "1"}
//{"clientId": "2", "routeId": "2"}
func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	route := route2.NewRoute()
	json.Unmarshal(msg.Value, &route)
	route.LoadPositions()
	position, err := route.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}
	for _, p := range position {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 500)
	}
}
