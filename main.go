package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	"os"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}

	log.Printf("abc = %s", os.Getenv("abc"))
	var connString, connKey, connEndpoint, saslMechanism, securityProtocol, topic string

	if connString = os.Getenv("connection.string"); connString == "" {
		log.Fatal("no connection.string")
	}
	if connKey = os.Getenv("connection.key"); connKey == "" {
		log.Fatal("no connection.key")
	}
	if connEndpoint = os.Getenv("connection.endpoint"); connEndpoint == "" {
		log.Fatal("no connection.endpoint")
	}
	if saslMechanism = os.Getenv("sasl.mechanism"); saslMechanism == "" {
		log.Fatal("no sasl.mechanism")
	}
	if securityProtocol = os.Getenv("security.protocol"); securityProtocol == "" {
		log.Fatal("no connection.string")
	}
	if topic = os.Getenv("producer.topic"); topic == "" {
		log.Fatal("no producer.topic")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": connEndpoint,
		"sasl.mechanisms":   saslMechanism,
		"security.protocol": securityProtocol,
		"sasl.username":     "$ConnectionString",
		"sasl.password":     connString,
	})

	if err != nil {
		log.Fatal(err)
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			// log.Printf("rx msg %+v", e)
			// log.Printf("rx msg %+v", reflect.TypeOf(e))
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			case kafka.Error:
				log.Printf("error %v\n", e)
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	for _, word := range []string{"Welcome", "to", "the", "Kafka", "head", "on", "Azure", "EventHubs"} {
		log.Println("sending " + word)
		if err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil); err != nil {
			log.Printf("error is %+v ", err)
		}
	}

	// Wait for message deliveries
	p.Flush(5000)

}
