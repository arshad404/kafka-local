package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "topic-0"
	brokerAddress = "localhost:29092"
)

func main() {
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		Balancer: &kafka.Hash{},
	})

	defer producer.Close()

	for i := 0; ; i++ {
		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i)),
			Value: []byte(fmt.Sprintf("Message-%d", i)),
		}

		err := producer.WriteMessages(context.Background(), message)
		if err != nil {
			log.Fatalf("Error while sending message: %v", err)
		}

		log.Printf("Sent message: %s", message.Value)
		time.Sleep(1 * time.Second)
	}
}
