package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "topic-0"
	brokerAddress = "localhost:29092"
	groupID       = "test-group"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	defer reader.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	stop := make(chan struct{}) // Channel to signal the loop to stop

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				msg, err := reader.ReadMessage(context.Background())
				if err != nil {
					log.Fatalf("Error while reading message: %v", err)
				}
				fmt.Printf("Received message: %s\n", msg.Value)
			}
		}
	}()

	<-sig       // Wait for termination signal
	close(stop) // Signal the loop to stop
}
