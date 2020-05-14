package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	kafkaclientgo "github.com/Shopify/kafka-client-go/v2"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type ConsumerHandler struct {
	ready chan bool
}

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	sarama.Logger = logrus.StandardLogger()

	topics := os.Getenv("TOPICS")
	handler := ConsumerHandler{ready: make(chan bool)}
	consumer, err := kafkaclientgo.NewConsumerGroup(os.Getenv("GROUP_NAME"))
	if err != nil {
		logrus.Panic("couldn't create a consumer", err)
	}
	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumer.Consume(context.TODO(), strings.Split(topics, ","), &handler); err != nil {
				logrus.Panicf("Error from consumer: %v", err)
			}
			handler.ready = make(chan bool)
		}
	}()
	<-handler.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		logrus.Println("terminating: via signal")
	}
}

func (consumer *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}
	return nil
}
