package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"net/smtp"
)

func sendEmails(body string) {
	from := ""
	password := ""

	toEmailAddress := ""
	to := []string{toEmailAddress}

	host := "smtp.yandex.com"
	port := "465"
	address := host + ":" + port

	subject := "Subject: This is the subject of the mail\n"
	message := []byte(subject + body)

	auth := smtp.PlainAuth("", from, password, host)

	fmt.Println("AUTH: ", auth)

	err := smtp.SendMail(address, auth, from, to, message)
	if err != nil {
		panic(err)
	}

	fmt.Printf("SENT")
}

func consumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"emails"}, nil) // "^aRegex.*[Ee]mails"

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			go sendEmails(string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

func main() {
	consumer()
}
