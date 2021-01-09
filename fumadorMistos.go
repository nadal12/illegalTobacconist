package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	match, err := ch.QueueDeclare(
		"match", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Es declara com a consumidor de la cua de mistos.
	messages, err := ch.Consume(
		match.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	// Cua per realitzar les solicituts de mistos.
	requests, err := ch.QueueDeclare(
		"requests", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fmt.Print("Som fumador. Tinc tabac però me falten mistos\n")

	for true {

		message := "misto"
		err = ch.Publish(
			"",            // exchange
			requests.Name, // routing key
			false,         // mandatory
			false,         // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		failOnError(err, "Failed to publish a message")

		for d := range messages {
			if strings.Contains(bytesToString(d.Body), "Misto") {
				fmt.Printf("He agafat el misto %s. Gràcies!\n", d.Body)
				break
			} else if bytesToString(d.Body) == "policia" {
				fmt.Printf("\nAnem que ve la policia!")
				os.Exit(0)
			}
		}

		time.Sleep(2 * time.Second)
		fmt.Printf(". . .\nMe dones un altre misto?\n")
	}
}

func bytesToString(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
