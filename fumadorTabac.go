package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	tobacco, err := ch.QueueDeclare(
		"tabac", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Es declara com a consumidor de la cua de tabac.
	messages, err := ch.Consume(
		tobacco.Name, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	// Cua per realitzar les solicituts de tabac
	tobaccoOrder, err := ch.QueueDeclare(
		"demanarTabac", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fmt.Print("Som fumador. Tinc mistos però me falta tabac\n")

	for true {

		message := "Vull tabac"
		err = ch.Publish(
			"",                // exchange
			tobaccoOrder.Name, // routing key
			false,             // mandatory
			false,             // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		failOnError(err, "Failed to publish a message")

		for d := range messages {
			fmt.Printf("He agafat el tabac %s. Gràcies!\n", d.Body)
			break
		}

		time.Sleep(2 * time.Second)
		fmt.Printf(". . .\nMe dones més tabac?\n")
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
