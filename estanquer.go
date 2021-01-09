package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func main() {
	fmt.Print("Hola, som l'estanquer il·legal\n")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Cua per enviar tabac.
	tobacco, err := ch.QueueDeclare(
		"tabac", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

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

	// Es declara com a consumidor de la cua de peticions de tabac.
	messages, err := ch.Consume(
		tobaccoOrder.Name, // queue
		"",                // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	// Declaraciones previas al bucle.
	var tobaccoNumber = 1

	forever := make(chan bool)
	go func() {
		for range messages {
			message := fmt.Sprintf("Tabac %d", tobaccoNumber)
			err = ch.Publish(
				"",           // exchange
				tobacco.Name, // routing key
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(message),
				})
			failOnError(err, "Failed to publish a message")
			fmt.Printf("He posat el tabac %d damunt la taula\n", tobaccoNumber)
			tobaccoNumber++
		}
	}()

	<-forever

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
