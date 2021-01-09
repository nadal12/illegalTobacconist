package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"reflect"
	"unsafe"
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

	// Cua per enviar mistos.
	match, err := ch.QueueDeclare(
		"match", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Cua per realitzar les solicituts de tabac i mistos
	requests, err := ch.QueueDeclare(
		"requests", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Es declara com a consumidor de la cua de peticions de tabac i mistos.
	messages, err := ch.Consume(
		requests.Name, // queue
		"",            // consumer
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	failOnError(err, "Failed to register a consumer")

	// Declaraciones previas al bucle.
	var tobaccoNumber = 1
	var matchNumber = 1

	forever := make(chan bool)
	go func() {
		for d := range messages {

			// Mirar si el client ens ha demanat tabac o mistos.
			if bytesToString(d.Body) == "tabac" {
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
			} else if bytesToString(d.Body) == "misto" {
				message := fmt.Sprintf("Misto %d", matchNumber)
				err = ch.Publish(
					"",         // exchange
					match.Name, // routing key
					false,      // mandatory
					false,      // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(message),
					})
				failOnError(err, "Failed to publish a message")
				fmt.Printf("He posat el misto %d damunt la taula\n", matchNumber)
				matchNumber++
			}
		}
	}()
	<-forever
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
