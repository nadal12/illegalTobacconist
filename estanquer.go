package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"reflect"
	"time"
	"unsafe"
)

func main() {
	fmt.Print("Hola, som l'estanquer il·legal\n")

	// Crear connexió
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Obrir canal
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

	// Cua per realitzar les solicituts de tabac, mistos o notificacions de la policia.
	requests, err := ch.QueueDeclare(
		"requests", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Es declara com a consumidor de la cua de peticions de tabac, mistos o avisos de policia.
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

			// Mirar si el client ens ha demanat tabac, mistos o ens avisa de la policia.
			if bytesToString(d.Body) == "tabac" {

				// Enviar tabac.
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

				// Mostrar per pantalla i incrementar el comptador de tabac.
				fmt.Printf("He posat el tabac %d damunt la taula\n", tobaccoNumber)
				tobaccoNumber++
			} else if bytesToString(d.Body) == "misto" {

				// Enviar misto
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

				// Mostrar per pantalla i incrementar el comptador de mistos.
				fmt.Printf("He posat el misto %d damunt la taula\n", matchNumber)
				matchNumber++
			} else if bytesToString(d.Body) == "policia" {

				// Avisar al fumador/s de tabac que ve la policia.
				message := fmt.Sprintf("policia")
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

				// Avisar al fumador/s de mistos que ve la policia.
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

				fmt.Printf("\nUyuyuy la policia! Men vaig\n")

				//Espera perque els clients tinguin temps d'agafar el missatge de la cua abans d'esborrar-la.
				time.Sleep(2 * time.Second)

				fmt.Printf(". . . Men duc la taula!!!!\n")

				//Esborrar cues.
				ch.QueueDelete(tobacco.Name, false, false, true)
				ch.QueueDelete(match.Name, false, false, true)
				ch.QueueDelete(requests.Name, false, false, true)

				//Tancar canal i connexió.
				ch.Close()
				conn.Close()

				//Sortir.
				os.Exit(0)
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
