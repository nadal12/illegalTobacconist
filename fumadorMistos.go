/*
Nom i llinatges: Nadal Llabrés Belmar.
Enllaç al vídeo:
*/

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

	// Crear connexió.
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Obrir canal.
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declarar cua de mistos.
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

	// Declarar cua per realitzar les solicituts de mistos.
	requests, err := ch.QueueDeclare(
		"requests", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Presentació inicial.
	fmt.Print("Som fumador. Tinc tabac però me falten mistos\n")

	for true {

		// Demanar misto.
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

		// Llegir el missatge rebut.
		for d := range messages {

			// El missatge és un misto, notifica i surt del bucle.
			if strings.Contains(bytesToString(d.Body), "Misto") {
				fmt.Printf("He agafat el misto %s. Gràcies!\n", d.Body)
				break
			} else if bytesToString(d.Body) == "policia" { //Ve la policia.

				//Avisar als altres posibles companys de que hi ha policia.
				message := "policia"
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

				// Mostrar missatge i sortir
				fmt.Printf("\nAnem que ve la policia!")
				os.Exit(0)
			}
		}

		// Espera de dos segons per demanar més mistos.
		time.Sleep(2 * time.Second)
		fmt.Printf(". . .\nMe dones un altre misto?\n")
	}
}

/*
Funció que Converteix un array de bytes a String.
*/
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
