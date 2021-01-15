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

	// Obrir connexió.
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Obrir canal.
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declarar cua de tabac.
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

	// Declarar cua per realitzar les solicituts de tabac
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
	fmt.Print("Som fumador. Tinc mistos però me falta tabac\n")

	for true {

		// Sol·licitar tabac.
		message := "tabac"
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

		// Llegir els missatges de la cua.
		for d := range messages {

			// Si és tabac, indicar que l'ha agafat i sortir del bucle.
			if strings.Contains(bytesToString(d.Body), "Tabac") {
				fmt.Printf("He agafat el tabac %s. Gràcies!\n", d.Body)
				break
			} else if bytesToString(d.Body) == "policia" { // Ve la policia.

				//Avisar als altres posibles companys de que hi ha policia.
				message := "policia"
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

				// Mostrar missatge i sortir.
				fmt.Printf("\nAnem que ve la policia!")
				os.Exit(0)
			}
		}

		// Espera de 2 segons per tornar a demanar tabac.
		time.Sleep(2 * time.Second)
		fmt.Printf(". . .\nMe dones més tabac?\n")
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
