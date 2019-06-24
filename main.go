package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/streadway/amqp"
)

func consumeMessage(qName string) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("rabbit-smoke-test-queue", false, false, false, false, nil)
	if err != nil {
		return err
	}

	msg, _, err := ch.Get(q.Name, true)
	if err != nil {
		return err
	}
	fmt.Printf("Consumed message: %s from queue: %s\n", msg.Body, qName)
	return nil

}

func publishMessage(qName string) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("rabbit-smoke-test-queue", false, false, false, false, nil)
	if err != nil {
		return err
	}

	timeNow := time.Now()
	body := timeNow.String()
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
	if err != nil {
		return err
	}
	fmt.Printf("Published message: %s to queue: %s\n", body, qName)
	return nil
}

func queuesHandler(w http.ResponseWriter, r *http.Request) {
	qName := r.URL.Path[1:]

	if r.Method == "GET" {
		err := consumeMessage(qName)
		if err != nil {
			fmt.Println(err, "Failed to consume message from queue")
			http.Error(w, err.Error()+"Failed to consume message from queue", http.StatusInternalServerError)
		} else {
			w.Write([]byte("Successfully consumed message from queue"))
		}
	}

	if r.Method == "POST" {
		err := publishMessage(qName)
		if err != nil {
			fmt.Println(err, "Failed to publish message to queue")
			http.Error(w, err.Error()+"Failed to publish message to queue", http.StatusInternalServerError)
		} else {
			w.Write([]byte("Successfully published message to queue"))
		}
	}
}

func main() {
	http.HandleFunc("/queues/", queuesHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
