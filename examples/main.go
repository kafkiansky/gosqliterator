package main

import (
	"github.com/kafkiansky/gosqliterator"
	"log"
)

func main() {
	iterator := gosqliterator.Iterate(func(q string, args ...interface{}) ([]string, error) {
		return []string{"event_1", "event_2"}, nil
	}, gosqliterator.Q("SELECT * FROM events").WithLimit(2))

	for iterator.Valid() {
		events, err := iterator.Next()
		if err != nil {
			panic(err)
		}

		for _, event := range events {
			log.Println(event)
		}
	}
}
