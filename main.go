package main

import (
	"context"
	"fmt"
	"mini_cron/delayqueue"
	"time"
)

func main() {
	names := []string{"a", "b", "c", "d", "e", "f"}
	times := []int{1, 2, 3, 5, 8, 13}
	q := delayqueue.New[string]()
	for idx, t := range times {
		q.Push(names[idx], time.Second*time.Duration(t))
	}
	fmt.Println("start...")
	for _, name := range names {
		value, ok := q.Take(context.Background())
		if !ok {
			_ = fmt.Errorf("want %v, but %v", true, ok)
		}
		if value != name {
			_ = fmt.Errorf("want %v, but %v", name, value)
		}
		fmt.Println(name)
	}

}
