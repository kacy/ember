package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	ember "github.com/kacy/ember-go"
)

const (
	leaderboardKey = "match:42:scores"
	pubChannel     = "match:42:updates"
	matchTTL       = 24 * 60 * 60 // 24 hours in seconds
)

var players = []string{
	"shadow_knight", "pixel_rage", "nova_runner", "iron_wolf", "stellar_ace",
}

// simulate runs 10 rounds, awarding random points per player and publishing
// a notification after each round so the dashboard can re-render.
func simulate(client *ember.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	scores := make(map[string]float64, len(players))
	ctx := context.Background()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for round := 1; round <= 10; round++ {
		time.Sleep(300 * time.Millisecond)

		updates := make([]ember.ScoreMember, 0, len(players))
		for _, p := range players {
			scores[p] += float64(rng.Intn(51)) // 0–50 pts per round
			updates = append(updates, ember.ScoreMember{Member: p, Score: scores[p]})
		}

		if _, err := client.ZAdd(ctx, leaderboardKey, updates...); err != nil {
			log.Printf("zadd: %v", err)
			continue
		}
		client.Expire(ctx, leaderboardKey, matchTTL) //nolint:errcheck

		msg := fmt.Sprintf("round %d complete", round)
		if _, err := client.Publish(ctx, pubChannel, []byte(msg)); err != nil {
			log.Printf("publish: %v", err)
		}
	}
}

// dashboard subscribes to the match channel and re-renders the leaderboard
// every time the simulator posts a round result.
func dashboard(client *ember.Client, wg *sync.WaitGroup, done <-chan struct{}) {
	defer wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := client.Subscribe(ctx, []string{pubChannel}, nil)
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	for {
		select {
		case <-done:
			return
		case evt, ok := <-events:
			if !ok {
				return
			}

			top, err := client.ZRange(context.Background(), leaderboardKey, 0, 9, true)
			if err != nil {
				log.Printf("zrange: %v", err)
				continue
			}

			// ZRange returns ascending order; sort descending for display.
			sort.Slice(top, func(i, j int) bool {
				return top[i].Score > top[j].Score
			})

			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("\n[%s]\n", evt.Data))
			for rank, entry := range top {
				sb.WriteString(fmt.Sprintf("  %d. %-15s  %.0f pts\n", rank+1, entry.Member, entry.Score))
			}
			fmt.Print(sb.String())
		}
	}
}

func main() {
	client, err := ember.Dial("localhost:6380")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	client.Del(ctx, leaderboardKey) //nolint:errcheck

	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go dashboard(client, &wg, done)
	time.Sleep(100 * time.Millisecond) // let the subscription establish

	wg.Add(1)
	go simulate(client, &wg)

	// 10 rounds × 300ms + buffer
	time.Sleep(4 * time.Second)
	close(done)
	wg.Wait()

	fmt.Println("\nfinal standings:")
	top, err := client.ZRange(ctx, leaderboardKey, 0, 4, true)
	if err != nil {
		log.Fatalf("zrange: %v", err)
	}
	sort.Slice(top, func(i, j int) bool { return top[i].Score > top[j].Score })
	for rank, entry := range top {
		fmt.Printf("  %d. %-15s  %.0f pts\n", rank+1, entry.Member, entry.Score)
	}
}
