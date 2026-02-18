# live leaderboard

a competitive multiplayer game leaderboard with real-time rank changes. sorted sets give O(log n) score updates and rank queries; gRPC streaming pushes changes to a dashboard connection without polling.

## how it works

a simulation goroutine posts score updates for 5 players across 10 rounds using **ZADD**. after each round it publishes a message to a channel. a separate dashboard goroutine holds a gRPC **Subscribe** stream and on every message re-fetches the top-10 with **ZRANGE** (reversed by score), then prints the current standings. match keys carry a 24-hour **EXPIRE** so old matches clean themselves up.

## run it

```sh
cd examples/live-leaderboard
go mod tidy
go run main.go
```

## what to look for

each round the dashboard prints within milliseconds of the ZADD — the push latency of a streaming subscription is far lower than any polling interval you'd realistically use.

```
[round 1 complete]
  1. nova_runner        312 pts
  2. iron_wolf          289 pts
  3. shadow_knight      244 pts
  4. stellar_ace        201 pts
  5. pixel_rage         178 pts

[round 2 complete]
  1. nova_runner        589 pts
  ...
```
