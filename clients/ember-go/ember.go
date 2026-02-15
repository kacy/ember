// Package ember provides a Go client for the ember cache server over gRPC.
//
// The client wraps the generated gRPC stubs with an idiomatic Go API,
// handling connection management and type conversions.
package ember

import (
	"context"
	"fmt"

	pb "github.com/kacy/ember-go/proto/ember/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Client is a gRPC client for ember.
type Client struct {
	conn *grpc.ClientConn
	rpc  pb.EmberCacheClient
	opts clientOptions
}

type clientOptions struct {
	password string
}

// Option configures the client.
type Option func(*clientOptions)

// WithPassword sets the authentication password.
func WithPassword(password string) Option {
	return func(o *clientOptions) {
		o.password = password
	}
}

// Dial connects to an ember server at the given address.
func Dial(addr string, opts ...Option) (*Client, error) {
	var o clientOptions
	for _, opt := range opts {
		opt(&o)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("ember: dial %s: %w", addr, err)
	}

	return &Client{
		conn: conn,
		rpc:  pb.NewEmberCacheClient(conn),
		opts: o,
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ctx adds auth metadata if a password is configured.
func (c *Client) ctx(parent context.Context) context.Context {
	if c.opts.password == "" {
		return parent
	}
	return metadata.AppendToOutgoingContext(parent, "authorization", c.opts.password)
}

// --- strings ---

// Get returns the value for a key, or nil if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.rpc.Get(c.ctx(ctx), &pb.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// SetOption configures a SET command.
type SetOption func(*pb.SetRequest)

// WithEX sets the expiration in seconds.
func WithEX(seconds uint64) SetOption {
	return func(r *pb.SetRequest) {
		r.ExpireSeconds = seconds
	}
}

// WithPX sets the expiration in milliseconds.
func WithPX(millis uint64) SetOption {
	return func(r *pb.SetRequest) {
		r.ExpireMillis = millis
	}
}

// WithNX only sets the key if it does not already exist.
func WithNX() SetOption {
	return func(r *pb.SetRequest) {
		r.Nx = true
	}
}

// WithXX only sets the key if it already exists.
func WithXX() SetOption {
	return func(r *pb.SetRequest) {
		r.Xx = true
	}
}

// Set stores a key-value pair. Returns true if the key was set.
func (c *Client) Set(ctx context.Context, key string, value []byte, opts ...SetOption) (bool, error) {
	req := &pb.SetRequest{Key: key, Value: value}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.rpc.Set(c.ctx(ctx), req)
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}

// Del removes the specified keys and returns the number deleted.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	resp, err := c.rpc.Del(c.ctx(ctx), &pb.DelRequest{Keys: keys})
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}

// Exists returns the number of keys that exist.
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	resp, err := c.rpc.Exists(c.ctx(ctx), &pb.ExistsRequest{Keys: keys})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// Incr increments a key by 1 and returns the new value.
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	resp, err := c.rpc.Incr(c.ctx(ctx), &pb.IncrRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// IncrBy increments a key by delta and returns the new value.
func (c *Client) IncrBy(ctx context.Context, key string, delta int64) (int64, error) {
	resp, err := c.rpc.IncrBy(c.ctx(ctx), &pb.IncrByRequest{Key: key, Delta: delta})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// Expire sets a timeout on a key in seconds. Returns true if the timeout was set.
func (c *Client) Expire(ctx context.Context, key string, seconds uint64) (bool, error) {
	resp, err := c.rpc.Expire(c.ctx(ctx), &pb.ExpireRequest{Key: key, Seconds: seconds})
	if err != nil {
		return false, err
	}
	return resp.Value, nil
}

// TTL returns the remaining time to live in seconds. Returns -1 if the key
// has no expiry, -2 if the key does not exist.
func (c *Client) TTL(ctx context.Context, key string) (int64, error) {
	resp, err := c.rpc.Ttl(c.ctx(ctx), &pb.TtlRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// --- lists ---

// LPush prepends values to a list and returns the new length.
func (c *Client) LPush(ctx context.Context, key string, values ...[]byte) (int64, error) {
	resp, err := c.rpc.LPush(c.ctx(ctx), &pb.LPushRequest{Key: key, Values: values})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// RPush appends values to a list and returns the new length.
func (c *Client) RPush(ctx context.Context, key string, values ...[]byte) (int64, error) {
	resp, err := c.rpc.RPush(c.ctx(ctx), &pb.RPushRequest{Key: key, Values: values})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// LPop removes and returns the first element of a list, or nil if empty.
func (c *Client) LPop(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.rpc.LPop(c.ctx(ctx), &pb.LPopRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// RPop removes and returns the last element of a list, or nil if empty.
func (c *Client) RPop(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.rpc.RPop(c.ctx(ctx), &pb.RPopRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// LRange returns elements from a list in the given range.
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	resp, err := c.rpc.LRange(c.ctx(ctx), &pb.LRangeRequest{Key: key, Start: start, Stop: stop})
	if err != nil {
		return nil, err
	}
	return resp.Values, nil
}

// LLen returns the length of a list.
func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	resp, err := c.rpc.LLen(c.ctx(ctx), &pb.LLenRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// --- hashes ---

// HSet sets fields in a hash. Returns the number of new fields added.
func (c *Client) HSet(ctx context.Context, key string, fields map[string][]byte) (int64, error) {
	fvs := make([]*pb.FieldValue, 0, len(fields))
	for f, v := range fields {
		fvs = append(fvs, &pb.FieldValue{Field: f, Value: v})
	}
	resp, err := c.rpc.HSet(c.ctx(ctx), &pb.HSetRequest{Key: key, Fields: fvs})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// HGet returns the value for a field in a hash, or nil if it doesn't exist.
func (c *Client) HGet(ctx context.Context, key, field string) ([]byte, error) {
	resp, err := c.rpc.HGet(c.ctx(ctx), &pb.HGetRequest{Key: key, Field: field})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// HGetAll returns all fields and values in a hash.
func (c *Client) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	resp, err := c.rpc.HGetAll(c.ctx(ctx), &pb.HGetAllRequest{Key: key})
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte, len(resp.Fields))
	for _, fv := range resp.Fields {
		result[fv.Field] = fv.Value
	}
	return result, nil
}

// HDel removes fields from a hash. Returns the number of fields removed.
func (c *Client) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	resp, err := c.rpc.HDel(c.ctx(ctx), &pb.HDelRequest{Key: key, Fields: fields})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// --- sets ---

// SAdd adds members to a set. Returns the number of new members added.
func (c *Client) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	resp, err := c.rpc.SAdd(c.ctx(ctx), &pb.SAddRequest{Key: key, Members: members})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// SMembers returns all members of a set.
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	resp, err := c.rpc.SMembers(c.ctx(ctx), &pb.SMembersRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Keys, nil
}

// SCard returns the number of members in a set.
func (c *Client) SCard(ctx context.Context, key string) (int64, error) {
	resp, err := c.rpc.SCard(c.ctx(ctx), &pb.SCardRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// --- sorted sets ---

// ScoreMember is a member with its score for sorted set operations.
type ScoreMember struct {
	Member string
	Score  float64
}

// ZAdd adds members to a sorted set. Returns the number added.
func (c *Client) ZAdd(ctx context.Context, key string, members ...ScoreMember) (int64, error) {
	pbMembers := make([]*pb.ScoreMember, len(members))
	for i, m := range members {
		pbMembers[i] = &pb.ScoreMember{Score: m.Score, Member: m.Member}
	}
	resp, err := c.rpc.ZAdd(c.ctx(ctx), &pb.ZAddRequest{Key: key, Members: pbMembers})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// ZRange returns members in a sorted set within the given rank range.
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ScoreMember, error) {
	resp, err := c.rpc.ZRange(c.ctx(ctx), &pb.ZRangeRequest{
		Key:        key,
		Start:      start,
		Stop:       stop,
		WithScores: withScores,
	})
	if err != nil {
		return nil, err
	}
	result := make([]ScoreMember, len(resp.Members))
	for i, m := range resp.Members {
		result[i] = ScoreMember{Member: m.Member, Score: m.Score}
	}
	return result, nil
}

// --- vectors ---

// VSimResult holds a vector similarity search result.
type VSimResult struct {
	Element  string
	Distance float32
}

// VAddOption configures a VADD command.
type VAddOption func(*pb.VAddRequest)

// WithMetric sets the distance metric for the vector set.
func WithMetric(metric pb.VectorMetric) VAddOption {
	return func(r *pb.VAddRequest) {
		r.Metric = metric
	}
}

// WithConnectivity sets the HNSW M parameter.
func WithConnectivity(m uint32) VAddOption {
	return func(r *pb.VAddRequest) {
		r.Connectivity = &m
	}
}

// WithEfConstruction sets the HNSW ef_construction parameter.
func WithEfConstruction(ef uint32) VAddOption {
	return func(r *pb.VAddRequest) {
		r.EfConstruction = &ef
	}
}

// VAdd adds a vector to a vector set. The vector is passed as raw float32
// values — no string parsing overhead.
func (c *Client) VAdd(ctx context.Context, key, element string, vector []float32, opts ...VAddOption) (bool, error) {
	req := &pb.VAddRequest{
		Key:     key,
		Element: element,
		Vector:  vector,
	}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.rpc.VAdd(c.ctx(ctx), req)
	if err != nil {
		return false, err
	}
	return resp.Value, nil
}

// VSimOption configures a VSIM command.
type VSimOption func(*pb.VSimRequest)

// WithEfSearch sets the ef_search parameter for the query.
func WithEfSearch(ef uint32) VSimOption {
	return func(r *pb.VSimRequest) {
		r.EfSearch = &ef
	}
}

// VSim searches for the nearest neighbors to a query vector.
func (c *Client) VSim(ctx context.Context, key string, query []float32, count uint32, opts ...VSimOption) ([]VSimResult, error) {
	req := &pb.VSimRequest{
		Key:   key,
		Query: query,
		Count: count,
	}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.rpc.VSim(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	results := make([]VSimResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = VSimResult{Element: r.Element, Distance: r.Distance}
	}
	return results, nil
}

// --- server ---

// Ping sends a PING and returns the response.
func (c *Client) Ping(ctx context.Context) (string, error) {
	resp, err := c.rpc.Ping(c.ctx(ctx), &pb.PingRequest{})
	if err != nil {
		return "", err
	}
	return resp.Message, nil
}

// FlushDB removes all keys from the database.
func (c *Client) FlushDB(ctx context.Context) error {
	_, err := c.rpc.FlushDb(c.ctx(ctx), &pb.FlushDbRequest{})
	return err
}

// DBSize returns the total number of keys across all shards.
func (c *Client) DBSize(ctx context.Context) (int64, error) {
	resp, err := c.rpc.DbSize(c.ctx(ctx), &pb.DbSizeRequest{})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// Echo sends a message and returns it back.
func (c *Client) Echo(ctx context.Context, message string) (string, error) {
	resp, err := c.rpc.Echo(c.ctx(ctx), &pb.EchoRequest{Message: message})
	if err != nil {
		return "", err
	}
	return resp.Message, nil
}

// Decr decrements a key by 1 and returns the new value.
func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	resp, err := c.rpc.Decr(c.ctx(ctx), &pb.DecrRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// Unlink removes keys asynchronously (background deallocation). Returns the
// number of keys removed.
func (c *Client) Unlink(ctx context.Context, keys ...string) (int64, error) {
	resp, err := c.rpc.Unlink(c.ctx(ctx), &pb.UnlinkRequest{Keys: keys})
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}

// BgSave triggers a background snapshot.
func (c *Client) BgSave(ctx context.Context) (string, error) {
	resp, err := c.rpc.BgSave(c.ctx(ctx), &pb.BgSaveRequest{})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

// BgRewriteAof triggers a background AOF rewrite.
func (c *Client) BgRewriteAof(ctx context.Context) (string, error) {
	resp, err := c.rpc.BgRewriteAof(c.ctx(ctx), &pb.BgRewriteAofRequest{})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

// SlowLogEntry represents a single entry in the slow log.
type SlowLogEntry struct {
	ID            uint64
	TimestampUnix uint64
	DurationMicro uint64
	Command       string
}

// SlowLogGet returns slow log entries.
func (c *Client) SlowLogGet(ctx context.Context, count *uint32) ([]SlowLogEntry, error) {
	req := &pb.SlowLogGetRequest{}
	if count != nil {
		req.Count = count
	}
	resp, err := c.rpc.SlowLogGet(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	entries := make([]SlowLogEntry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = SlowLogEntry{
			ID:            e.Id,
			TimestampUnix: e.TimestampUnix,
			DurationMicro: e.DurationMicros,
			Command:       e.Command,
		}
	}
	return entries, nil
}

// SlowLogLen returns the number of slow log entries.
func (c *Client) SlowLogLen(ctx context.Context) (int64, error) {
	resp, err := c.rpc.SlowLogLen(c.ctx(ctx), &pb.SlowLogLenRequest{})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// SlowLogReset clears the slow log.
func (c *Client) SlowLogReset(ctx context.Context) error {
	_, err := c.rpc.SlowLogReset(c.ctx(ctx), &pb.SlowLogResetRequest{})
	return err
}

// Publish sends a message to a channel. Returns the number of subscribers
// that received the message.
func (c *Client) Publish(ctx context.Context, channel string, message []byte) (int64, error) {
	resp, err := c.rpc.Publish(c.ctx(ctx), &pb.PublishRequest{
		Channel: channel,
		Message: message,
	})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// SubscribeEvent represents a message received on a subscription.
type SubscribeEvent struct {
	Kind    string // "message" or "pmessage"
	Channel string
	Data    []byte
	Pattern string // only set for pmessage
}

// Subscribe opens a server-streaming subscription for the given channels
// and/or patterns. Returns a channel that yields events until the context
// is cancelled or the stream ends.
func (c *Client) Subscribe(ctx context.Context, channels []string, patterns []string) (<-chan SubscribeEvent, error) {
	stream, err := c.rpc.Subscribe(c.ctx(ctx), &pb.SubscribeRequest{
		Channels: channels,
		Patterns: patterns,
	})
	if err != nil {
		return nil, err
	}

	ch := make(chan SubscribeEvent, 64)
	go func() {
		defer close(ch)
		for {
			evt, err := stream.Recv()
			if err != nil {
				return
			}
			se := SubscribeEvent{
				Kind:    evt.Kind,
				Channel: evt.Channel,
				Data:    evt.Data,
			}
			if evt.Pattern != nil {
				se.Pattern = *evt.Pattern
			}
			select {
			case ch <- se:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// PubSubChannels returns active channel names, optionally filtered by pattern.
func (c *Client) PubSubChannels(ctx context.Context, pattern *string) ([]string, error) {
	req := &pb.PubSubChannelsRequest{}
	if pattern != nil {
		req.Pattern = pattern
	}
	resp, err := c.rpc.PubSubChannels(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	return resp.Keys, nil
}

// PubSubNumSub returns subscriber counts for the given channels.
func (c *Client) PubSubNumSub(ctx context.Context, channels ...string) (map[string]int64, error) {
	resp, err := c.rpc.PubSubNumSub(c.ctx(ctx), &pb.PubSubNumSubRequest{Channels: channels})
	if err != nil {
		return nil, err
	}
	result := make(map[string]int64, len(resp.Counts))
	for _, c := range resp.Counts {
		result[c.Channel] = c.Count
	}
	return result, nil
}
