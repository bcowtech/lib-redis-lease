package lease

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	redis "github.com/bcowtech/lib-redis-stream"
)

type Watcher struct {
	Group                string
	Name                 string
	RedisOption          *RedisOption
	MaxInFlight          int64
	MaxPollingTimeout    time.Duration
	AutoClaimMinIdleTime time.Duration
	IdlingTimeout        time.Duration
	ClaimSensitivity     int
	ClaimOccurrenceRate  int32
	EventHandler         EventHandleProc
	ErrorHandler         ErrorHandleProc

	consumer *redis.Consumer
}

func (w *Watcher) Subscribe(streams ...StreamOffset) error {
	{
		consumer := &redis.Consumer{
			Group:                   w.Group,
			Name:                    w.Name,
			RedisOption:             w.RedisOption,
			MaxInFlight:             w.MaxInFlight,
			MaxPollingTimeout:       w.MaxPollingTimeout,
			AutoClaimMinIdleTime:    w.AutoClaimMinIdleTime,
			IdlingTimeout:           w.IdlingTimeout,
			ClaimSensitivity:        w.ClaimSensitivity,
			ClaimOccurrenceRate:     w.ClaimOccurrenceRate,
			MessageHandler:          w.processMessage,
			UnhandledMessageHandler: nil,
			ErrorHandler:            w.ErrorHandler,
		}
		w.consumer = consumer
	}
	c := w.consumer

	w.configRedisConsumerGroup(streams...)

	return c.Subscribe(streams...)
}

func (w *Watcher) Close() {
	if w.consumer != nil {
		w.consumer.Close()
	}
}

func (w *Watcher) processMessage(ctx *redis.ConsumeContext, stream string, message *redis.XMessage) {
	ev := &Event{
		Sink: stream,
	}
	w.fillEventFromMessage(ev, message)

	err := w.EventHandler(ev)
	if err == nil {
		ctx.Ack(stream, message.ID)
		ctx.Del(stream, message.ID)
	}
}

func (w *Watcher) configRedisConsumerGroup(streams ...StreamOffset) error {
	var (
		group string = w.Group
	)

	admin, err := redis.NewAdminClient(w.RedisOption)
	if err != nil {
		return err
	}
	defer admin.Close()

	var handlers = []func(stream string) error{
		func(stream string) error {
			_, err := admin.CreateConsumerGroupAndStream(stream, group, redis.StartingStreamOffset)
			return err
		},
		func(stream string) error {
			_, err := admin.AlterConsumerGroupOffset(stream, group, redis.StartingStreamOffset)
			return err
		},
	}

	for _, offset := range streams {
		for _, handler := range handlers {
			err := handler(offset.Stream)
			if err != nil {
				switch {
				case strings.HasPrefix(err.Error(), "BUSYGROUP "):
					fmt.Printf("%% NOTICE %+v\n", err)
					break

				default:
					return err
				}
				continue
			}
			break
		}
	}
	return nil
}

func (w *Watcher) fillEventFromMessage(ev *Event, src *redis.XMessage) {
	var (
		action    string
		workspace string
		leaseID   string
		exipreAt  Timestamp
		timestamp Timestamp
	)

	// action
	if v, ok := src.Values["action"]; ok {
		if str, ok := v.(string); ok {
			action = str
		}
	}
	// workspace
	if v, ok := src.Values["workspace"]; ok {
		if str, ok := v.(string); ok {
			workspace = str
		}
	}
	// leaseID
	if v, ok := src.Values["lease"]; ok {
		if str, ok := v.(string); ok {
			leaseID = str
		}
	}
	// expireAt
	if v, ok := src.Values["expire_at"]; ok {
		if str, ok := v.(string); ok {
			t, err := strconv.ParseInt(str, 10, 64)
			if err == nil {
				exipreAt = Timestamp(t)
			}
		}
	}
	// timestamp
	{
		offset := strings.SplitN(src.ID, "-", 2)
		if len(offset) > 1 {
			t, err := strconv.ParseInt(offset[0], 10, 64)
			if err == nil {
				timestamp = Timestamp(t)
			}
		}
	}

	ev.Action = action
	ev.Workspace = workspace
	ev.LeaseID = leaseID
	ev.ExpireAt = exipreAt
	ev.Timestamp = timestamp
}
