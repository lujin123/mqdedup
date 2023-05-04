package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/lujin123/mqdedup"
	"github.com/pkg/errors"
)

type RedisPersist struct {
	client *redis.Client
	topic  string
}

func NewRedisPersist(client *redis.Client, topic string) *RedisPersist {
	return &RedisPersist{
		client: client,
		topic:  topic,
	}
}

func (s *RedisPersist) MarkConsuming(ctx context.Context, id string, seconds int) (bool, error) {
	key := s.key(id)
	ok, err := s.client.WithContext(ctx).SetNX(key, mqdedup.Consuming, time.Duration(seconds)*time.Second).Result()
	if err != nil {
		return false, errors.Wrapf(err, "[redis_persist] mark consuming failed, key: %s", key)
	}
	return ok, nil
}

func (s *RedisPersist) MarkConsumed(ctx context.Context, id string, seconds int) error {
	key := s.key(id)
	if _, err := s.client.WithContext(ctx).Set(key, mqdedup.Consumed, time.Duration(seconds)*time.Second).Result(); err != nil {
		return errors.Wrapf(err, "[redis_persist] mark consumed failed, key: %s", key)
	}
	return nil
}

func (s *RedisPersist) Get(ctx context.Context, id string) (mqdedup.ConsumeType, error) {
	key := s.key(id)
	v, err := s.client.WithContext(ctx).Get(key).Result()
	switch err {
	case nil:
		return mqdedup.NewConsumeTypeFromStr(v), nil
	case redis.Nil:
		return 0, errors.New(fmt.Sprintf("key of <%s> not exists", id))
	default:
		return 0, errors.Wrapf(err, "[redis_persist] get persist key failed, key: %s", key)
	}
}

func (s *RedisPersist) Delete(ctx context.Context, id string) error {
	key := s.key(id)
	if _, err := s.client.WithContext(ctx).Del(key).Result(); err != nil {
		return errors.Wrapf(err, "[redis_persist] delete failed, key: %s", key)
	}
	return nil
}

func (s *RedisPersist) key(id string) string {
	return fmt.Sprintf("cmq:%s:%s", s.topic, id)
}
