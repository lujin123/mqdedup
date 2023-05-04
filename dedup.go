package mqdedup

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
)

type ConsumeType int

const (
	Consuming ConsumeType = 1
	Consumed  ConsumeType = 2
)

func NewConsumeTypeFromStr(s string) ConsumeType {
	v, _ := strconv.Atoi(s)
	return ConsumeType(v)
}

var (
	ErrMessageConsuming = errors.New("message consuming")
)

type (
	WorkerFunc[T any] func(context.Context, T) error
	IdFunc[T any]     func(T) (string, error)
)

type Persist interface {
	MarkConsuming(ctx context.Context, id string, seconds int) (bool, error)
	MarkConsumed(ctx context.Context, id string, seconds int) error
	Get(ctx context.Context, id string) (ConsumeType, error)
	Delete(ctx context.Context, id string) error
}

type Config[T any] struct {
	retentionSeconds int
	workerSeconds    int
	workerFunc       WorkerFunc[T]
	idFunc           IdFunc[T]
	persist          Persist
}

type Option[T any] func(c *Config[T])

func Invoke[T any](ctx context.Context, message T, opts ...Option[T]) error {
	var config Config[T]
	applyOptions(&config, opts...)

	if config.persist == nil {
		if err := config.workerFunc(ctx, message); err != nil {
			return errors.Wrapf(err, "[dedup] invoke worker consume failed, message: %+v", message)
		}
		return nil
	}

	id, err := config.idFunc(message)
	if err != nil {
		return err
	}

	ok, err := config.persist.MarkConsuming(ctx, id, config.workerSeconds)
	if err != nil {
		return err
	}

	if !ok {
		status, err := config.persist.Get(ctx, id)
		if err != nil {
			return errors.Wrapf(err, "[dedup] invoke persist query failed, id: %s", id)
		}
		switch status {
		case Consuming:
			// 消费中的状态，直接一个特定异常，等待重新投递
			return errors.Wrapf(ErrMessageConsuming, "[dedup] invoke message consuming, try again later, id: %s", id)
		case Consumed:
			// 已消费完的重复消息，直接返回即可
			return nil
		}
	}
	if err := config.workerFunc(ctx, message); err != nil {
		// 消费失败，删除标记
		if err = config.persist.Delete(ctx, id); err != nil {
			err = errors.Wrapf(err, "persist delete message failed, id: %s", id)
		}
		return errors.Wrapf(err, "[dedup] invoke worker consume failed, message: %+v", message)
	}

	// 消费成功，标记成功
	if err := config.persist.MarkConsumed(ctx, id, config.retentionSeconds); err != nil {
		return errors.Wrapf(err, "[dedup] invoke persist mark consumed failed, id: %s", id)
	}

	return nil
}
