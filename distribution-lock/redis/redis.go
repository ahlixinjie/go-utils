/*
简单的redis分布式锁，绝大部分情况可用，redis server有问题的情况下，可能会有短暂时间同时多个实例认为自己持有锁
*/

package redis

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ahlixinjie/go-utils/goroutine/wrapper"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Lua 脚本：续期
const refreshScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`

// Lua 脚本：释放
const releaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`

type Lock struct {
	client     *redis.Client
	key        string
	value      string
	ttl        time.Duration
	refreshGap time.Duration
	stopCh     chan struct{}
	mut        *sync.Mutex

	validTill time.Time
	isHeld    bool

	onLockLost func() //续期失败回调，主动unlock or context超时并不会调用
}

type redisLockOptions struct {
	OnLockLost func()
}
type Option func(*redisLockOptions)

func WithOnLockLost(onLockLostFunc func()) Option {
	return func(o *redisLockOptions) {
		o.OnLockLost = onLockLostFunc
	}
}

func NewRedisLock(client *redis.Client, key string, opts ...Option) *Lock {
	var options redisLockOptions
	for _, o := range opts {
		o(&options)
	}
	ttl := time.Second * 9
	return &Lock{
		client:     client,
		key:        key,
		value:      uuid.NewString(),
		ttl:        ttl,
		refreshGap: ttl / 3,
		stopCh:     make(chan struct{}),
		mut:        &sync.Mutex{},
		isHeld:     false,
		onLockLost: options.OnLockLost,
	}
}

func (l *Lock) IsHeld() bool {
	l.mut.Lock()
	defer l.mut.Unlock()
	return l.isHeld
}

// Lock 尝试加锁
func (l *Lock) Lock(ctx context.Context) (bool, error) {
	l.mut.Lock()
	defer l.mut.Unlock()

	if l.isHeld {
		return true, nil
	}

	newValidTill := time.Now().Add(l.ttl)
	ok, err := l.client.SetNX(ctx, l.key, l.value, l.ttl).Result()
	if err != nil {
		return false, err
	}
	if ok {
		l.validTill = newValidTill
		l.isHeld = true

		go wrapper.WithRecover(func() {
			defer func() {
				if r := recover(); r != nil {
					l.isHeld = false
					panic(r)
				}
			}()
			l.watchdog(ctx)
		})()
	}
	return ok, nil
}

// Unlock 主动释放
func (l *Lock) Unlock(ctx context.Context) error {
	l.mut.Lock()
	defer l.mut.Unlock()
	ctx = context.WithoutCancel(ctx)

	if !l.isHeld {
		return nil
	}
	select {
	case <-l.stopCh:
	default:
		close(l.stopCh)
	}
	//删除报err没关系，相当于自己主动释放锁不再处理了，等一段时间自然过期即可
	_, err := l.client.Eval(ctx, releaseScript, []string{l.key}, l.value).Result()
	l.isHeld = false
	return err
}

// watchdog 自动续期
func (l *Lock) watchdog(ctx context.Context) {
	ticker := time.NewTicker(l.refreshGap)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			newValidTill := time.Now().Add(l.ttl)

			// 续期
			res, err := l.client.Eval(ctx, refreshScript,
				[]string{l.key}, l.value, l.ttl.Milliseconds()).Result()

			if err != nil {
				slog.ErrorContext(ctx, "refresh error", slog.Any("err", err))
				// 网络错误，如果还在有效期内则继续 retry，否则则认为丢锁了
				if time.Now().After(l.validTill) {
					l.refreshFailHandler(ctx)
					return
				}

				continue
			}
			if res.(int64) == int64(0) {
				// 锁被别人拿走
				l.refreshFailHandler(ctx)
				return
			}
			l.validTill = newValidTill
			slog.DebugContext(ctx, "refresh success",
				slog.String("key", l.key),
				slog.String("value", l.value),
				slog.Int64("validTill", l.validTill.Unix()),
			)

		case <-l.stopCh:
			return
		case <-ctx.Done():
			_ = l.Unlock(ctx)
			return
		}
	}
}

func (l *Lock) refreshFailHandler(ctx context.Context) {
	l.mut.Lock()
	defer l.mut.Unlock()

	slog.WarnContext(ctx, "lost key", slog.String("key", l.key))
	if l.onLockLost != nil {
		l.onLockLost()
	}
	l.isHeld = false
}
