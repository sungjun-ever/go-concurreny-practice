package cacheutil

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type CacheLoader[T any] struct {
	RDB *redis.Client
}

func NewCacheLoader[T any](rdb *redis.Client) *CacheLoader[T] {
	return &CacheLoader[T]{RDB: rdb}
}

// EnsureCache - 캐시 존재 여부를 확인하고, 없으면 분산 락을 잡고 대기하며 캐시를 안전하게 로드함
func (l *CacheLoader[T]) EnsureCache(
	ctx context.Context,
	lockKey string,
	cacheKey string,
	lockTTL time.Duration,
	dbFetchFunc func() (T, error),
	redisSetFunc func(T) error,
) error {
	exists, err := l.RDB.Exists(ctx, cacheKey).Result()
	if err == nil && exists > 0 {
		return nil
	}

	lockValue := uuid.New().String()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			return errors.New("캐시 로딩 실패")

		case <-ticker.C:
			nowExists, _ := l.RDB.Exists(ctx, cacheKey).Result()
			// 이미 채워졌으므로 즉시 통과
			if nowExists > 0 {
				return nil
			}

			// 분산 락 획득 시도
			acquired, err := l.RDB.SetNX(ctx, lockKey, lockValue, lockTTL).Result()
			if err != nil {
				return errors.New("락 획득 실패")
			}

			if acquired {
				finalCheck, _ := l.RDB.Exists(ctx, cacheKey).Result()
				if finalCheck > 0 {
					l.releaseLock(ctx, lockKey, lockValue)
					return nil
				}

				dbData, err := dbFetchFunc()
				if err != nil {
					l.releaseLock(ctx, lockKey, lockValue)
					return errors.New("DB 데이터 가져오기 실패")
				}

				err = redisSetFunc(dbData)

				l.releaseLock(ctx, lockKey, lockValue)

				if err != nil {
					return errors.New("레디스 데이터 쓰기 실패")
				}
				return nil
			}
		}
	}
}

func (l *CacheLoader[T]) releaseLock(ctx context.Context, key string, value string) {
	var luaReleaseScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)
	_ = luaReleaseScript.Run(ctx, l.RDB, []string{key}, value).Err()
}
