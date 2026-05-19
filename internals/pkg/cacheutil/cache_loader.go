package cacheutil

import (
	"context"
	"database/sql"
	"time"

	"github.com/redis/go-redis/v9"
)

type CacheLoader[T any] struct {
	rds *redis.Client
	db  *sql.DB
}

func (l *CacheLoader[T]) EnsureCache(
	ctx context.Context,
	lockKey string,
	itemKey string,
	dbFetchFunc func() (T, error),
	redisSetFunc func(T) error,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)

	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)

	defer ticker.Stop()

	var acquired bool
	var err error
}
