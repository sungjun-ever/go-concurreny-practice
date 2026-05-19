package repository

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisProductRepo struct {
	RDB *redis.Client
}

func NewRedisProductRepo(rdb *redis.Client) *RedisProductRepo {
	return &RedisProductRepo{RDB: rdb}
}

// AcquireProductLock - SETNX를 이용해 분산 락 획득 시도
func (r *RedisProductRepo) AcquireProductLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return r.RDB.SetNX(ctx, key, value, ttl).Result()
}

// ReleaseProductLock - 내가 잡은 락이 맞는지 확인 후 삭제
func (r *RedisProductRepo) ReleaseProductLock(ctx context.Context, key, value string) error {
	// 루아 스크립트를 활용해 원자적으로 처리 (GET, DEL)
	var luaScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return  redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

	return luaScript.Run(ctx, r.RDB, []string{key}, value).Err()
}

func (r *RedisProductRepo) StockExists(ctx context.Context, key string) (int64, error) {
	return r.RDB.Exists(ctx, key).Result()
}

func (r *RedisProductRepo) SetStock(ctx context.Context, key string, quantity int, ttl time.Duration) error {
	return r.RDB.Set(ctx, key, quantity, ttl).Err()
}

// DecreaseStockAtomic - 원자적으로 데이터를 차감, GET->DECR 2개의 명령어가 아닌 단일 명령어로, 원자적 단위로 실행된다.
func (r *RedisProductRepo) DecreaseStockAtomic(ctx context.Context, key string, quantity int) (int64, error) {
	return r.RDB.DecrBy(ctx, key, int64(quantity)).Result()
}

func (r *RedisProductRepo) IncreaseStockAtomic(ctx context.Context, key string, quantity int) (int64, error) {
	return r.RDB.IncrBy(ctx, key, int64(quantity)).Result()
}
