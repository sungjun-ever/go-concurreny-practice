package repository

import (
	"concurreny_test/internals/pkg/cacheutil"
	"concurreny_test/internals/pkg/rediskey"
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisProductRepo struct {
	RDB         *redis.Client
	CacheLoader *cacheutil.CacheLoader[int]
}

func NewRedisProductRepo(
	rdb *redis.Client,
) *RedisProductRepo {
	return &RedisProductRepo{RDB: rdb, CacheLoader: cacheutil.NewCacheLoader[int](rdb)}
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

func (r *RedisProductRepo) DecreaseStockAtomicWithLoad(ctx context.Context, productID int, quantity int, dbRepo *ProductRepo) (int64, error) {
	stockKey := rediskey.ProductStockKey(productID)
	lockKey := rediskey.LoadProductLockKey(productID)

	for {
		remaining, err := r.RDB.DecrBy(ctx, stockKey, int64(quantity)).Result()
		if err == nil {
			return remaining, nil
		}

		if errors.Is(err, redis.Nil) {
			err = r.CacheLoader.EnsureCache(ctx, lockKey, stockKey, 2*time.Second,
				func() (int, error) {
					return dbRepo.GetStock(productID)
				},
				func(dbStock int) error {
					return r.RDB.Set(ctx, stockKey, dbStock, 24*time.Hour).Err()
				},
			)
			if err != nil {
				return 0, err
			}
			// 캐시 생성 후 다시 감소 로직 실행
			continue
		}

		return 0, err
	}
}

func (r *RedisProductRepo) IncreaseStockAtomic(ctx context.Context, key string, quantity int) (int64, error) {
	return r.RDB.IncrBy(ctx, key, int64(quantity)).Result()
}
