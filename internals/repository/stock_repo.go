package repository

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrStockNotFound        = errors.New("재고 키가 없음")
	ErrStockIsNotSufficient = errors.New("재고 부족")
	ErrCacheLoadTimeOut     = errors.New("캐시 로드 타임아웃 발생")
)

var (
	stockKeyPrefix     = "stock:"
	stockLockKeyPrefix = "lock:stock:"
	lockTTL            = 3 * time.Second
	retryInterval      = 50 * time.Millisecond
	maxRetries         = 5
)

type RedisStockRepo struct {
	db                  *sql.DB
	rdb                 *redis.Client
	decreaseStockScript *redis.Script
	lockReleaseScript   *redis.Script
}

func NewRedisStockRepo(
	db *sql.DB,
	rdb *redis.Client,
) *RedisStockRepo {
	// luaScript를 서버 시작 시에 한 번만 생성해서 사용하도록
	return &RedisStockRepo{
		db:  db,
		rdb: rdb,
		decreaseStockScript: redis.NewScript(`
			local current = tonumber(redis.call("GET", KEYS[1]))
			if current == nil then
				return -2
			end
			if current < tonumber(ARGV[1]) then
				return -1
			end
			redis.call("DECRBY", KEYS[1], ARGV[1])
			return current - tonumber(ARGV[1])
		`),
		lockReleaseScript: redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else 
			return 0 
		end
		`),
	}
}

func (r *RedisStockRepo) GetStock(ctx context.Context, productID int) (int64, error) {
	var stock int64
	err := r.db.QueryRowContext(ctx, "SELECT stock FROM products WHERE id = ?", productID).Scan(&stock)
	return stock, err
}

func (r *RedisStockRepo) DecreaseStock(ctx context.Context, productID int, quantity int) error {
	_, err := r.db.ExecContext(ctx, "UPDATE products SET stock = stock - ? WHERE id = ?", quantity, productID)
	return err
}

// ReleaseProductLock - 내가 잡은 락이 맞는지 확인 후 삭제
func (r *RedisStockRepo) ReleaseProductLock(ctx context.Context, key, value string) error {
	// 루아 스크립트를 활용해 원자적으로 처리 (GET, DEL)
	return r.lockReleaseScript.Run(ctx, r.rdb, []string{key}, value).Err()
}

func (r *RedisStockRepo) DecreaseStockWithLua(ctx context.Context, productID int, quantity int) (int64, error) {
	stockKey := r.getStockKey(productID)
	result, err := r.decreaseStockScript.Run(ctx, r.rdb, []string{stockKey}, quantity).Int64()

	if err != nil {
		return 0, err
	}

	switch result {
	case -2:
		result, err = r.loadStockToCache(ctx, productID)
	case -1:
		return 0, ErrStockIsNotSufficient
	default:
		return result, nil
	}

	if err != nil {
		return 0, err
	}

	return result, nil
}

func (r *RedisStockRepo) IncreaseStockAtomic(ctx context.Context, key string, quantity int) (int64, error) {
	return r.rdb.IncrBy(ctx, key, int64(quantity)).Result()
}

func (r *RedisStockRepo) loadStockToCache(ctx context.Context, productID int) (int64, error) {
	lockKey := r.getStockLockKey(productID)
	lockValue := uuid.New().String()

	// 분산 락 획득 시도
	acquired, err := r.rdb.SetNX(ctx, lockKey, lockValue, lockTTL).Result()

	if err != nil {
		return 0, err
	}

	// 락 습득 -> DB 조회 후 캐시에 올림
	if acquired {
		defer r.ReleaseProductLock(ctx, lockKey, lockValue)

		stock, err := r.GetStock(ctx, productID)
		if err != nil {
			return 0, err
		}

		stockKey := r.getStockKey(productID)
		if err := r.rdb.Set(ctx, stockKey, stock, lockTTL).Err(); err != nil {
			return 0, err
		}

		return stock, nil
	}

	// 락 획득 실패 시 대기 및 재시도
	return r.waitAndGetLock(ctx, productID)
}

func (r *RedisStockRepo) waitAndGetLock(ctx context.Context, productID int) (int64, error) {
	stockKey := r.getStockKey(productID)

	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(retryInterval):
		}

		stock, err := r.rdb.Get(ctx, stockKey).Int64()
		// 캐시가 생성 됐다면 반환
		if err == nil {
			return stock, nil
		}
		// 키 없음 오류가 아니면 에러 반환
		if !errors.Is(err, redis.Nil) {
			return 0, err
		}
	}

	return 0, ErrCacheLoadTimeOut
}

func (r *RedisStockRepo) getStockKey(productID int) string {
	return stockKeyPrefix + strconv.Itoa(productID)
}

func (r *RedisStockRepo) getStockLockKey(productID int) string {
	return stockLockKeyPrefix + strconv.Itoa(productID)
}
