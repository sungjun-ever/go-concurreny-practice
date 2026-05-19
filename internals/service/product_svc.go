package service

import (
	"concurreny_test/internals/dto"
	"concurreny_test/internals/pkg/rediskey"
	"concurreny_test/internals/repository"
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type ProductService struct {
	ProductRepo *repository.ProductRepo
	RedisRepo   *repository.RedisProductRepo
}

func NewOrderHandler(productRepo *repository.ProductRepo, redisRepo *repository.RedisProductRepo) *ProductService {
	return &ProductService{ProductRepo: productRepo, RedisRepo: redisRepo}
}

// NativeOrder - 동시성 제어가 없는 로직
// 동시에 요청이 들어오는 경우 재고가 정확히 관리되지 않음
func (h *ProductService) NativeOrder(c *gin.Context, req dto.OrderRequest) error {
	currentStock, err := h.ProductRepo.GetStock(req.ProductID)
	if err != nil {
		return errors.New("DB error")
	}

	if currentStock < req.Quantity {
		return errors.New("재고 소진")
	}

	err = h.ProductRepo.DecreaseStock(req.ProductID, req.Quantity)

	if err != nil {
		return errors.New("재고 감소 실패")
	}

	return nil
}

// PessimisticOrder - 정합성은 유지된다. 하지만 커밋되기전까지 다른 요청들은 락을 획득하기 위해 대기하거나, 획득에 실패하면 에러 발생
// 요청이 밀린다면 커넥션 풀이 고갈되고, 요청들이 쌓여 다른 디비 요청들도 연쇄 장애 발생할 수 있음
func (h *ProductService) PessimisticOrder(c *gin.Context, req dto.OrderRequest) error {
	tx, err := h.ProductRepo.DB.Begin()
	if err != nil {
		return errors.New("트랜잭션 사용 실패")
	}

	// 함수가 끝나기전 에러가 발생하면 롤백
	defer tx.Rollback()

	// 락 획득 재시도 로직
	// 최대 3초 대기
	ctx := c.Request.Context()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// 0.1초마다 신호
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var currentStock int

	for {
		select {
		case <-waitCtx.Done():
			// 3초안에 락을 획득하지 못한 경우
			return errors.New("DB lock acquisition timeout")
		case <-ticker.C:
			currentStock, err = h.ProductRepo.GetStockForUpdate(tx, req.ProductID)
			if err == nil {
				goto LockAcquired
			}
		}

	}

LockAcquired:
	if currentStock < req.Quantity {
		return errors.New("재고 부족")
	}

	err = h.ProductRepo.DecreaseStockWithTrx(tx, req.ProductID, req.Quantity)
	if err != nil {
		return errors.New("재고 감소 실패")
	}

	if err = tx.Commit(); err != nil {
		return errors.New("커밋 실패")
	}

	return nil
}

// RedisLockOrder - DB 트랜잭션 대기열은 없음, 동시에 여러 요청이 들어와도 1개만 락을 얻어 DB로 들어가고 나머지는 락 해제를 대기하며 기다림
// 재시도 로직(스핀 락)으로 인해 redis에 부하를 준다, 결국 락을 쥐고 있는 하나의 요청이 SELECT와 UPDATE 쿼리를 사용해야함
func (h *ProductService) RedisLockOrder(c *gin.Context, req dto.OrderRequest) error {
	ctx := c.Request.Context()
	lockKey := rediskey.LoadProductLockKey(req.ProductID)

	// 락 소유를 확인하기위한 값 생성
	lockValue := uuid.New().String()
	ttl := 3 * time.Second

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)

	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)

	defer ticker.Stop()

	var acquired bool
	var err error

	for {
		select {
		case <-waitCtx.Done():
			return errors.New("레디스 락 대기 실패")
		case <-ticker.C:
			acquired, err = h.RedisRepo.AcquireProductLock(ctx, lockKey, lockValue, ttl)

			if err == nil && acquired {
				goto LockAcquire
			}
			return errors.New("레디스 락 획득 오류")

		}
	}

LockAcquire:
	// 안전하게 락해제
	defer h.RedisRepo.ReleaseProductLock(ctx, lockKey, lockValue)

	currentStock, err := h.ProductRepo.GetStock(req.ProductID)
	if err != nil {
		return errors.New("DB 오류 발생")
	}

	if currentStock < req.Quantity {
		return errors.New("재고 부족")
	}

	err = h.ProductRepo.DecreaseStock(req.ProductID, req.Quantity)
	if err != nil {
		return errors.New("재고 감소 실패")
	}

	return nil
}

func (h *ProductService) RedisAtomicOrder(c *gin.Context, req dto.OrderRequest) error {
	ctx := c.Request.Context()
	remainingStock, err := h.RedisRepo.DecreaseStockAtomicWithLoad(ctx, req.ProductID, req.Quantity, h.ProductRepo)
	if err != nil {
		// 재고 소진(Business Error)이거나 레디스 장애(System Error) 처리
		return err
	}

	stockKey := rediskey.ProductStockKey(req.ProductID)

	if remainingStock < 0 {
		_, err = h.RedisRepo.IncreaseStockAtomic(ctx, stockKey, req.Quantity)
	}

	return nil
}
