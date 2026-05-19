package service

import (
	"concurreny_test/internals/dto"
	"concurreny_test/internals/repository"
	"context"
	"errors"
	"strconv"
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

	lockKey := "lock:product:" + strconv.Itoa(req.ProductID)
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

	stockKey := "stock:product:" + strconv.Itoa(req.ProductID)

	// 레디스에 키가 존재하는지 확인
	exists, err := h.RedisRepo.StockExists(ctx, stockKey)

	if err != nil {
		return errors.New("레디스 오류")
	}

	// 캐시 미스가 발생한 경우, 캐시를 생성한다
	// 이 때, 동시에 여러 요청이 발샌하는 경우, 요청들마다 DB에 접근해서 레디스에 값을 넣기 때문에 락을 걸어준다
	if exists == 0 {
		lockKey := "lock:load:product:" + strconv.Itoa(req.ProductID)
		lockValue := uuid.New().String()
		ttl := 2 * time.Second

		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-waitCtx.Done():
				return errors.New("재고 set 락 획득 실패")
			case <-ticker.C:
				acquired, err := h.RedisRepo.AcquireProductLock(ctx, lockKey, lockValue, ttl)

				if err != nil {
					return errors.New("레디스 오류")
				}

				// 락을 습득했다면 db 조회 후 redis에 넣어줌
				if acquired {
					// 한 번 더 레디스에 값이 있는지 체크한다
					// 여러 요청이 동시에 발생했다면 어느 요청들은 이미 첫 부분을 통과해 값이 없다고 판단하고 내려온다.
					// 다른 요청에서 레디스에 값을 넣었는지 마지막으로 체크한다

					// 이미 값이 셋팅 되어있으면 넘어간다
					if nowExists, _ := h.RedisRepo.StockExists(ctx, stockKey); nowExists == 1 {
						h.RedisRepo.ReleaseProductLock(ctx, lockKey, lockValue)
						goto CacheLoaded
					}

					dbStock, err := h.ProductRepo.GetStock(req.ProductID)
					if err != nil {
						h.RedisRepo.ReleaseProductLock(ctx, lockKey, lockValue)
						return errors.New("DB 오류 발생")
					}

					err = h.RedisRepo.SetStock(ctx, stockKey, dbStock, time.Hour)

					// 데이터를 레디스에 저장했다면 오류 발생 여부와 상관없이 락을 해제
					h.RedisRepo.ReleaseProductLock(ctx, lockKey, lockValue)

					if err != nil {
						return errors.New("레디스 재고 생성 오류")
					}

					goto CacheLoaded
				}

			}

		}

	}
CacheLoaded:
	remainingStock, err := h.RedisRepo.DecreaseStockAtomic(ctx, stockKey, req.Quantity)

	if remainingStock < 0 {
		_, _ = h.RedisRepo.IncreaseStockAtomic(ctx, stockKey, req.Quantity)
		return errors.New("재고 소진")
	}

	return nil
}
