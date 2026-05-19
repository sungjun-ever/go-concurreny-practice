package service

import (
	"concurreny_test/internals/controller"
	"concurreny_test/internals/repository"
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"
)

type ProductService struct {
	ProductRepo *repository.ProductRepo
}

func NewOrderHandler(productRepo *repository.ProductRepo) *ProductService {
	return &ProductService{ProductRepo: productRepo}
}

// NativeOrder - 동시성 제어가 없는 로직
// 동시에 요청이 들어오는 경우 재고가 정확히 관리되지 않음
func (h *ProductService) NativeOrder(c *gin.Context, req controller.OrderRequest) error {
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
func (h *ProductService) PessimisticOrder(c *gin.Context, req controller.OrderRequest) error {
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
