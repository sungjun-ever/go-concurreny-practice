package service

import (
	"concurreny_test/internals/dto"
	"concurreny_test/internals/repository"
	"context"
	"errors"
)

type OrderService struct {
	StockRepo *repository.RedisStockRepo
}

func NewOrderHandler(stockRepo *repository.RedisStockRepo) *OrderService {
	return &OrderService{StockRepo: stockRepo}
}

func (s *OrderService) DecreaseStock(ctx context.Context, req dto.OrderRequest) (int64, error) {

	remaining, err := s.StockRepo.DecreaseStockWithLua(ctx, req.ProductID, req.Quantity)
	if err != nil {
		// error type마다 별도로 에러 핸들링
		switch {
		case errors.Is(err, repository.ErrStockNotFound):
			return 0, err // 404
		case errors.Is(err, repository.ErrStockIsNotSufficient):
			return 0, err // 409
		default:
			return 0, err
		}
	}

	// 주문서 관련 로작 후 실패 시
	// 재고 원상복구 로직 추가 가능

	return remaining, nil
}
