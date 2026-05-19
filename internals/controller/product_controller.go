package controller

import (
	"concurreny_test/internals/dto"
	"concurreny_test/internals/service"

	"github.com/gin-gonic/gin"
)

type ProductController struct {
	ps *service.ProductService
}

func NewProductController(ps *service.ProductService) *ProductController {
	return &ProductController{ps: ps}
}

func (pc *ProductController) BuyProduct(c *gin.Context) {
	ctx := c.Request.Context()
	var req dto.OrderRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "입력값 오류"})
	}

	err := pc.ps.NativeOrder(ctx, req)

	// 에러 처리는 생략
	if err != nil {
		c.JSON(500, gin.H{"error": "에러 발생"})
	}

	c.JSON(200, gin.H{"message": "성공"})
}
