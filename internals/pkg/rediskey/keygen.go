package rediskey

import "fmt"

func ProductStockKey(productID int) string {
	return fmt.Sprintf("stock:product:%d", productID)
}

func LoadProductLockKey(productID int) string {
	return fmt.Sprintf("lock:load:product:%d", productID)
}
