package repository

import "database/sql"

type ProductRepo struct {
	DB *sql.DB
}

func NewProductRepo(db *sql.DB) *ProductRepo {
	return &ProductRepo{DB: db}
}

func (s *ProductRepo) GetStock(productID int) (int, error) {
	var stock int
	err := s.DB.QueryRow("SELECT stock FROM products WHERE id = ?", productID).Scan(&stock)
	return stock, err
}

func (s *ProductRepo) DecreaseStock(productID int, quantity int) error {
	_, err := s.DB.Exec("UPDATE products SET stock = stock - ? WHERE id = ?", quantity, productID)
	return err
}

// GetStockForUpdate - 트랜잭션 사용
func (s *ProductRepo) GetStockForUpdate(tx *sql.Tx, productID int) (int, error) {
	var stock int
	err := tx.QueryRow("SELECT stock FROM products WHERE id = ? FOR UPDATE", productID).Scan(&stock)
	return stock, err
}

func (s *ProductRepo) DecreaseStockWithTrx(tx *sql.Tx, productID int, quantity int) error {
	_, err := tx.Exec("UPDATE products SET stock = stock - ? WHERE id = ?", quantity, productID)
	return err
}
