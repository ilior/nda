package normalizer

import (
	"database/sql"
	"log"
	"nda/productsImporter/internal/entity"
)

type ProductUC struct {
	*sql.DB
}

func NewProductUC(db *sql.DB) *ProductUC {
	return &ProductUC{
		db,
	}
}

func (p *ProductUC) Find(externalID int, platformID int) (prod entity.Product, error error) {
	row := p.QueryRow("SELECT id, name, platform_id, brand_id, seller_id, external_id FROM products WHERE external_id = ? and platform_id = ?", externalID, platformID)

	if err := row.Scan(&prod.ID, &prod.Name, &prod.PlatformID, &prod.BrandID, &prod.SellerID, &prod.ExternalID); err != nil {
		if err == sql.ErrNoRows {
			log.Println("Creating Product")
			return entity.Product{
				ID:         0,
				Name:       "",
				PlatformID: platformID,
				BrandID:    0,
				SellerID:   0,
				ExternalID: externalID,
			}, nil
		}

		return prod, err
	}

	return prod, nil
}
