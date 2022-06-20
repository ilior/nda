package normalizer

import (
	"database/sql"
	"log"
	"nda/productsImporter/internal/entity"
)

type SellerNormalizer struct {
	*sql.DB
}

func NewSellerNormalizer(db *sql.DB) *SellerNormalizer {
	return &SellerNormalizer{db}
}

func (sn *SellerNormalizer) Get(platform int, name string, externalID int, fallback int) (seller entity.Seller, error error) {

	if name == "" && externalID == 0 {
		return entity.Seller{ID: fallback}, nil
	}

	row := sn.QueryRow("SELECT id, name, external_id FROM sellers WHERE external_id = ? AND platform_id = ?", externalID, platform)

	if err := row.Scan(&seller.ID, &seller.Name, &seller.ExternalID); err != nil {

		if err == sql.ErrNoRows {
			row := sn.QueryRow("SELECT id, name, external_id FROM sellers WHERE name = ? AND platform_id = ? AND external_id is NULL", name, platform)

			if err := row.Scan(&seller.ID, &seller.Name, &seller.ExternalID); err != nil {
				if err == sql.ErrNoRows {
					log.Println("Creating Seller")
					return entity.Seller{
						ID:         0,
						Name:       name,
						ExternalID: externalID,
					}, nil
				}

				return seller, err
			}

			seller.ExternalID = externalID

			return seller, nil
		}

		return seller, err
	}

	seller.Name = name

	return seller, nil

}
