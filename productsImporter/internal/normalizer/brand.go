package normalizer

import (
	"database/sql"
	"log"
	"nda/productsImporter/internal/entity"
)

type BrandNormalizer struct {
	*sql.DB
}

func NewBrandNormalizer(db *sql.DB) *BrandNormalizer {
	return &BrandNormalizer{db}
}

func (bn *BrandNormalizer) Get(platform int, name string, externalID int, fallback int) (brand entity.Brand, error error) {

	if name == "" && externalID == 0 {
		return entity.Brand{ID: fallback}, nil
	}

	row := bn.QueryRow("SELECT id, name, external_id FROM brands WHERE external_id = ? AND platform_id = ?", externalID, platform)

	if err := row.Scan(&brand.ID, &brand.Name, &brand.ExternalID); err != nil {

		if err == sql.ErrNoRows {
			row := bn.QueryRow("SELECT id, name, external_id FROM brands WHERE name = ? AND platform_id = ? AND external_id is NULL", name, platform)

			if err := row.Scan(&brand.ID, &brand.Name, &brand.ExternalID); err != nil {
				if err == sql.ErrNoRows {
					log.Println("Creating Brand")
					return entity.Brand{
						ID:         0,
						Name:       name,
						ExternalID: externalID,
					}, nil
				}

				return brand, err
			}

			brand.ExternalID = externalID

			return brand, nil

		}

		return brand, err
	}

	brand.Name = name

	return brand, nil

}
