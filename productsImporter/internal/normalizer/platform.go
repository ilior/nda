package normalizer

import (
	"database/sql"
	"log"
	"nda/productsImporter/internal/entity"
)

type PlatformNormalizer struct {
	*sql.DB
}

func NewPlatformNormalizer(db *sql.DB) *PlatformNormalizer {
	return &PlatformNormalizer{db}
}

func (pn *PlatformNormalizer) Get(name string) (platform entity.Platform, error error) {

	row := pn.QueryRow("SELECT id, name, active FROM platform WHERE name = ?", name)

	if err := row.Scan(&platform.ID, &platform.Name, &platform.Active); err != nil {

		if err == sql.ErrNoRows {

			log.Println("Creating Platform")
			return entity.Platform{
				ID:   0,
				Name: name,
			}, nil

		}

		return platform, err
	}

	return platform, nil

}
