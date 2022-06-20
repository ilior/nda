package normalizer

import (
	"database/sql"
	"log"
	"nda/productsImporter/internal/entity"
	"strconv"
	"strings"
)

func Normalize(db *sql.DB, datach <-chan entity.ParsedLine, out chan<- entity.ChLine, done <-chan struct{}) {
	//platform_id := getPlatform(data["platform"])
	for i := range datach {
		select {
		case out <- process(db, i):
		case <-done:
			return

		}
	}
}

func normalizeString(s string) string {
	s = strings.Trim(s, " ")

	if s == "-" {
		return ""
	}

	return s
}

func process(db *sql.DB, data entity.ParsedLine) entity.ChLine {
	pn := NewPlatformNormalizer(db)
	bn := NewBrandNormalizer(db)
	sn := NewSellerNormalizer(db)
	pr := NewProductUC(db)

	externalId, err := strconv.Atoi(data["brand_id"])

	if err != nil {
		externalId = 0
	}

	platform, err := pn.Get(normalizeString(data["platform"]))

	if err != nil {
		log.Fatal(err)
	}

	innerId, err := strconv.Atoi(data["inner_product_id"])

	if err != nil {
		innerId = 0
	}

	product, err := pr.Find(innerId, platform.ID)

	if err != nil {
		log.Fatal(err)
	}

	brand, err := bn.Get(platform.ID, normalizeString(data["brand"]), externalId, product.BrandID)

	if err != nil {
		log.Fatal(err)
	}

	externalId, err = strconv.Atoi(data["seller_id"])

	if err != nil {
		externalId = 0
	}

	seller, err := sn.Get(platform.ID, normalizeString(data["seller"]), externalId, product.SellerID)

	if err != nil {
		log.Fatal(err)
	}

	return entity.ChLine{Platform: platform, Product: product, Seller: seller, Brand: brand}
}
