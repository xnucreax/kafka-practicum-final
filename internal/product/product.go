package product

import (
	"encoding/json"
	"fmt"
)

type Price struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type Stock struct {
	Available int `json:"available"`
	Reserved  int `json:"reserved"`
}

type Image struct {
	URL string `json:"url"`
	Alt string `json:"alt"`
}

type Specifications struct {
	Weight          string `json:"weight"`
	Dimensions      string `json:"dimensions"`
	BatteryLife     string `json:"battery_life"`
	WaterResistance string `json:"water_resistance"`
}

type Product struct {
	ProductID      string         `json:"product_id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Price          Price          `json:"price"`
	Category       string         `json:"category"`
	Brand          string         `json:"brand"`
	Stock          Stock          `json:"stock"`
	SKU            string         `json:"sku"`
	Tags           []string       `json:"tags"`
	Images         []Image        `json:"images"`
	Specifications Specifications `json:"specifications"`
	CreatedAt      string         `json:"created_at"`
	UpdatedAt      string         `json:"updated_at"`
	Index          string         `json:"index"`
	StoreID        string         `json:"store_id"`
}

type Codec struct{}

func (c *Codec) Encode(value any) ([]byte, error) {
	if _, ok := value.(*Product); !ok {
		return nil, fmt.Errorf("тип должен быть *Product, получен %T", value)
	}
	return json.Marshal(value)
}

func (c *Codec) Decode(data []byte) (any, error) {
	var p Product
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("ошибка десериализации: %v", err)
	}
	return &p, nil
}
