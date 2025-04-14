package models

// Column represents a column in a table or file schema
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
