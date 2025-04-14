package models

// ClickHouseConfig represents the configuration for a ClickHouse connection
type ClickHouseConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	JWTToken string `json:"jwt_token"`
}

// JoinConfig represents the configuration for a JOIN operation
type JoinConfig struct {
	JoinTable     string `json:"join_table"`
	JoinType      string `json:"join_type"`
	JoinCondition string `json:"join_condition"`
}
