package api

import (
	"clickhouse-flat-ingestion/models"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// connectToClickHouse establishes a connection to ClickHouse database
func connectToClickHouse(config models.ClickHouseConfig) (driver.Conn, error) {
	ctx := context.Background()
	
	// Determine if using secure connection
	var protocol string
	secure := false
	if config.Port == "8443" || config.Port == "9440" {
		protocol = "https"
		secure = true
	} else {
		protocol = "http"
	}

	// Create connection options
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
		},
		Protocol: protocol,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 10,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	}

	// Add JWT token if provided
	if config.JWTToken != "" {
		options.Auth.Token = config.JWTToken
	}

	// Set TLS options for secure connections
	if secure {
		options.TLS = &clickhouse.TLS{
			Secure: true,
			// Skip verification for simplicity in this demo
			// In production, you should properly verify certificates
			SkipVerify: true,
		}
	}

	// Create connection
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse connection: %w", err)
	}

	// Test connection
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, nil
}

// getClickHouseTableSchema retrieves column information for a table
func getClickHouseTableSchema(conn driver.Conn, tableName string) ([]models.Column, error) {
	ctx := context.Background()
	
	// Get column information from system.columns table
	query := fmt.Sprintf(`
		SELECT name, type, position
		FROM system.columns
		WHERE table = '%s'
		ORDER BY position
	`, tableName)
	
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema: %w", err)
	}
	defer rows.Close()

	var columns []models.Column
	for rows.Next() {
		var column models.Column
		var position uint32
		
		if err := rows.Scan(&column.Name, &column.Type, &position); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// If no columns found
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}

	return columns, nil
}

// getClickHouseTables returns a list of tables in the database
func getClickHouseTables(conn driver.Conn) ([]string, error) {
	ctx := context.Background()
	
	// Query system.tables to get table names
	query := `
		SELECT name
		FROM system.tables
		WHERE database = currentDatabase()
		ORDER BY name
	`
	
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return tables, nil
}

// previewClickHouseData returns a preview of data from a table
func previewClickHouseData(conn driver.Conn, tableName string, columns []string, joinConfig *models.JoinConfig) ([]map[string]interface{}, error) {
	ctx := context.Background()
	
	// Build query
	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT ")
	
	// Add columns
	if len(columns) > 0 {
		queryBuilder.WriteString(strings.Join(columns, ", "))
	} else {
		queryBuilder.WriteString("*")
	}
	
	queryBuilder.WriteString(" FROM ")
	queryBuilder.WriteString(tableName)
	
	// Add JOIN if specified
	if joinConfig != nil && joinConfig.JoinTable != "" {
		joinType := joinConfig.JoinType
		if joinType == "" {
			joinType = "INNER JOIN"
		}
		
		queryBuilder.WriteString(fmt.Sprintf(" %s %s ON ", joinType, joinConfig.JoinTable))
		queryBuilder.WriteString(joinConfig.JoinCondition)
	}
	
	// Limit results
	queryBuilder.WriteString(" LIMIT 100")
	
	// Execute query
	rows, err := conn.Query(ctx, queryBuilder.String())
	if err != nil {
		return nil, fmt.Errorf("failed to execute preview query: %w", err)
	}
	defer rows.Close()
	
	// Get column names
	columnNames := rows.ColumnNames()
	
	// Build result
	var result []map[string]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the row values
		rowValues := make([]interface{}, len(columnNames))
		rowValuesPtr := make([]interface{}, len(columnNames))
		
		// Create pointers to each interface{}
		for i := range rowValues {
			rowValuesPtr[i] = &rowValues[i]
		}
		
		// Scan the row into the slice of interface{}
		if err := rows.Scan(rowValuesPtr...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		// Create a map for this row
		rowMap := make(map[string]interface{})
		for i, columnName := range columnNames {
			rowMap[columnName] = rowValues[i]
		}
		
		result = append(result, rowMap)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	
	return result, nil
}

// transferClickHouseToFlatFile transfers data from ClickHouse to a flat file
func transferClickHouseToFlatFile(conn driver.Conn, tableName string, columns []string, outputFile string, delimiter string, joinConfig *models.JoinConfig) (int64, error) {
	ctx := context.Background()
	
	// Build query
	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT ")
	
	// Add columns
	if len(columns) > 0 {
		queryBuilder.WriteString(strings.Join(columns, ", "))
	} else {
		queryBuilder.WriteString("*")
	}
	
	queryBuilder.WriteString(" FROM ")
	queryBuilder.WriteString(tableName)
	
	// Add JOIN if specified
	if joinConfig != nil && joinConfig.JoinTable != "" {
		joinType := joinConfig.JoinType
		if joinType == "" {
			joinType = "INNER JOIN"
		}
		
		queryBuilder.WriteString(fmt.Sprintf(" %s %s ON ", joinType, joinConfig.JoinTable))
		queryBuilder.WriteString(joinConfig.JoinCondition)
	}
	
	// Execute query
	rows, err := conn.Query(ctx, queryBuilder.String())
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	
	// Get column names
	columnNames := rows.ColumnNames()
	
	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()
	
	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Set delimiter
	if delimiter == "\\t" || delimiter == "tab" {
		writer.Comma = '\t'
	} else {
		writer.Comma = rune(delimiter[0])
	}
	
	// Write header
	if err := writer.Write(columnNames); err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}
	
	// Write data
	var rowCount int64
	for rows.Next() {
		// Create a slice of interface{} to hold the row values
		rowValues := make([]interface{}, len(columnNames))
		rowValuesPtr := make([]interface{}, len(columnNames))
		
		// Create pointers to each interface{}
		for i := range rowValues {
			rowValuesPtr[i] = &rowValues[i]
		}
		
		// Scan the row into the slice of interface{}
		if err := rows.Scan(rowValuesPtr...); err != nil {
			return rowCount, fmt.Errorf("failed to scan row: %w", err)
		}
		
		// Convert values to strings
		rowStrings := make([]string, len(columnNames))
		for i, v := range rowValues {
			rowStrings[i] = fmt.Sprintf("%v", v)
		}
		
		// Write the row to the CSV file
		if err := writer.Write(rowStrings); err != nil {
			return rowCount, fmt.Errorf("failed to write row: %w", err)
		}
		
		rowCount++
		
		// Flush every 1000 rows to avoid using too much memory
		if rowCount%1000 == 0 {
			writer.Flush()
		}
	}
	
	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}
	
	return rowCount, nil
}

// createClickHouseTable creates a new table in ClickHouse based on inferred schema
func createClickHouseTable(conn driver.Conn, tableName string, columns []models.Column) error {
	ctx := context.Background()
	
	// Build CREATE TABLE query
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tableName))
	
	// Add columns
	for i, column := range columns {
		// Map inferred types to ClickHouse types
		clickhouseType := "String" // Default type
		
		// Try to infer better types - this is simplified for the demo
		// In a real implementation, you'd want more sophisticated type inference
		switch {
		case strings.Contains(strings.ToLower(column.Type), "int"):
			clickhouseType = "Int64"
		case strings.Contains(strings.ToLower(column.Type), "float") || strings.Contains(strings.ToLower(column.Type), "double"):
			clickhouseType = "Float64"
		case strings.Contains(strings.ToLower(column.Type), "date"):
			clickhouseType = "Date"
		case strings.Contains(strings.ToLower(column.Type), "datetime"):
			clickhouseType = "DateTime"
		}
		
		queryBuilder.WriteString(fmt.Sprintf("`%s` %s", column.Name, clickhouseType))
		
		// Add comma if not the last column
		if i < len(columns)-1 {
			queryBuilder.WriteString(", ")
		}
	}
	
	// Close query and add engine
	queryBuilder.WriteString(") ENGINE = MergeTree() ORDER BY tuple()")
	
	// Execute query
	if err := conn.Exec(ctx, queryBuilder.String()); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	
	return nil
}

// transferFlatFileToClickHouse transfers data from a flat file to ClickHouse
func transferFlatFileToClickHouse(filePath, delimiter string, selectedColumns []string, conn driver.Conn, tableName string, createTable bool) (int64, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	
	// Create CSV reader
	reader := csv.NewReader(file)
	
	// Set delimiter
	if delimiter == "\\t" || delimiter == "tab" {
		reader.Comma = '\t'
	} else if len(delimiter) > 0 {
		reader.Comma = rune(delimiter[0])
	}
	
	// Read header
	header, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("failed to read header: %w", err)
	}
	
	// Filter columns if specified
	columnIndexes := make([]int, 0)
	columnNames := make([]string, 0)
	
	if len(selectedColumns) > 0 {
		// Find indexes of selected columns
		for _, selectedColumn := range selectedColumns {
			for i, column := range header {
				if column == selectedColumn {
					columnIndexes = append(columnIndexes, i)
					columnNames = append(columnNames, column)
					break
				}
			}
		}
	} else {
		// Use all columns
		for i := range header {
			columnIndexes = append(columnIndexes, i)
		}
		columnNames = header
	}
	
	// Create table if needed
	if createTable {
		// Infer column types from the first few rows
		columns := make([]models.Column, len(columnNames))
		for i, name := range columnNames {
			columns[i] = models.Column{
				Name: name,
				Type: "String", // Default type
			}
		}
		
		// Try to improve type inference by sampling a few rows
		sampleSize := 10
		for i := 0; i < sampleSize; i++ {
			record, err := reader.Read()
			if err != nil {
				break // End of file or error
			}
			
			// Update column types based on values
			for j, columnIndex := range columnIndexes {
				if columnIndex < len(record) {
					inferredType := inferType(record[columnIndex])
					// Only upgrade the type if the current inferred type is more specific
					if isMoreSpecificType(inferredType, columns[j].Type) {
						columns[j].Type = inferredType
					}
				}
			}
		}
		
		// Reopen the file to reset the reader
		file.Close()
		file, err = os.Open(filePath)
		if err != nil {
			return 0, fmt.Errorf("failed to reopen file: %w", err)
		}
		defer file.Close()
		
		reader = csv.NewReader(file)
		if delimiter == "\\t" || delimiter == "tab" {
			reader.Comma = '\t'
		} else if len(delimiter) > 0 {
			reader.Comma = rune(delimiter[0])
		}
		
		// Skip header
		_, err = reader.Read()
		if err != nil {
			return 0, fmt.Errorf("failed to re-read header: %w", err)
		}
		
		// Create the table
		if err := createClickHouseTable(conn, tableName, columns); err != nil {
			return 0, err
		}
	}
	
	// Prepare INSERT query
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName))
	
	// Add column names
	for i, name := range columnNames {
		queryBuilder.WriteString(fmt.Sprintf("`%s`", name))
		if i < len(columnNames)-1 {
			queryBuilder.WriteString(", ")
		}
	}
	
	queryBuilder.WriteString(") VALUES ")
	
	// Batch insertion for better performance
	batchSize := 1000
	batch := make([][]string, 0, batchSize)
	var rowCount int64
	
	ctx := context.Background()
	
	// Process rows in batches
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break // End of file
			}
			return rowCount, fmt.Errorf("failed to read row: %w", err)
		}
		
		// Extract selected columns
		values := make([]string, len(columnIndexes))
		for i, columnIndex := range columnIndexes {
			if columnIndex < len(record) {
				values[i] = record[columnIndex]
			} else {
				values[i] = "" // Empty value for missing columns
			}
		}
		
		batch = append(batch, values)
		rowCount++
		
		// Execute batch insert when batch is full or at EOF
		if len(batch) >= batchSize {
			if err := insertBatch(ctx, conn, queryBuilder.String(), batch); err != nil {
				return rowCount, err
			}
			batch = make([][]string, 0, batchSize)
		}
	}
	
	// Insert any remaining rows
	if len(batch) > 0 {
		if err := insertBatch(ctx, conn, queryBuilder.String(), batch); err != nil {
			return rowCount, err
		}
	}
	
	return rowCount, nil
}

// insertBatch inserts a batch of rows into ClickHouse
func insertBatch(ctx context.Context, conn driver.Conn, queryPrefix string, batch [][]string) error {
	if len(batch) == 0 {
		return nil
	}
	
	// Build values part of the query
	var valuesBuilder strings.Builder
	for i, row := range batch {
		valuesBuilder.WriteString("(")
		
		for j, value := range row {
			// Escape single quotes in values
			escapedValue := strings.ReplaceAll(value, "'", "''")
			valuesBuilder.WriteString(fmt.Sprintf("'%s'", escapedValue))
			
			if j < len(row)-1 {
				valuesBuilder.WriteString(", ")
			}
		}
		
		valuesBuilder.WriteString(")")
		
		if i < len(batch)-1 {
			valuesBuilder.WriteString(", ")
		}
	}
	
	// Combine prefix and values
	query := queryPrefix + valuesBuilder.String()
	
	// Execute query
	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}
	
	return nil
}

// inferType tries to determine the data type from a string value
func inferType(value string) string {
	// Check if empty
	if value == "" {
		return "String"
	}
	
	// Try to parse as int
	if _, err := fmt.Sscanf(value, "%d", new(int)); err == nil {
		return "Int64"
	}
	
	// Try to parse as float
	if _, err := fmt.Sscanf(value, "%f", new(float64)); err == nil {
		return "Float64"
	}
	
	// Try to parse as date (simple check for this demo)
	if len(value) == 10 && strings.Count(value, "-") == 2 {
		if _, err := time.Parse("2006-01-02", value); err == nil {
			return "Date"
		}
	}
	
	// Try to parse as datetime (simple check)
	if strings.Count(value, "-") == 2 && strings.Count(value, ":") == 2 {
		if _, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
			return "DateTime"
		}
	}
	
	// Default to string
	return "String"
}

// isMoreSpecificType determines if newType is more specific than currentType
func isMoreSpecificType(newType, currentType string) bool {
	// Type specificity hierarchy (from most to least specific)
	typeHierarchy := map[string]int{
		"DateTime": 5,
		"Date":     4,
		"Float64":  3,
		"Int64":    2,
		"String":   1,
	}
	
	return typeHierarchy[newType] > typeHierarchy[currentType]
}
