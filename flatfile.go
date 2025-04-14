package api

import (
	"clickhouse-flat-ingestion/models"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// getFlatFileSchema analyzes a flat file to determine its schema
func getFlatFileSchema(filePath, delimiter string) ([]models.Column, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
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
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	
	// Create columns from header
	columns := make([]models.Column, len(header))
	for i, name := range header {
		columns[i] = models.Column{
			Name: name,
			Type: "String", // Default type
		}
	}
	
	// Sample a few rows to infer types
	sampleSize := 10
	for i := 0; i < sampleSize; i++ {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		
		// Update column types based on values
		for j, value := range record {
			if j < len(columns) {
				inferredType := inferType(value)
				// Only upgrade the type if the current inferred type is not more specific
				if isMoreSpecificType(inferredType, columns[j].Type) {
					columns[j].Type = inferredType
				}
			}
		}
	}
	
	return columns, nil
}

// previewFlatFileData returns a preview of data from a flat file
func previewFlatFileData(filePath, delimiter string, selectedColumns []string) ([]map[string]interface{}, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
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
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	
	// Determine column indexes
	columnIndexes := make([]int, 0)
	columnNames := make([]string, 0)
	
	if len(selectedColumns) > 0 {
		// Only include selected columns
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
		// Include all columns
		for i, column := range header {
			columnIndexes = append(columnIndexes, i)
			columnNames = append(columnNames, column)
		}
	}
	
	// Read up to 100 rows
	var result []map[string]interface{}
	for i := 0; i < 100; i++ {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		
		// Create map for this row
		rowMap := make(map[string]interface{})
		
		// Extract values for selected columns
		for j, columnIndex := range columnIndexes {
			if columnIndex < len(record) {
				// Convert value based on type
				value := record[columnIndex]
				columnName := columnNames[j]
				
				// Try to convert to appropriate type
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					rowMap[columnName] = intVal
				} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
					rowMap[columnName] = floatVal
				} else {
					rowMap[columnName] = value
				}
			}
		}
		
		result = append(result, rowMap)
	}
	
	return result, nil
}
