package api

import (
	"clickhouse-flat-ingestion/models"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ConnectClickHouse handles connection requests to ClickHouse
func ConnectClickHouse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var config models.ClickHouseConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Try to establish connection to ClickHouse
	conn, err := connectToClickHouse(config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to ClickHouse: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Connection successful
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Successfully connected to ClickHouse",
	})
}

// ConnectFlatFile handles flat file upload/selection
func ConnectFlatFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form with 10MB max memory
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	// Get uploaded file
	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to get file from form", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Create a unique filename
	timestamp := time.Now().UnixNano()
	filename := fmt.Sprintf("%d_%s", timestamp, handler.Filename)
	filepath := filepath.Join("uploads", filename)

	// Create destination file
	dst, err := os.Create(filepath)
	if err != nil {
		http.Error(w, "Failed to create destination file", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// Copy uploaded file to destination
	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, "Failed to save uploaded file", http.StatusInternalServerError)
		return
	}

	// Return success with filepath
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "success",
		"message":  "File uploaded successfully",
		"filepath": filepath,
	})
}

// GetClickHouseSchema retrieves schema information from a ClickHouse table
func GetClickHouseSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Config    models.ClickHouseConfig `json:"config"`
		TableName string                 `json:"table_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Connect to ClickHouse
	conn, err := connectToClickHouse(request.Config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to ClickHouse: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Get schema from table
	columns, err := getClickHouseTableSchema(conn, request.TableName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get schema: %v", err), http.StatusInternalServerError)
		return
	}

	// Return columns
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(columns)
}

// GetFlatFileSchema analyzes and returns the schema of a flat file
func GetFlatFileSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		FilePath  string `json:"filepath"`
		Delimiter string `json:"delimiter"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Check if delimiter is provided, default to comma
	if request.Delimiter == "" {
		request.Delimiter = ","
	}

	// Get schema from file
	columns, err := getFlatFileSchema(request.FilePath, request.Delimiter)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get schema: %v", err), http.StatusInternalServerError)
		return
	}

	// Return columns
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(columns)
}

// PreviewClickHouseData returns preview data from a ClickHouse table
func PreviewClickHouseData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Config     models.ClickHouseConfig `json:"config"`
		TableName  string                 `json:"table_name"`
		Columns    []string               `json:"columns"`
		JoinConfig *models.JoinConfig     `json:"join_config,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Connect to ClickHouse
	conn, err := connectToClickHouse(request.Config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to ClickHouse: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Get preview data
	data, err := previewClickHouseData(conn, request.TableName, request.Columns, request.JoinConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get preview data: %v", err), http.StatusInternalServerError)
		return
	}

	// Return preview data
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// PreviewFlatFileData returns preview data from a flat file
func PreviewFlatFileData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		FilePath  string   `json:"filepath"`
		Delimiter string   `json:"delimiter"`
		Columns   []string `json:"columns"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Check if delimiter is provided, default to comma
	if request.Delimiter == "" {
		request.Delimiter = ","
	}

	// Get preview data
	data, err := previewFlatFileData(request.FilePath, request.Delimiter, request.Columns)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get preview data: %v", err), http.StatusInternalServerError)
		return
	}

	// Return preview data
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// IngestClickHouseToFlatFile handles data transfer from ClickHouse to a flat file
func IngestClickHouseToFlatFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Config     models.ClickHouseConfig `json:"config"`
		TableName  string                 `json:"table_name"`
		Columns    []string               `json:"columns"`
		OutputFile string                 `json:"output_file"`
		Delimiter  string                 `json:"delimiter"`
		JoinConfig *models.JoinConfig     `json:"join_config,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Check if delimiter is provided, default to comma
	if request.Delimiter == "" {
		request.Delimiter = ","
	}

	// Create output file path
	outputPath := filepath.Join("output", request.OutputFile)
	if !strings.HasSuffix(outputPath, ".csv") && !strings.HasSuffix(outputPath, ".tsv") && !strings.HasSuffix(outputPath, ".txt") {
		outputPath += ".csv"
	}

	// Connect to ClickHouse
	conn, err := connectToClickHouse(request.Config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to ClickHouse: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Transfer data
	count, err := transferClickHouseToFlatFile(conn, request.TableName, request.Columns, outputPath, request.Delimiter, request.JoinConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to transfer data: %v", err), http.StatusInternalServerError)
		return
	}

	// Return result
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"message":     "Data transferred successfully",
		"record_count": count,
		"output_file": outputPath,
	})
}

// IngestFlatFileToClickHouse handles data transfer from a flat file to ClickHouse
func IngestFlatFileToClickHouse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		FilePath   string                 `json:"filepath"`
		Delimiter  string                 `json:"delimiter"`
		Columns    []string               `json:"columns"`
		Config     models.ClickHouseConfig `json:"config"`
		TableName  string                 `json:"table_name"`
		CreateTable bool                  `json:"create_table"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Check if delimiter is provided, default to comma
	if request.Delimiter == "" {
		request.Delimiter = ","
	}

	// Connect to ClickHouse
	conn, err := connectToClickHouse(request.Config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to ClickHouse: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Transfer data
	count, err := transferFlatFileToClickHouse(request.FilePath, request.Delimiter, request.Columns, conn, request.TableName, request.CreateTable)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to transfer data: %v", err), http.StatusInternalServerError)
		return
	}

	// Return result
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"message":     "Data transferred successfully",
		"record_count": count,
		"table_name":  request.TableName,
	})
}
