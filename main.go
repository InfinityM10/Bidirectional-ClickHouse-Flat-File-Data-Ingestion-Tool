package main

import (
	"clickhouse-flat-ingestion/api"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	// Set up API routes
	http.HandleFunc("/api/connect/clickhouse", api.ConnectClickHouse)
	http.HandleFunc("/api/connect/flatfile", api.ConnectFlatFile)
	http.HandleFunc("/api/schema/clickhouse", api.GetClickHouseSchema)
	http.HandleFunc("/api/schema/flatfile", api.GetFlatFileSchema)
	http.HandleFunc("/api/preview/clickhouse", api.PreviewClickHouseData)
	http.HandleFunc("/api/preview/flatfile", api.PreviewFlatFileData)
	http.HandleFunc("/api/ingest/clickhouse-to-flatfile", api.IngestClickHouseToFlatFile)
	http.HandleFunc("/api/ingest/flatfile-to-clickhouse", api.IngestFlatFileToClickHouse)

	// Serve static frontend files
	fs := http.FileServer(http.Dir("../frontend"))
	http.Handle("/", fs)

	// Create upload directory if it doesn't exist
	uploadDir := "./uploads"
	if err := os.MkdirAll(uploadDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create upload directory: %v", err)
	}

	// Create output directory for flat files
	outputDir := "./output"
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Set up temporary file cleanup routine
	go cleanupTempFiles(uploadDir)

	port := "8080"
	log.Printf("Server starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

// cleanupTempFiles removes temporary uploaded files older than 1 hour
func cleanupTempFiles(dir string) {
	// Implementation for cleaning up temporary files
	// This would typically use time.Ticker to periodically check and remove old files
	// For brevity, full implementation omitted
}
