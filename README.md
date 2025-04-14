# Bidirectional ClickHouse & Flat File Data Ingestion Tool

A web application that allows bidirectional data transfer between ClickHouse databases and flat files with column selection capability and transfer reporting.

## Features

- Bidirectional data flow:
  - ClickHouse → Flat File
  - Flat File → ClickHouse
- Source/target configuration through UI
- JWT token-based authentication for ClickHouse
- Column selection for each transfer
- Record count reporting
- Error handling and user-friendly messages

## Setup Instructions

### Prerequisites

- Go 1.18+ installed
- Web browser with JavaScript enabled
- ClickHouse instance (local via Docker or cloud-based)

### Installation

1. Clone the repository:
   ```bash
    git clone https://github.com/InfinityM10/Bidirectional-ClickHouse-Flat-File-Data-Ingestion-Tool.git
   cd clickhouse-flat-ingestion
   ```

2. Install dependencies:
   ```bash
   cd backend
   go mod download
   ```

3. Start the server:
   ```bash
   go run main.go
   ```

4. Access the application:
   Open your browser and navigate to `http://localhost:8080`

## Usage Guide

### ClickHouse to Flat File

1. Select "ClickHouse" as the source
2. Enter ClickHouse connection details:
   - Host (e.g., localhost)
   - Port (e.g., 9440 for HTTPS, 8123 for HTTP)
   - Database name
   - Username
   - JWT Token
3. Click "Connect" to establish connection
4. Select the source table from the dropdown
5. Select columns you wish to transfer
6. Configure the target flat file name and delimiter
7. Click "Start Ingestion" to begin the transfer
8. View progress and final record count

### Flat File to ClickHouse

1. Select "Flat File" as the source
2. Upload or select a local flat file
3. Specify the delimiter used in the file
4. Click "Load Schema" to analyze the file structure
5. Select columns you wish to transfer
6. Enter ClickHouse connection details
7. Enter the target table name (new or existing)
8. Click "Start Ingestion" to begin the transfer
9. View progress and final record count

## Testing

Use the following test scenarios:

1. Transfer selected columns from ClickHouse example datasets to a flat file
2. Upload a CSV file and transfer selected columns to a new ClickHouse table
3. Test connection failures and observe error handling
4. Test with various data types to verify type conversion

## Technologies Used

- Backend: Go with the official ClickHouse Go client
- Frontend: HTML, CSS, JavaScript
- ClickHouse Client: github.com/ClickHouse/clickhouse-go/v2
