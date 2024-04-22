package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // SQL server driver
)

const (
	DEV_CONFIG  = "config.development.json"
	PROD_CONFIG = "config.production.json"
)

type Config struct {
	Database struct {
		Server             string `json:"serverName"`
		Database           string `json:"databaseName"`
		IntegratedSecurity bool   `json:"integratedSecurity"`
		ApplicationIntent  string `json:"applicationIntent"`
		ApplicationName    string `json:"applicationName"`
	} `json:"database"`
	QueryPath string `json:"queryPath"`
}

type Period struct {
	ID             int
	PeriodStart    time.Time
	PeriodEnd      time.Time
	Price          float64
	ProdNum        int
	PeriodPriority int
}

// Read config from a JSON file
func readConfig(path string) (*Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	var config Config
	if err := json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return &config, nil
}

// Connect to the dabatase
func connectDB(cfg Config) (*sql.DB, error) {
	connStr := fmt.Sprintf("server=%s;database=%s;integrated security=%t;application intent=%s; application name=%s",
		cfg.Database.Server,
		cfg.Database.Database,
		cfg.Database.IntegratedSecurity,
		cfg.Database.ApplicationIntent,
		cfg.Database.ApplicationName)
	fmt.Printf("Connection string: %s\n", connStr)
	db, err := sql.Open("mssql", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to the database: %w", err)
	}
	return db, nil
}

func fetchPeriods(db *sql.DB, queryPath string) ([]Period, error) {
	// read sql query from file
	query, err := os.ReadFile(queryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read query from file: %w", err)
	}

	// execute sql query
	rows, err := db.Query(string(query))
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close() // close rows after processing

	var periods []Period
	for rows.Next() {
		var p Period // scan each rows into Period struct
		// Scan field order must match sql query field order
		if err := rows.Scan(
			&p.ID,
			&p.PeriodStart,
			&p.PeriodEnd,
			&p.Price,
			&p.ProdNum,
			&p.PeriodPriority); err != nil {
			return nil, fmt.Errorf("error scanning period: %w", err)
		}
		periods = append(periods, p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}

	return periods, nil
}

func main() {
	devFlag := flag.Bool("dev", false, "Set to true to run in development mode")
	flag.Parse()
	var envConfig string

	if *devFlag {
		fmt.Println("Running in development mode")
		envConfig = DEV_CONFIG
	} else {
		fmt.Println("Running in production mode")
		envConfig = PROD_CONFIG
	}

	// add env flag
	config, err := readConfig(envConfig)
	if err != nil {
		log.Fatal("Config error: ", err)
	}

	fmt.Println(config)

	// connect to db
	db, err := connectDB(*config)
	if err != nil {
		log.Fatal("Database connection error: ", err)
	} else {
		fmt.Printf("Connected successfully to server %s, database name %s.\n", config.Database.Server, config.Database.Database)
	}
	defer db.Close()

	// fetch data
	periods, err := fetchPeriods(db, config.QueryPath)
	if err != nil {
		log.Fatalf("Failed to fetch periods from the database: %v", err)
	}

	// process data

	// output processed data
}
