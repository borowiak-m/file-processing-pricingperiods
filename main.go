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
	Logging   struct {
		DebugMode bool
		LogToFile bool   `json:"logToFile"`
		FilePath  string `json:"filePath"`
	} `json:"logging"`
}

// object corresponding to a row of data returned from db
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
	// check if file was read correctly
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	var config Config
	// map json file fields to struct
	if err := json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	// return pointer to new config objects and no error
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
	// debug mode: log connection string
	if cfg.Logging.DebugMode {
		fmt.Printf("Connection string: %s\n", connStr)
	}
	// open connection
	db, err := sql.Open("mssql", connStr)
	// check for error
	if err != nil {
		return nil, fmt.Errorf("error connecting to the database: %w", err)
	}
	// return db object and no error
	return db, nil
}

func fetchPeriods(db *sql.DB, config *Config) ([]Period, error) {
	// read sql query from file
	query, err := os.ReadFile(config.QueryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read query from file: %w", err)
	}
	// debug mode: log query read from file
	if config.Logging.DebugMode {
		fmt.Println("Query: ", string(query))
	}

	// execute sql query
	rows, err := db.Query(string(query))
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close() // close rows after processing

	// results read from db will be stored in the slice of Period objects
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
			// if error return no results and an error
			return nil, fmt.Errorf("error scanning period: %w", err)
		}
		periods = append(periods, p)
	}
	// if error reading rows
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}
	// return slice of Period objects and no error
	return periods, nil
}

func logRecordset(periods []Period, config *Config) error {
	// open log file in append mode (or create it if does not exist)
	file, err := os.OpenFile(config.Logging.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("error opening log file: %v", err)
		return err
	}
	defer file.Close()
	var totalPeriodsLogged int
	// datetime format to be used for timestamps
	timestampFormat := "2006-01-02 15:04:05"
	// write all fetched periods to log file like there is no tomorrow
	for count, period := range periods {
		timestamp := time.Now().Format(timestampFormat)
		logEntry := fmt.Sprintf("%s - Period %v to %v, Prodnum: %d, Price %.2f, Priority %d\n",
			timestamp,
			period.PeriodStart.Format("2006-01-02"),
			period.PeriodEnd.Format("2006-01-02"),
			period.ProdNum, period.Price, period.PeriodPriority)
		_, err := file.WriteString(logEntry)
		if err != nil {
			fmt.Printf("error writing to file: %v\n", err)
			continue
		}
		totalPeriodsLogged = count
	}
	fmt.Printf("All periods logged ocrrectly.\nPeriods logged: %v", totalPeriodsLogged)
	return nil
}

func main() {
	// execution flag "-dev" for development environment variables
	devFlag := flag.Bool("dev", false, "Set to true to run in development mode.")
	// execution flag "-debug" for enhanced logging
	debugFlag := flag.Bool("debug", false, "Set true to run in debug mode.")
	flag.Parse()

	var envConfig string

	// add correct env flag
	if *devFlag {
		fmt.Println("Running in development mode")
		envConfig = DEV_CONFIG
	} else {
		fmt.Println("Running in production mode")
		envConfig = PROD_CONFIG
	}

	// load correct environment config variables
	config, err := readConfig(envConfig)
	if err != nil {
		log.Fatal("Config error: ", err)
	}

	// update dev flag to config object if set when executing
	config.Logging.DebugMode = *debugFlag
	// debug mode: log config object
	if config.Logging.DebugMode {
		fmt.Println(config)
	}

	// connect to db
	db, err := connectDB(*config)
	if err != nil {
		log.Fatal("Database connection error: ", err)
	} else if config.Logging.DebugMode {
		// debug mode: log successfull connections with params
		fmt.Printf("Connected successfully to server %s, database name %s.\n", config.Database.Server, config.Database.Database)
	}
	defer db.Close() // defer close connection to end of program

	// fetch data
	periods, err := fetchPeriods(db, config)
	if err != nil {
		log.Fatalf("Failed to fetch periods from the database: %v", err)
	}

	// log to file: log fetched data
	if config.Logging.LogToFile {
		logRecordset(periods, config)
	}

	// process data

	// output processed data
}
