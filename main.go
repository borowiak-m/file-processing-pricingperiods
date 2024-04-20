package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	devConfig  = "config.development.json"
	prodConfig = "config.production.json"
)

type Config struct {
	Database struct {
		Server             string `json:"serverName"`
		Database           string `json:"databaseName"`
		IntegratedSecurity bool   `json:"integratedSecurity"`
		ApplicationIntent  string `json:"applicationIntent"`
	} `json:"database"`
}

type Period struct {
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

func main() {
	// add env flag
	config, err := readConfig(devConfig)
	if err != nil {
		log.Fatal("Config error: ", err)
	}

	fmt.Println(config)

	// db, err := connectDB(*config)
	// if err != nil {
	// 	log.Fatal("Database connection error: ", err)
	// } else {
	// 	fmt.Printf("Connected successfully to server %s, database name %s.", config.Database.Server, config.Database.Database)
	// }
}
