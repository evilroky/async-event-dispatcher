package config

import (
	"log"
	"sync"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env  string     `yaml:"env" env:"ENV" env-default:"local"`
	HTTP HTTPConfig `yaml:"http"`
	PG   PGConfig   `yaml:"postgres"`
}

type HTTPConfig struct {
	Port string `yaml:"port" env:"HTTP_PORT" env-default:"8080"`
}

type PGConfig struct {
	Host     string `yaml:"host" env:"POSTGRES_HOST" env-default:"localhost"`
	Port     string `yaml:"port" env:"POSTGRES_PORT" env-default:"5432"`
	User     string `yaml:"user" env:"POSTGRES_USER" env-default:"<YOUR-USERS>"`
	Password string `yaml:"password" env:"POSTGRES_PASSWORD" env-default:"<YOUR-PASSWORD>"`
	DBName   string `yaml:"dbname" env:"POSTGRES_DB" env-default:"event-dispatcher_db"`
}

var (
	instance *Config
	once     sync.Once
)

func GetConfig() *Config {
	once.Do(func() {
		instance = &Config{}

		_ = cleanenv.ReadConfig("config/config.yaml", instance)

		if err := cleanenv.ReadEnv(instance); err != nil {
			log.Fatalf("failed to read config: %v", err)
		}
	})
	return instance
}
