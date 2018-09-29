package util

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"io/ioutil"
	"reflect"
)

type AggregatorConfig struct {
	Logger            zap.Config       `toml:"log"`
	BookKeeper        BookKeeperConfig `toml:"book_keeper"`
	General           GeneralConfig    `toml:"general"`
	EventSourceConfig EventSource      `toml:"event_source"`
	Storage           Storage          `toml:"storage"`
	loaded            bool
}

type GeneralConfig struct {
	Namespace      string `toml:"namespace"`
	EventBatchSize int    `toml:"event_batch_size"`
	WriteAsync     bool   `toml:"write_async"`
}

type BookKeeperConfig struct {
	Type string `toml:"type"`
}

type LoggerConfig struct {
	Level     string `toml:"level"`
	Format    string `toml:"format"`
	UseColors bool   `toml:"use_colors"`
}

type NatsConfig struct {
	URL       string `toml:"url"`
	ClientID  string `toml:"client_id"`
	ClusterID string `toml:"cluster_id"`
	GroupID   string `toml:"group_id"`
}

type Storage struct {
	Type  string       `toml:"type"`
	MySQL mysql.Config `toml:"mysql"`
}

var loadedConfig AggregatorConfig

func GetConfig() AggregatorConfig {
	if !loadedConfig.loaded {
		panic("config data not loaded")
	}
	return loadedConfig
}

func LoadConfig(tomlData string) {
	loadedConfig = AggregatorConfig{}
	_, err := toml.Decode(tomlData, &loadedConfig)
	if err != nil {
		panic(fmt.Errorf("error when parsing toml data: %v", err))
	}
	if reflect.DeepEqual(AggregatorConfig{}, loadedConfig) {
		panic("empty config data")
	} else {
		loadedConfig.loaded = true
	}
}

func LoadConfigFromFile(fileName string) {
	tomlData, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	LoadConfig(string(tomlData))
}
