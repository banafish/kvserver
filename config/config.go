package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Cfg struct {
	RaftCfg RaftCfg `yaml:"raft"`
	LogFile string  `yaml:"logFile"`
	Debug   bool    `yaml:"debug"`
}

type RaftCfg struct {
	PeerAddress []string `yaml:"peers,flow"`
	Me          string   `yaml:"me"`
}

func ReadCfg() Cfg {
	cfg := Cfg{}
	f, err := ioutil.ReadFile("conf.yaml")
	if err != nil {
		log.Fatal("读取配置文件出错", err)
	}

	if err := yaml.Unmarshal(f, &cfg); err != nil {
		log.Fatal("解析配置文件出错", err)
	}
	return cfg
}
