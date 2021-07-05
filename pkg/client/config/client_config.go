package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

import (
	"github.com/imdario/mergo"
	"github.com/pkg/errors"
	"github.com/shima-park/agollo"
	"gopkg.in/yaml.v2"
)

import (
	"github.com/transaction-wg/seata-golang/pkg/base/common/constant"
	"github.com/transaction-wg/seata-golang/pkg/base/common/extension"
	baseConfig "github.com/transaction-wg/seata-golang/pkg/base/config"
	"github.com/transaction-wg/seata-golang/pkg/base/config_center"
	"github.com/transaction-wg/seata-golang/pkg/tc/config"
	"github.com/transaction-wg/seata-golang/pkg/util/log"
)

type ClientConfig struct {
	ApplicationID                string                        `yaml:"application_id" json:"application_id,omitempty"`
	TransactionServiceGroup      string                        `yaml:"transaction_service_group" json:"transaction_service_group,omitempty"`
	EnableClientBatchSendRequest bool                          `yaml:"enable-rpc_client-batch-send-request" json:"enable-rpc_client-batch-send-request,omitempty"`
	SeataVersion                 string                        `yaml:"seata_version" json:"seata_version,omitempty"`
	GettyConfig                  GettyConfig                   `yaml:"getty" json:"getty,omitempty"`
	TMConfig                     TMConfig                      `yaml:"tm" json:"tm,omitempty"`
	ATConfig                     ATConfig                      `yaml:"at" json:"at,omitempty"`
	RegistryConfig               config.RegistryConfig         `yaml:"registry_config" json:"registry_config,omitempty"` //注册中心配置信息
	ConfigCenterConfig           baseConfig.ConfigCenterConfig `yaml:"config_center" json:"config_center,omitempty"`     //配置中心配置信息
}

var clientConfig ClientConfig
var (
	confFile string
)

func init() {
	//fs := flag.NewFlagSet("config", flag.ExitOnError)
	flag.StringVar(&confFile, "conConf", os.Getenv(constant.CONF_CLIENT_FILE_PATH), "default client config path")
}
func GetRegistryConfig() config.RegistryConfig {
	return clientConfig.RegistryConfig
}
func GetClientConfig() ClientConfig {
	return clientConfig
}

func GetTMConfig() TMConfig {
	return clientConfig.TMConfig
}

func GetATConfig() ATConfig {
	return clientConfig.ATConfig
}

func GetDefaultClientConfig(applicationID string) ClientConfig {
	return ClientConfig{
		ApplicationID:           applicationID,
		SeataVersion:            "1.1.0",
		TransactionServiceGroup: "127.0.0.1:8091",
		GettyConfig:             GetDefaultGettyConfig(),
		TMConfig:                GetDefaultTmConfig(),
		RegistryConfig: config.RegistryConfig{Mode: "file"},
	}
}

func InitConf() error {
	var err error
	flag.Parse()

	if confFile == "" {
		return errors.New(fmt.Sprintf("application configure file name is nil"))
	}
	if path.Ext(confFile) != ".yml" {
		return errors.New(fmt.Sprintf("application configure file name{%v} suffix must be .yml", confFile))
	}

	clientConfig = ClientConfig{}
	confFileStream, err := ioutil.ReadFile(confFile)
	if err != nil {
		return errors.WithMessagef(err, fmt.Sprintf("ioutil.ReadFile(file:%s) = error:%s", confFile, err))
	}
	err = yaml.Unmarshal(confFileStream, &clientConfig)
	if err != nil {
		return errors.WithMessagef(err, fmt.Sprintf("yaml.Unmarshal() = error:%s", err))
	}
	//加载获取远程配置
	loadConfigCenterConfig(&clientConfig.ConfigCenterConfig)

	(&clientConfig).GettyConfig.CheckValidity()
	(&clientConfig).ATConfig.CheckValidity()

	return nil
}

func loadConfigCenterConfig(conf *baseConfig.ConfigCenterConfig) {
	if conf.Mode == "" {
		return
	}
	cc, err := extension.GetConfigCenter(conf.Mode, conf)
	if err != nil {
		log.Error("ConfigCenter can not connect success.Error message is %s", err.Error())
	}
	confStr := config_center.LoadConfigCenterConfig(cc, conf, &ClientConfigListener{})
	updateConf(&clientConfig, confStr)
}
func updateConf(config *ClientConfig, confStr string) {
	newConf := &ClientConfig{}
	confByte := []byte(confStr)
	yaml.Unmarshal(confByte, newConf)
	//合并配置中心的配置和本地文件的配置，形成新的配置
	if err := mergo.Merge(config, newConf, mergo.WithOverride); err != nil {
		log.Error("merge config fail %s ", err.Error())
	}
}

type ClientConfigListener struct {
}

func (ClientConfigListener) Process(event *config_center.ConfigChangeEvent) {
	//更新conf
	conf := GetClientConfig()
	updateConf(&conf, event.Value.(string))
}

func InitConfWithDefault(applicationID string) {
	clientConfig = GetDefaultClientConfig(applicationID)
	(&clientConfig).GettyConfig.CheckValidity()
}

func InitApolloConf(serverAddr string, appID string, nameSpace string) error {

	a, err := agollo.New(serverAddr, appID, agollo.AutoFetchOnCacheMiss())
	if err != nil {
		return errors.WithMessagef(err, fmt.Sprintf("get etcd error:%s", err))
	}

	var config = a.Get("content", agollo.WithNamespace(nameSpace))
	return initCommonConf([]byte(config))
}

func initCommonConf(confStream []byte) error {
	var err error
	err = yaml.Unmarshal(confStream, &clientConfig)
	fmt.Println("config", clientConfig)
	if err != nil {
		return errors.WithMessagef(err, fmt.Sprintf("yaml.Unmarshal() = error:%s", err))
	}

	(&clientConfig).GettyConfig.CheckValidity()
	(&clientConfig).ATConfig.CheckValidity()

	return nil
}
