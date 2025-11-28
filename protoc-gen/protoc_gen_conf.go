package protocgen

import (
	"errors"
	"os"

	"github.com/995933447/easymicro/elect"
	"github.com/995933447/easymicro/loader"
	"github.com/995933447/natsevent"
	"github.com/995933447/routeredis"
	"github.com/spf13/viper"
)

type ProtocGenConfig struct {
	DisabledRelativeProjectMode                 bool   `mapstructure:"disabled_relative_project_mode" json:"disabled_relative_project_mode"`                                         // 服务端代码骨架输出到project_dir的目录下,而不采用project_dir+go import path方式输出代码位置
	ProjectDir                                  string `json:"project_dir" mapstructure:"project_dir"`                                                                               // 服务端代码输出目录路径
	DirNamingMethod                             string `json:"dir_naming_method" mapstructure:"dir_naming_method"`                                                                   // 目录命名风格，可取值：camel 小驼峰,snake 下划线,不设置则默认全小写无下划线
	GRPCResolveSchema                           string `json:"grpc_resolve_schema" mapstructure:"grpc_resolve_schema"`                                                               // 默认的grpc解析器名称
	DiscoveryName                               string `json:"discovery_name" mapstructure:"discovery_name"`                                                                         // 默认的服务注册前缀
	ElectDriverName                             string `json:"elect_driver_name" mapstructure:"elect_driver_name"`                                                                   // 默认的选举器驱动
	ElectDriverConn                             string `json:"elect_driver_conn" mapstructure:"elect_driver_conn"`                                                                   // 默认的选举器驱动选用的连接
	AutoGoModClient                             bool   `json:"auto_go_mod_client" mapstructure:"auto_go_mod_client"`                                                                 // 是否默认开启客户端包下生成go.mod
	AutoGoModServer                             bool   `json:"auto_go_mod_server" mapstructure:"auto_go_mod_server"`                                                                 // 是否默认开启服务端包下生成go.mod
	ServerConfigFileFormat                      string `json:"server_config_format" mapstructure:"server_config_format"`                                                             // 服务配置文件格式,json/toml 等等
	Debug                                       bool   `json:"debug" mapstructure:"debug"`                                                                                           // 用于debug调试protoc-gen插件
	MgormRedisCacheConn                         string `json:"mgorm_redis_cache_conn" mapstructure:"mgorm_redis_cache_conn"`                                                         // mgorm redis 缓存 的Redis 连接名称
	EnabledHealth                               bool   `json:"enabled_health" mapstructure:"enabled_health"`                                                                         // 是否启用easymicro内置的服务健康检查
	DisabledLoadFastlog                         bool   `json:"disabled_load_fastlog" mapstructure:"disabled_load_fastlog"`                                                           // 是否禁用生成自动从本地加载fastlog的代码
	DisabledLoadMongo                           bool   `json:"disabled_load_mongo" mapstructure:"disabled_load_mongo"`                                                               // 是否禁用生成自动从本地加载mongo的代码
	DisabledLoadRedis                           bool   `json:"disabled_load_redis" mapstructure:"disabled_load_redis"`                                                               // 是否禁用生成自动从本地加载redis的代码
	DisabledLoadEtcd                            bool   `json:"disabled_load_etcd" mapstructure:"disabled_load_etcd"`                                                                 // 是否禁用生成自动从本地加载etcd的代码
	DisabledLoadDiscovery                       bool   `json:"disabled_load_discovery" mapstructure:"disabled_load_discovery"`                                                       // 是否禁用自动从本地加载默认的服务发现的代码
	DisabledLoadNats                            bool   `json:"disabled_load_nats" mapstructure:"disabled_load_nats"`                                                                 // 是否禁用自动生成从本地加载nats的代码
	DisabledElect                               bool   `json:"disabled_elect" mapstructure:"disabled_elect"`                                                                         // 是否禁用生成选举器的代码
	GrpcSchema                                  string `json:"grpc_schema" mapstructure:"grpc_schema"`                                                                               // 指定grpc解析器协议, 默认是easymicro
	DisabledMgormRedisCacheConn                 bool   `json:"disabled_mgorm_redis_cache_conn" mapstructure:"disabled_mgorm_redis_cache_conn"`                                       // 是否禁用 mgorm Redis 缓存
	DisabledFastlogMgormQuery                   bool   `json:"disabled_fastlog_mgorm_query" mapstructure:"disabled_fastlog_mgorm_query"`                                             // 是否禁用 Fastlog Mgorm 查询日志
	JsonSchemaEnabledReference                  bool   `json:"json_schema_enabled_reference" mapstructure:"json_schema_enabled_reference"`                                           // 设置为true则github.com/invopop/jsonschema.DoNotReference为false,默认DoNotReference为true
	JsonSchemaDisabledAdditionalProperties      bool   `json:"json_schema_disabled_additional_properties" mapstructure:"json_schema_disabled_additional_properties"`                 // 设置为true则github.com/invopop/jsonschema.AllowAdditionalProperties为false,默认为true
	JsonSchemaDisabledRequireFromJsonSchemaTags bool   `json:"json_schema_disabled_require_from_json_schema_tags" mapstructure:"json_schema_disabled_require_from_json_schema_tags"` // 设置为true则github.com/invopop/jsonschema.RequiredFromJSONSchemaTags为false,默认为true
	JsonSchemaFieldNameTag                      string `json:"json_schema_field_name_tag" mapstructure:"json_schema_field_name_tag"`                                                 // 设置github.com/invopop/jsonschema.FieldNameTag
	EnabledGenJsonSchemaForAllMessages          bool   `json:"enabled_gen_json_schema_for_all_messages" mapstructure:"enabled_gen_json_schema_for_all_messages"`
}

var (
	protocGenConfig       ProtocGenConfig
	loadedProtocGenConfig bool
)

var DefaultProtocGenConfigFileName = "protogen"

const (
	DefaultDiscoveryName        = "default"
	DefaultGRPCResolveSchema    = "easymicro"
	EnvKeyProtocGenConfFilePath = "EASYMICRO_PROTOC_GEN_CONF_NAME"
)

func LoadProtocGenConfig() error {
	cfgName := os.Getenv(EnvKeyProtocGenConfFilePath)
	if cfgName == "" {
		cfgName = DefaultProtocGenConfigFileName
	}

	v, err := loader.LoadConfigToViper(cfgName)
	if err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			return err
		}

		loadedProtocGenConfig = true
		protocGenConfig.GRPCResolveSchema = DefaultGRPCResolveSchema
		protocGenConfig.DiscoveryName = DefaultDiscoveryName
		protocGenConfig.ElectDriverName = elect.DriverRedis
		protocGenConfig.ElectDriverConn = natsevent.ConnNameDefault
		protocGenConfig.MgormRedisCacheConn = routeredis.DefaultConnName
		protocGenConfig.JsonSchemaFieldNameTag = "json"
		protocGenConfig.ServerConfigFileFormat = "json"
		return nil
	}

	if err = v.Unmarshal(&protocGenConfig); err != nil {
		return err
	}

	if protocGenConfig.GRPCResolveSchema == "" {
		protocGenConfig.GRPCResolveSchema = DefaultGRPCResolveSchema
	}

	if protocGenConfig.DiscoveryName == "" {
		protocGenConfig.DiscoveryName = DefaultDiscoveryName
	}

	if protocGenConfig.ElectDriverName == "" {
		protocGenConfig.ElectDriverName = elect.DriverRedis
	}

	if protocGenConfig.ElectDriverConn == "" {
		protocGenConfig.ElectDriverConn = natsevent.ConnNameDefault
	}

	if protocGenConfig.MgormRedisCacheConn == "" {
		protocGenConfig.MgormRedisCacheConn = routeredis.DefaultConnName
	}

	if protocGenConfig.JsonSchemaFieldNameTag == "" {
		protocGenConfig.JsonSchemaFieldNameTag = "json"
	}

	if protocGenConfig.ServerConfigFileFormat == "" {
		protocGenConfig.ServerConfigFileFormat = "json"
	}

	loadedProtocGenConfig = true

	return nil
}

func MustGetProtocGenConfig() *ProtocGenConfig {
	if !loadedProtocGenConfig {
		panic("protoc-gen conf not loaded")
	}
	return &protocGenConfig
}
