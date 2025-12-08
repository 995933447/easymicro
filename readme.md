# 🚀 EasyMicro — Go 简洁易用的微服务开发框架，work for let micro how easy

**EasyMicro** 是一个高性能、无过度封装、用少量极简的代码优雅实现齐全实用功能、低使用成本、高可扩展可维护的 Go 微服务框架，支持：

* gRPC 服务注册与调用
* 服务发现与负载均衡
* 事件驱动与异步消息处理
* 配置热加载
* 信号驱动的优雅停止
* 全链路追踪与日志拦截
* rpc熔断fallback
* 内置mongo orm, redis连接池, 基于nats实现事件订阅发布系统, 备灾主从自动选举等开箱即用的实用组件
* 代码自动生成
* 集成配置中心、健康检查、作业调度系统、分布式缓存系统、redis pool proxy等开箱即用的服务中间件

本框架旨在简化微服务开发，提供模块化、可插拔的基础设施。在大型团队中往往会根据项目需求开发符合项目特性的团队内部微服务框架,
而封装过于厚重的框架难以改动并不适合作为项目的初始框架进行迭代开发，本框架用极简少量的代码实现开箱即用的功能，且模块实现
独立解藕可轻易替换，代码简洁容易阅读，非常适合需要大量迭代修改框架需求的团队作为项目初始框架快速上手。

---

## ✨ 特性

* ⚡ gRPC 微服务快速启动和注册
* 🧩 配置热加载和集中管理
* 📨 事件驱动与消息队列（NATS）集成
* 🛠 支持 mongo（mgorm）、Redis 等常用后端服务
* 🌐 服务发现（内置 `discovery` 模块）
* 🛡 健康检查与优雅停止
* 🪝 链式拦截器支持：追踪、日志、熔断限流等

---

## 📦 安装

```bash
go get github.com/995933447/easymicro
```

---

## 🚀 快速上手
安装代码相关protoc-gen插件:

go install github.com/995933447/easymicro/protoc-gen/protoc-gen-easymicro-server

go install github.com/995933447/easymicro/protoc-gen/protoc-gen-easymicro-client

go install google.golang.org/protobuf/cmd/protoc-gen-go

go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

如果需要使用mgorm作为orm框架(推荐使用,很好用的mongo orm框架)，则安装：

go install github.com/995933447/mgorm/protoc-gen-mgorm

定义一个proto: 

```go
syntax = "proto3";

package user;
option go_package = "github.com/995933447/easymicro/example/user";
import "mgorm_ext.proto";
import "easymicro_ext.proto";

service User {
  rpc GetUserInfo(GetUserInfoReq) returns (GetUserInfoResp);
  rpc ProxyEcho(GetUserInfoReq) returns (GetUserInfoResp);
}

message GetUserInfoReq {
  uint64 id = 1;
}

message GetUserInfoResp {
  string name = 1;
}

message Echoer {
  option(ext.mgorm_opts) = {
    conn: "default"
    db: "echo_%d" // 分库
    tb: "echoer"
    uniq_index_keys: ["echoer_name"]
    cached: true
  };
  string echoer_name = 1;
}

```
编写shell: rpc_gen.sh：
```
if [ $# -eq 0 ]; then
  echo "not service input"
  exit 0
fi

mkdir $1

protoc --go_out=./$1\
  --go-grpc_out=./$1\
  --easymicro-client_out=./$1\
  --easymicro-server_out=./$1\
  --mgorm_out=./$1\
  --jsonschema_out=./$1\
  --go_opt=paths=source_relative\
  --go-grpc_opt=paths=source_relative\
  --easymicro-client_opt=paths=source_relative\
  --easymicro-server_opt=paths=source_relative\
  --mgorm_opt=paths=source_relative\
  --jsonschema_opt=paths=source_relative\
  --proto_path=./proto\
  --proto_path=../../proto\
  --proto_path=../../../mgorm/proto\
  $1.proto

# 以下内容生成protobuf message的jsonschema,不需要的话可以删除

shopt -s nullglob

# 匹配到的文件数组
files=($1/jsonschemaoutput/*/*.go)

# 如果没有文件，直接退出
if [ ${#files[@]} -eq 0 ]; then
  exit 0
fi

# 只有有文件才创建目录
mkdir -p "jsonschema/$1"

# 遍历文件
for f in "${files[@]}"; do
  echo "running $f"
  go run "$f" > "jsonschema/$1/$(basename "$f").json"

```
执行shell:
````
./rpc_gen.sh user
````

下面是一个典型的代码生成的微服务启动示例（`main.go`）：

```go
package main

import (
    "context"
    "log"
    "strings"

    "github.com/995933447/easymicro/example/user"
    "github.com/995933447/easymicro/example/userserver/boot"
    "github.com/995933447/easymicro/example/userserver/config"
    "github.com/995933447/easymicro/example/userserver/event"
    "github.com/995933447/easymicro/grpc/interceptor"
    ggrpc "google.golang.org/grpc"

    "github.com/995933447/discovery"
    "github.com/995933447/easymicro/grpc"
    "github.com/995933447/runtimeutil"
)

func main() {
    // 初始化节点和服务配置
    if err := boot.InitNode("user"); err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }

    if err := config.LoadConfig(); err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }

    boot.InitRouteredis() // 初始化 Redis

    if err := boot.InitElect(context.TODO()); err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }

    if err := boot.InitMgorm(); err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }

    // 注册事件监听器
    if err := event.RegisterEventListeners(); err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }

    // 在非生产环境注册 NATS RPC 路由
    if !config.GetServerConfig().IsProd() {
        if err := boot.RegisterNatsRPCRoutes(); err != nil {
            log.Fatal(runtimeutil.NewStackErr(err))
        }
    }

    // 准备 gRPC 服务发现
    if err := grpc.PrepareDiscoverGRPC(context.TODO(), user.EasymicroGRPCSchema, "default"); err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }

    boot.RegisterGRPCDialOpts() // gRPC 拨号选项

    // 信号处理
    signal, err := boot.InitSignal()
    if err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }

    stopCtx, stopCancel := context.WithCancel(context.Background())
    gracefulStopCtx, gracefulStopCancel := context.WithCancel(stopCtx)

    // 优雅停止回调
    _ = signal.AppendSignalCallbackByAlias(boot.SignalAliasStop, func() { gracefulStopCancel() })
    _ = signal.AppendSignalCallbackByAlias(boot.SignalAliasInterrupt, func() { stopCancel() })

    // 启动 gRPC 服务
    err = grpc.ServeGRPC(context.TODO(), &grpc.ServeGRPCOptions{
        ServiceNames:    boot.ServiceNames,
        StopCtx:         stopCtx,
        GracefulStopCtx: gracefulStopCtx,
        OnRunServer: func(server *ggrpc.Server, node *discovery.Node) {
            signal.Start()
            log.Printf("up node %s:%d !\n", node.Host, node.Port)
            log.Printf(">>>>>>>>>>>>>>> run %s successful ! <<<<<<<<<<<<<<<", strings.Join(boot.ServiceNames, ", "))
        },
        RegisterServiceServersFunc: boot.RegisterServiceServers,
        EnabledHealth:              true,
        GRPCServerOpts: []ggrpc.ServerOption{
            ggrpc.ChainUnaryInterceptor(
                interceptor.TraceServeRPCUnaryInterceptor,
                interceptor.FastlogServeRPCUnaryInterceptor,
            ),
            ggrpc.ChainStreamInterceptor(
                interceptor.TraceServeRPCStreamInterceptor,
                interceptor.FastlogServeRPCStreamInterceptor,
            ),
        },
    })
    if err != nil {
        log.Fatal(runtimeutil.NewStackErr(err))
    }
}
```

---

## 🧩 核心模块

| 模块            | 功能                    |
| ------------- | --------------------- |
| `boot`        | 初始化节点、数据库、Redis、选举服务等 |
| `config`      | 加载和管理配置               |
| `event`       | 注册事件监听器               |
| `grpc`        | gRPC 服务启动、服务发现、拨号管理   |
| `interceptor` | 链式拦截器（日志、追踪等）         |
| `discovery`   | 服务发现与节点管理             |
| `runtimeutil` | 错误堆栈与工具函数             |

---

除了可以通过proto自定义代码生成规则外,还可以通过全局配置文件自定义全局代码生成规则,如果同时存在全局配置文件和proto定义,proto定义优先级将更高。

#### 全局文件需定义在执行protoc命令的工作目录的easymicro_loader中，如在/var/work/goproj中执行protoc，则全局文件请定义在/var/work/goproj/easymicro_loader/即目录中。
配置文件可以是json、toml等格式，需要以protogen作为文件名，如/var/work/goproj/easymicro_loader/protogen.toml或者D:\goproj\protogen.json等。

可配置项目结构定义：
```go
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
```
如定义一个json文件参考：
```shell
cat /etc/easymicro/protogen.json                             
{
    "disabled_relative_project_mode":true,
    "project_dir":"/var/work/easymicro/example",
    "enabled_health":true
}
```
## ⚡ 功能亮点

### 1️⃣ 信号驱动的热重载和优雅停止

* `SIGHUP` / `SIGUSR1`：触发配置热加载
* `SIGINT` / `SIGTERM`：触发优雅停止
* 支持多模块回调，灵活扩展

### 2️⃣ gRPC 服务注册与发现

* 自动注册服务到内置发现模块
* 支持多服务名和多实例
* 内置健康检查

### 3️⃣ 拦截器支持

* 链式 unary / stream 拦截器
* 内置追踪与快速日志
* 可自定义拦截器插件

---

## 📄 项目结构示例

```
userserver/
├── boot/  项目启动的初始化代码存放目录
│   ├── app.go 应用启动自定义逻辑
│   ├── elect.go 选举相关
│   ├── grpc.go grpc相关
│   ├── mgorm.go mgorm相关
│   ├── nats_rpc.go 集成nats rpc本地调试相关
│   ├── node.go 服务节点信息相关
│   ├── redis.go redis相关
│   ├── signal.go 信号处理器相关
│   └── svc_reg.go 注册服务相关
├── config/ 项目配置加载的代码存放目录
│   └── config.go 加载应用配置和常用后端服务
├── event/ 事件发布订阅等代码存放目录
│   └── event.go 消息订阅相关
├── handler/ rpc handler代码存放目录
│   ├── user_hdl.go 实现rpc的handler结构体定义
│   ├── user_hdl_user_info.go rpc方法具体实现逻辑
│   └── user_hdl_proxy_echo.go rpc方法具体实现逻辑
├── main.go 启动文件
```

---

## 💡 设计理念

> **EasyMicro = Modular + Observable + Configurable**
* 大道至简：没有厚重繁冗的封装，代码简单少量，所见即所得，高可扩展可维护
* 功能齐全：以最简单的代码实现微服务开发过程中最常用的功能组件，开箱即用
* 模块化：服务、配置、事件、拦截器、流量熔断fallback等独立模块，任意组合或移除
* 可观测：内置日志和追踪
* 可配置：支持动态配置热加载和优雅停机

---

## 📝 Best Practices

* 将每个服务的配置、事件和 gRPC 注册逻辑放到对应模块
* 使用信号机制触发配置更新，避免直接在业务逻辑中操作
* 非生产环境可集成 NATS RPC 模块进行本地调试(如把服务器流量调用到本地进行debug)
* 拦截器顺序影响日志和追踪效果，建议统一管理

---

## 📄 License

MIT License © 2025 [一只猪架构师bobby]
