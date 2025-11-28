package main

type mainGoFileTemplateSlot struct {
	ServiceServerImportPath string
	ServiceClientImportPath string
	DisabledLoadRedis       bool
	DisabledElect           bool
	DisabledLoadMongo       bool
	DisabledLoadNats        bool
	DisabledLoadFastlog     bool
	EnabledHealth           bool
	EnabledJobRPC           bool
}

type svcRegGoFileTemplateSlot struct {
	ServiceClientImportPath string
	ServiceServerImportPath string
	ServiceNames            []string
}

type nodeGoFileTemplateSlot struct {
	RPCPort        string
	EnumImportPath string
}

type serviceHandlerGoFileTemplateSlot struct {
	ServiceName             string
	ServiceClientPackage    string
	ServiceClientImportPath string
}

type serviceHandlerMethodGoFileTemplateSlot struct {
	ServiceName string
	MethodName  string
	Imports     []string
	Req         string
	Resp        string
}

type configGoFileTemplateSlot struct {
	ServiceServerImportPath string
	DisabledLoadFastlog     bool
	DisabledLoadEtcd        bool
	DisabledLoadDiscovery   bool
	DisabledLoadNats        bool
	DisabledLoadRedis       bool
	DisabledLoadMongo       bool
}

type initElectGoFileTemplateSlot struct {
	ElectDriverName string
	ElectDriverConn string
}

type initGRPCGoFileTemplateSlot struct {
	ServiceServerImportPath string // 服务端 import 路径，用于引入 config
	DisabledLoadNats        bool   // 是否禁用 Nats 加载
	DisabledLoadFastlog     bool   // 是否禁用 Fastlog 拦截器
}

type initMgormGoFileTemplateSlot struct {
	MgormRedisCacheConn         string // mgorm redis 缓存 的Redis 连接名称
	DisabledMgormRedisCacheConn bool   // 是否禁用 mgorm Redis 缓存
	DisabledFastlogMgormQuery   bool   // 是否禁用 Fastlog Mgorm 查询日志
}

type NatsGRPCMethod struct {
	Service string // 服务名
	Method  string // 方法名
	Req     string // 请求类型名
}

type initNatsRPCGoFileTemplateSlot struct {
	Imports                 []string
	ServiceClientImportPath string            // gRPC 客户端路径
	ServiceServerImportPath string            // gRPC 服务端路径
	NatsGRPCs               []*NatsGRPCMethod // 需要注册到 NATS 的 gRPC 方法列表
	EnabledHealth           bool
	EnabledJob              bool
}

type initRedisGoFileTemplateSlot struct {
	DisabledLoadFastlog bool
}

type initSignalGoFileTemplateSlot struct {
	ServiceServerImportPath string // 服务 server import 路径
	DisabledFastlog         bool   // 是否禁用 fastlog
}

type jobPubGoFileTemplateSlot struct {
	ServiceClientImportPath string
	ServiceName             string
	MethodName              string
	Req                     string
}

type jobPubImplGoFileTemplateSlot struct {
	ServiceClientImportPath string
	ServiceServerImportPath string
	ServiceName             string
	MethodName              string
	Req                     string
}
