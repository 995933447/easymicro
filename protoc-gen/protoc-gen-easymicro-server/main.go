package main

import (
	_ "embed"
	"flag"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/995933447/easymicro/pb"
	easymicroprotogen "github.com/995933447/easymicro/protoc-gen"
	"github.com/995933447/runtimeutil"
	"github.com/995933447/stringhelper-go"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

//go:embed template/main.tmpl
var mainGoFileTemplate string

//go:embed template/svc_reg.tmpl
var svcRegGoFileTemplate string

//go:embed template/node.tmpl
var nodeGoFileTemplate string

//go:embed template/handler.tmpl
var serviceHandlerGoFileTemplate string

//go:embed template/handler_unary_method.tmpl
var serviceHandlerUnaryMethodGoFileTemplate string

//go:embed template/handler_srv_stream_method.tmpl
var serviceHandlerServerStreamMethodGoFileTemplate string

//go:embed template/handler_cli_stream_method.tmpl
var serviceHandlerClientStreamMethodGoFileTemplate string

//go:embed template/handler_both_stream_method.tmpl
var serviceHandlerBothStreamMethodGoFileTemplate string

//go:embed template/config.tmpl
var configGoFileTemplate string

//go:embed template/event.tmpl
var eventGoFileTemplate string

//go:embed template/init_elect.tmpl
var initElectGoFileTemplate string

//go:embed template/init_grpc.tmpl
var initGRPCGoFileTemplate string

//go:embed template/init_mongo.tmpl
var initMgormGoFileTemplate string

//go:embed template/init_nats_rpc.tmpl
var initNatsRPCGoFileTemplate string

//go:embed template/init_redis.tmpl
var initRedisGoFileTemplate string

//go:embed template/init_signal.tmpl
var initSignalGoFileTemplate string

//go:embed template/init_app.tmpl
var initAppGoFileTemplate string

//go:embed template/job_pub_type.tmpl
var jobPubTypeGoFileTmpl string

//go:embed template/job_pub_impl.tmpl
var jobPubImplGoFileTmpl string

func init() {
	// 自定义的 protoc 插件（例如 protoc-gen-xxx）必须通过 标准输入/输出 (stdin/stdout) 与 protoc 交互
	// 避免log 输出污染了 stdout，log 会把内容写到 stdout，而 protoc 会把 stdout 当成 CodeGeneratorResponse 解析。
	log.SetOutput(os.Stderr)
}

func main() {
	log.Println("======= Starting protoc-gen-grpc-server =========")

	if err := easymicroprotogen.LoadProtocGenConfig(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	debug := flag.Bool("d", false, "是否开启debug")
	inputFile := flag.String("i", "", "调试pb")
	flag.Parse() // 解析命令行参数

	var (
		input []byte
		err   error
	)
	if *debug {
		if *inputFile == "" {
			log.Fatal("input file is required")
		}

		input, err = os.ReadFile("req.pb")
		if err != nil {
			log.Fatal(err)
		}
	} else {
		input, err = io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}

		if easymicroprotogen.MustGetProtocGenConfig().Debug {
			log.Println("enable debug, store input to a file: req.pb")
			err = os.WriteFile("./req.pb", input, os.ModePerm)
			if err != nil {
				log.Fatal(runtimeutil.NewStackErr(err))
			}
			return
		}
	}

	var req pluginpb.CodeGeneratorRequest
	if err := proto.Unmarshal(input, &req); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	log.Println("Files to generate:", req.GetFileToGenerate())

	opts := protogen.Options{}
	plugin, err := opts.New(&req)
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	for _, f := range plugin.Files {
		if !f.Generate {
			log.Printf("easymicro gen-grpc-server, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		//  只生成有 Service 的 proto
		if len(f.Services) == 0 {
			log.Printf("easymicro gen-grpc-server, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		if err = genServerSkeleton(plugin, f); err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
			return
		}
	}

	resp := &pluginpb.CodeGeneratorResponse{}
	out, err := proto.Marshal(resp)
	if err != nil {
		panic(err)
	}
	// 必须写到 stdout
	os.Stdout.Write(out)

	log.Printf("server generated successfully!\n")
}

func genServerSkeleton(plugin *protogen.Plugin, f *protogen.File) error {
	projDir := easymicroprotogen.MustGetProtocGenConfig().ProjectDir
	if projDir == "" {
		var err error
		projDir, err = os.Getwd()
		if err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
		projDir = strings.TrimSuffix(projDir, "/")
	}

	log.Printf("Project dir: %s\n", projDir)

	ext := &pb.ProtoGenOpts{}
	opts := f.Desc.Options().(*descriptorpb.FileOptions)
	if proto.HasExtension(opts, pb.E_ProtoGenOpts) {
		ext = proto.GetExtension(opts, pb.E_ProtoGenOpts).(*pb.ProtoGenOpts)
	}

	goImportPath := string(f.GoImportPath) + normalizeDirName("Server")
	if ext.ServerPackage != "" {
		goImportPath = ext.ServerPackage
	}

	svcRootDirPath := strings.TrimSuffix(projDir, "/") + "/" + goImportPath
	if easymicroprotogen.MustGetProtocGenConfig().DisabledRelativeProjectMode {
		svcRootDirPath = strings.TrimSuffix(projDir, "/") + "/" + path.Base(goImportPath)
	}

	log.Printf("Service root dir path: %s\n", svcRootDirPath)

	if _, err := os.Stat(svcRootDirPath); err != nil {
		if !os.IsNotExist(err) {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
		if err = os.MkdirAll(svcRootDirPath, os.ModePerm); err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
	} else {
		log.Printf("Service root dir exists: %s\n", svcRootDirPath)
	}

	goModName := path.Dir(goImportPath)

	// ========= go.mod =========
	if easymicroprotogen.MustGetProtocGenConfig().AutoGoModServer || ext.AutoGoModServer {
		goModFilePath := svcRootDirPath + "/go.mod"
		if _, err := os.Stat(goModFilePath); err != nil {
			if !os.IsNotExist(err) {
				log.Println(runtimeutil.NewStackErr(err))
				return err
			}

			cmd := exec.Command("go", "mod", "init", goModName)
			cmd.Dir = svcRootDirPath
			output, err := cmd.CombinedOutput()
			if err != nil {
				log.Println(runtimeutil.NewStackErr(err))
				return err
			}

			log.Print(string(output))
		} else {
			log.Printf("go mod file exists, skipped gen\n")
		}
	}

	var serviceNames []string
	for _, service := range f.Services {
		serviceNames = append(serviceNames, service.GoName)
	}

	if err := genMainGoFile(f, svcRootDirPath, goImportPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	configDirPath := svcRootDirPath + "/config"
	if _, err := os.Stat(configDirPath); err != nil {
		if !os.IsNotExist(err) {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
		if err = os.MkdirAll(configDirPath, os.ModePerm); err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
	} else {
		log.Printf("config dir exists: %s\n", configDirPath)
	}

	if err := genConfigGoFile(configDirPath, goImportPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genServerConfigFile(goImportPath, svcRootDirPath); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	evtDirPath := svcRootDirPath + "/event"
	if _, err := os.Stat(evtDirPath); err != nil {
		if !os.IsNotExist(err) {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
		if err = os.MkdirAll(evtDirPath, os.ModePerm); err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
	} else {
		log.Printf("event dir exists: %s\n", evtDirPath)
	}

	if err := genEventGoFile(evtDirPath); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genJobPubGoFiles(f, svcRootDirPath, goImportPath); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	bootDirPath := svcRootDirPath + "/boot"
	if _, err := os.Stat(bootDirPath); err != nil {
		if !os.IsNotExist(err) {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
		if err = os.MkdirAll(bootDirPath, os.ModePerm); err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
	} else {
		log.Printf("boot dir exists: %s\n", bootDirPath)
	}

	if err := genSvcRegGoFile(f, bootDirPath, goImportPath, serviceNames); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genNodeGoFile(plugin, f, bootDirPath); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genInitElectGoFile(bootDirPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genInitGRPCGoFile(bootDirPath, goImportPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genInitMgormGoFile(bootDirPath, goImportPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genInitNatsRPCGoFile(f, bootDirPath, goImportPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genInitRedisGoFile(bootDirPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genInitSignalGoFile(bootDirPath, goImportPath, ext); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genInitAppGoFile(bootDirPath); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	if err := genHandlerGoFiles(f, svcRootDirPath); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	return nil
}

func genInitAppGoFile(bootDirPath string) error {
	filePath := bootDirPath + "/app.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("initAppFileTemplate").Funcs(funcMap).Parse(initAppGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, nil)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genInitSignalGoFile(bootDirPath, serviceServerImportPath string, ext *pb.ProtoGenOpts) error {
	filePath := bootDirPath + "/signal.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("initSignalFileTemplate").Funcs(funcMap).Parse(initSignalGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, &initSignalGoFileTemplateSlot{
		ServiceServerImportPath: serviceServerImportPath,
		DisabledFastlog:         ext.DisabledLoadFastlog || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadFastlog,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genInitRedisGoFile(bootDirPath string, ext *pb.ProtoGenOpts) error {
	filePath := bootDirPath + "/redis.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("initRedisFileTemplate").Funcs(funcMap).Parse(initRedisGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, &initRedisGoFileTemplateSlot{
		DisabledLoadFastlog: ext.DisabledLoadFastlog || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadFastlog,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genInitNatsRPCGoFile(f *protogen.File, bootDirPath, serviceServerImportPath string, ext *pb.ProtoGenOpts) error {
	filePath := bootDirPath + "/nats_rpc.go"

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	var (
		rpcs       []*NatsGRPCMethod
		imports    []string
		importSet  = make(map[string]struct{})
		enabledJob bool
	)
	for _, service := range f.Services {
		for _, method := range service.Methods {
			if proto.HasExtension(method.Desc.Options(), pb.E_JobOpts) {
				enabledJob = true
			}

			//  取 import & 类型名
			reqImport := string(method.Input.GoIdent.GoImportPath)
			reqPkg := path.Base(reqImport)
			if _, ok := importSet[reqImport]; !ok {
				imports = append(imports, reqImport)
				importSet[reqImport] = struct{}{}
			}
			reqName := reqPkg + "." + method.Input.GoIdent.GoName
			rpcs = append(rpcs, &NatsGRPCMethod{
				Service: service.GoName,
				Method:  method.GoName,
				Req:     reqName,
			})
		}
	}

	tmpl, err := template.New("initNastRPCFileTemplate").Funcs(funcMap).Parse(initNatsRPCGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, &initNatsRPCGoFileTemplateSlot{
		Imports:                 imports,
		ServiceServerImportPath: serviceServerImportPath,
		ServiceClientImportPath: string(f.GoImportPath),
		NatsGRPCs:               rpcs,
		EnabledHealth:           ext.EnabledHealth || easymicroprotogen.MustGetProtocGenConfig().EnabledHealth,
		EnabledJob:              enabledJob,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genInitMgormGoFile(bootDirPath, serviceServerImportPath string, ext *pb.ProtoGenOpts) error {
	filePath := bootDirPath + "/mgorm.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("initMgormFileTemplate").Funcs(funcMap).Parse(initMgormGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	redisCacheConn := ext.MgormRedisCacheConn
	if redisCacheConn == "" {
		redisCacheConn = easymicroprotogen.MustGetProtocGenConfig().MgormRedisCacheConn
	}

	err = tmpl.Execute(file, &initMgormGoFileTemplateSlot{
		MgormRedisCacheConn:         redisCacheConn,
		DisabledFastlogMgormQuery:   ext.DisabledFastlogMgormQuery || ext.DisabledLoadFastlog,
		DisabledMgormRedisCacheConn: ext.DisabledMgormRedisCacheConn || ext.DisabledLoadRedis,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genInitGRPCGoFile(bootDirPath, serviceServerImportPath string, ext *pb.ProtoGenOpts) error {
	filePath := bootDirPath + "/grpc.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("initGRPCFileTemplate").Funcs(funcMap).Parse(initGRPCGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, &initGRPCGoFileTemplateSlot{
		ServiceServerImportPath: serviceServerImportPath,
		DisabledLoadNats:        ext.DisabledLoadNats || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadNats,
		DisabledLoadFastlog:     ext.DisabledLoadFastlog || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadFastlog,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genInitElectGoFile(bootDirPath string, ext *pb.ProtoGenOpts) error {
	filePath := bootDirPath + "/elect.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("initElectFileTemplate").Funcs(funcMap).Parse(initElectGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	driverName := easymicroprotogen.MustGetProtocGenConfig().ElectDriverName
	if ext.ElectDriverName != "" {
		driverName = ext.ElectDriverName
	}

	driverConn := easymicroprotogen.MustGetProtocGenConfig().ElectDriverConn
	if ext.ElectDriverConn != "" {
		driverConn = ext.ElectDriverConn
	}

	err = tmpl.Execute(file, &initElectGoFileTemplateSlot{
		ElectDriverConn: driverConn,
		ElectDriverName: driverName,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genJobPubGoFiles(f *protogen.File, svcRootDirPath, serviceServerImportPath string) error {
	jobPubDirPath := svcRootDirPath + "/jobpub"
	jobPubImplDirPath := jobPubDirPath + "/impl"
	for _, service := range f.Services {
		for _, method := range service.Methods {
			if !proto.HasExtension(method.Desc.Options(), pb.E_JobOpts) {
				continue
			}

			reqImport := string(method.Input.GoIdent.GoImportPath)
			reqPkg := path.Base(reqImport)
			reqName := reqPkg + "." + method.Input.GoIdent.GoName

			err := func() error {
				if _, err := os.Stat(jobPubDirPath); err != nil {
					if !os.IsNotExist(err) {
						log.Println(runtimeutil.NewStackErr(err))
						return err
					}
					if err = os.MkdirAll(jobPubDirPath, os.ModePerm); err != nil {
						log.Println(runtimeutil.NewStackErr(err))
						return err
					}
				}

				typeFilePath := jobPubDirPath + "/" + stringhelper.Snake(service.GoName) + "_svc_" + stringhelper.Snake(method.GoName) + ".go"

				typeFile, err := os.OpenFile(typeFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}
				defer typeFile.Close()

				typeFileTmpl, err := template.New("jobPubTypeFileTemplate").Funcs(funcMap).Parse(jobPubTypeGoFileTmpl)
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}

				err = typeFileTmpl.Execute(typeFile, &jobPubGoFileTemplateSlot{
					ServiceName:             service.GoName,
					MethodName:              method.GoName,
					ServiceClientImportPath: string(f.GoImportPath),
					Req:                     reqName,
				})
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}

				cmd := exec.Command("go", "fmt", typeFilePath)
				_, _ = cmd.CombinedOutput()

				if _, err = os.Stat(jobPubImplDirPath); err != nil {
					if !os.IsNotExist(err) {
						log.Println(runtimeutil.NewStackErr(err))
						return err
					}
					if err = os.MkdirAll(jobPubImplDirPath, os.ModePerm); err != nil {
						log.Println(runtimeutil.NewStackErr(err))
						return err
					}
				}

				implFilePath := jobPubImplDirPath + "/" + stringhelper.Snake(service.GoName) + "_svc_" + stringhelper.Snake(method.GoName) + ".go"

				implFile, err := os.OpenFile(implFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}
				defer typeFile.Close()

				implFileTmpl, err := template.New("jobPubImplFileTemplate").Funcs(funcMap).Parse(jobPubImplGoFileTmpl)
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}

				err = implFileTmpl.Execute(implFile, &jobPubImplGoFileTemplateSlot{
					ServiceName:             service.GoName,
					MethodName:              method.GoName,
					ServiceClientImportPath: string(f.GoImportPath),
					ServiceServerImportPath: serviceServerImportPath,
					Req:                     reqName,
				})
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}

				cmd = exec.Command("go", "fmt", implFilePath)
				_, _ = cmd.CombinedOutput()

				return nil
			}()
			if err != nil {
				log.Println(runtimeutil.NewStackErr(err))
				return err
			}
		}
	}
	return nil
}

func genEventGoFile(evtDirPath string) error {
	filePath := evtDirPath + "/event.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("eventFileTemplate").Funcs(funcMap).Parse(eventGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, nil)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genServerConfigFile(goImportPath string, svcRootDirPath string) error {
	dir := svcRootDirPath + "/easymicro_loader"
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
	}

	filePath := dir + "/" + filepath.Base(goImportPath) + "." + easymicroprotogen.MustGetProtocGenConfig().ServerConfigFileFormat
	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	var content string
	switch easymicroprotogen.MustGetProtocGenConfig().ServerConfigFileFormat {
	case "json":
		content = `{
	"sample_pprof_time_long_sec": 20,
	"env": "dev"
}
`
	case "toml":
		content = `
sample_pprof_time_long_sec=20
env="dev"
`
	default:
	}

	_, err = file.WriteString(content)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	return nil
}

func genConfigGoFile(configDirPath, serviceServerImportPath string, ext *pb.ProtoGenOpts) error {
	filePath := configDirPath + "/config.go"

	_, err := os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("configFileTemplate").Funcs(funcMap).Parse(configGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, &configGoFileTemplateSlot{
		ServiceServerImportPath: serviceServerImportPath,
		DisabledLoadNats:        ext.DisabledLoadNats || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadNats,
		DisabledLoadRedis:       ext.DisabledLoadRedis || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadRedis,
		DisabledLoadMongo:       ext.DisabledLoadMongo || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadMongo,
		DisabledLoadFastlog:     ext.DisabledLoadFastlog || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadFastlog,
		DisabledLoadDiscovery:   ext.DisabledLoadDiscovery || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadDiscovery,
		DisabledLoadEtcd:        ext.DisabledLoadEtcd || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadEtcd,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genHandlerGoFiles(f *protogen.File, svcRootDirPath string) error {
	for _, service := range f.Services {
		svcHandlerDirPath := svcRootDirPath + "/handler"
		if _, err := os.Stat(svcHandlerDirPath); err != nil {
			if !os.IsNotExist(err) {
				log.Println(runtimeutil.NewStackErr(err))
				return err
			}
			if err = os.MkdirAll(svcHandlerDirPath, os.ModePerm); err != nil {
				log.Println(runtimeutil.NewStackErr(err))
				return err
			}
		}

		err := func() error {
			svcHandlerFilePath := svcHandlerDirPath + "/" + stringhelper.Snake(service.GoName) + "_hdl.go"
			if _, err := os.Stat(svcHandlerFilePath); os.IsNotExist(err) {
				file, err := os.OpenFile(svcHandlerFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}
				defer file.Close()

				tmpl, err := template.New("svcHandlerTmpl").Parse(serviceHandlerGoFileTemplate)
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}

				err = tmpl.Execute(file, &serviceHandlerGoFileTemplateSlot{
					ServiceName:             service.GoName,
					ServiceClientImportPath: string(f.GoImportPath),
					ServiceClientPackage:    string(f.GoPackageName),
				})
				if err != nil {
					log.Println(runtimeutil.NewStackErr(err))
					return err
				}
			} else {
				log.Printf("%s exists, skipped gen\n", svcHandlerFilePath)
			}

			// ========= 生成方法 =========
			for _, m := range service.Methods {
				svcHandlerMethodFilePath := svcHandlerDirPath + "/" +
					stringhelper.Snake(service.GoName) + "_hdl_" + stringhelper.Snake(m.GoName) + ".go"

				if _, err := os.Stat(svcHandlerMethodFilePath); err != nil {
					if !os.IsNotExist(err) {
						log.Println(runtimeutil.NewStackErr(err))
						return err
					}

					err = func() error {
						svcHandlerMethodFile, err := os.OpenFile(svcHandlerMethodFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
						if err != nil {
							log.Println(runtimeutil.NewStackErr(err))
							return err
						}
						defer svcHandlerMethodFile.Close()

						var (
							tmpl    *template.Template
							imports []string
						)
						if !m.Desc.IsStreamingServer() && !m.Desc.IsStreamingClient() {
							tmpl, err = template.New("serviceHandlerUnaryMethodFileTemplate").Parse(serviceHandlerUnaryMethodGoFileTemplate)
							if err != nil {
								log.Println(runtimeutil.NewStackErr(err))
								return err
							}
							imports = append(imports, "context")
						} else if m.Desc.IsStreamingServer() && !m.Desc.IsStreamingClient() {
							tmpl, err = template.New("serviceHandlerServerStreamMethodFileTemplate").Parse(serviceHandlerServerStreamMethodGoFileTemplate)
							if err != nil {
								log.Println(runtimeutil.NewStackErr(err))
								return err
							}
							imports = append(imports, "google.golang.org/grpc")
						} else if m.Desc.IsStreamingClient() && !m.Desc.IsStreamingServer() {
							tmpl, err = template.New("serviceHandlerClientStreamMethodFileTemplate").Parse(serviceHandlerClientStreamMethodGoFileTemplate)
							if err != nil {
								log.Println(runtimeutil.NewStackErr(err))
								return err
							}
							imports = append(imports, "google.golang.org/grpc")
						} else {
							tmpl, err = template.New("serviceHandlerBothStreamMethodFileTemplate").Parse(serviceHandlerBothStreamMethodGoFileTemplate)
							if err != nil {
								log.Println(runtimeutil.NewStackErr(err))
								return err
							}
							imports = append(imports, "google.golang.org/grpc")
						}

						//  取 import & 类型名
						reqImport := string(m.Input.GoIdent.GoImportPath)
						respImport := string(m.Output.GoIdent.GoImportPath)
						reqPkg := path.Base(reqImport)
						respPkg := path.Base(respImport)

						imports = append(imports, reqImport)
						if reqImport != respImport {
							imports = append(imports, respImport)
						}
						reqName := reqPkg + "." + m.Input.GoIdent.GoName
						respName := respPkg + "." + m.Output.GoIdent.GoName

						err = tmpl.Execute(svcHandlerMethodFile, &serviceHandlerMethodGoFileTemplateSlot{
							ServiceName: service.GoName,
							MethodName:  m.GoName,
							Req:         reqName,
							Resp:        respName,
							Imports:     imports,
						})
						if err != nil {
							log.Println(runtimeutil.NewStackErr(err))
							return err
						}
						return nil
					}()
					if err != nil {
						return err
					}
				} else {
					log.Printf("%s exists, skipped gen\n", svcHandlerMethodFilePath)
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func genNodeGoFile(plugin *protogen.Plugin, f *protogen.File, bootDirPath string) error {
	filePath := bootDirPath + "/node.go"

	// ========= 查找 Port =========
	var (
		portEnumImportPath string
		port               int32
		enumKey            = "RPCPort" + stringhelper.UpperFirstASCII(stringhelper.Camel(string(f.Desc.Package())))
	)
	for _, enum := range f.Enums {
		ext := proto.GetExtension(enum.Desc.Options(), pb.E_IsRpcPort)
		if isRPCPort, ok := ext.(bool); !ok || !isRPCPort {
			continue
		}
		portEnumImportPath = string(f.GoImportPath)
		for _, value := range enum.Values {
			if string(value.Desc.Name()) == enumKey {
				port = int32(value.Desc.Number())
				log.Println("find rpc port in top level enum")
				break
			}
		}
	}

	if port == 0 {
		// imports 的枚举
		imports := f.Desc.Imports()
		for i := 0; i < imports.Len(); i++ {
			imp := imports.Get(i)
			importedFile := imp.FileDescriptor

			enums := importedFile.Enums()
			for j := 0; j < enums.Len(); j++ {
				enum := enums.Get(j)
				ext := proto.GetExtension(enum.Options(), pb.E_IsRpcPort)
				if isRPCPort, ok := ext.(bool); !ok || !isRPCPort {
					continue
				}
				portEnumImportFile := plugin.FilesByPath[imp.Path()]
				portEnumImportPath = string(portEnumImportFile.GoImportPath)
				values := enum.Values()
				for k := 0; k < values.Len(); k++ {
					val := values.Get(k)
					if string(val.Name()) == enumKey {
						port = int32(val.Number())
						break
					}
				}
			}
		}
	}

	log.Println("portEnumImportPath", portEnumImportPath)
	if port > 0 {
		log.Printf("specify rpc port: %d\n", port)
	} else {
		log.Printf("specify no rpc port\n")
	}

	// ========= 生成 node.go =========
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("nodeFileTemplate").Funcs(funcMap).Parse(nodeGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	var portStr string
	if port != 0 {
		portStr = "int(" + path.Base(portEnumImportPath) + "." + "RPCPort_" + enumKey + ")"
	} else {
		portStr = "0"
	}
	err = tmpl.Execute(file, &nodeGoFileTemplateSlot{
		RPCPort:        portStr,
		EnumImportPath: portEnumImportPath,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

func genSvcRegGoFile(f *protogen.File, bootDirPath, serviceServerImportPath string, serviceNames []string) error {
	filePath := bootDirPath + "/svc_reg.go"

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}
	defer file.Close()

	tmpl, err := template.New("svcRegFileTemplate").Funcs(funcMap).Parse(svcRegGoFileTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	err = tmpl.Execute(file, &svcRegGoFileTemplateSlot{
		ServiceNames:            serviceNames,
		ServiceClientImportPath: string(f.GoImportPath),
		ServiceServerImportPath: serviceServerImportPath,
	})
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	cmd := exec.Command("go", "fmt", filePath)
	_, _ = cmd.CombinedOutput()

	return nil
}

// 生成 main.go
func genMainGoFile(f *protogen.File, svcRootDirPath, serviceServerImportPath string, ext *pb.ProtoGenOpts) error {
	filePath := svcRootDirPath + "/main.go"
	if _, err := os.Stat(filePath); err != nil {
		if !os.IsNotExist(err) {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}

		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		if err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}
		defer file.Close()

		tmpl, err := template.New("mainFileTemplate").Funcs(funcMap).Parse(mainGoFileTemplate)
		if err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}

		var enabledJobRPC bool
		for _, service := range f.Services {
			for _, method := range service.Methods {
				if proto.HasExtension(method.Desc.Options(), pb.E_JobOpts) {
					enabledJobRPC = true
					break
				}
			}
		}

		err = tmpl.Execute(file, &mainGoFileTemplateSlot{
			EnabledHealth:           ext.EnabledHealth || easymicroprotogen.MustGetProtocGenConfig().EnabledHealth,
			DisabledElect:           ext.DisabledElect || easymicroprotogen.MustGetProtocGenConfig().DisabledElect,
			DisabledLoadRedis:       ext.DisabledLoadRedis || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadRedis,
			DisabledLoadFastlog:     ext.DisabledLoadFastlog || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadFastlog,
			DisabledLoadMongo:       ext.DisabledLoadMongo || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadMongo,
			DisabledLoadNats:        ext.DisabledLoadNats || easymicroprotogen.MustGetProtocGenConfig().DisabledLoadNats,
			ServiceServerImportPath: serviceServerImportPath,
			ServiceClientImportPath: string(f.GoImportPath),
			EnabledJobRPC:           enabledJobRPC,
		})
		if err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}

		cmd := exec.Command("go", "fmt", filePath)
		_, _ = cmd.CombinedOutput()
	} else {
		log.Printf("%s exists. skipped gen\n", filePath)
	}

	return nil
}

func normalizeDirName(name string) string {
	switch easymicroprotogen.MustGetProtocGenConfig().DirNamingMethod {
	case "camel":
		return stringhelper.LowerFirstASCII(stringhelper.Camel(name))
	case "snake":
		return stringhelper.Snake(name)
	default:
		return strings.Replace(stringhelper.Snake(name), "_", "", -1)
	}
}
