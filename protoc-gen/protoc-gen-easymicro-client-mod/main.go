package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/995933447/easymicro/pb"
	easymicroprotogen "github.com/995933447/easymicro/protoc-gen"
	"github.com/995933447/runtimeutil"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func init() {
	// 自定义的 protoc 插件（例如 protoc-gen-xxx）必须通过 标准输入/输出 (stdin/stdout) 与 protoc 交互
	// 避免log 输出污染了 stdout，log 会把内容写到 stdout，而 protoc 会把 stdout 当成 CodeGeneratorResponse 解析。
	log.SetOutput(os.Stderr)
}

func main() {
	log.Println("======= Starting protoc-gen-grpc-client =========")

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
			log.Printf("easymicro gen-grpc-client, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		//  只生成有 Service 的 proto
		if len(f.Services) == 0 {
			log.Printf("easymicro gen-grpc-client, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		autoGenGoMod := easymicroprotogen.MustGetProtocGenConfig().AutoGoModClient
		fileOpts := f.Desc.Options().(*descriptorpb.FieldOptions)
		if !autoGenGoMod && proto.HasExtension(fileOpts, pb.E_ProtoGenOpts) {
			ext := proto.GetExtension(fileOpts, pb.E_ProtoGenOpts).(*pb.ProtoGenOpts)
			autoGenGoMod = ext.AutoGoModClient
		}

		var b bytes.Buffer
		b.WriteString("module " + string(f.GoImportPath) + "\ngo " + runtime.Version() + "\n")

		if _, err = plugin.NewGeneratedFile("go.mod", f.GoImportPath).Write(b.Bytes()); err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return
		}
	}

	stdout := plugin.Response()
	out, err := proto.Marshal(stdout)
	if err != nil {
		panic(err)
	}

	// 必须写到 stdout
	os.Stdout.Write(out)

	log.Printf("client generated successfully!\n")
}
