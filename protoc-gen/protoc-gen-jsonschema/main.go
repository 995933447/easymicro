package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/995933447/easymicro/pb"
	easymicroprotogen "github.com/995933447/easymicro/protoc-gen"
	mgormpb "github.com/995933447/mgorm/pb"
	"github.com/995933447/runtimeutil"
	"github.com/995933447/stringhelper-go"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

type JsonSchemaOutputGoFilTemplateSlot struct {
	ServiceClientImportPath    string
	DoNotReference             bool
	AllowAdditionalProperties  bool
	RequiredFromJSONSchemaTags bool
	FieldNameTag               string
	MessageName                string
	IsORM                      bool
}

func PathBase(path string) string {
	return filepath.Base(path)
}

func init() {
	// 自定义的 protoc 插件（例如 protoc-gen-xxx）必须通过 标准输入/输出 (stdin/stdout) 与 protoc 交互
	// 避免log 输出污染了 stdout，log 会把内容写到 stdout，而 protoc 会把 stdout 当成 CodeGeneratorResponse 解析。
	log.SetOutput(os.Stderr)
}

func main() {
	log.Println("======= Starting protoc-gen-jsonschema =========")

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
			log.Printf("easymicro gen jsonschema output, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		//  只生成有 Service 的 proto
		if len(f.Messages) == 0 {
			log.Printf("easymicro gen jsonschema output, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		if err = genJsonShemaOutput(plugin, f); err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}

		log.Printf("easymicro gen-grpc-client, gen %s\n", f.Desc.Name())
	}

	stdout := plugin.Response()
	out, err := proto.Marshal(stdout)
	if err != nil {
		panic(err)
	}

	// 必须写到 stdout
	os.Stdout.Write(out)

	log.Printf("jsonschema output generated successfully!\n")
}

func genJsonShemaOutput(plugin *protogen.Plugin, f *protogen.File) error {
	tmpl, err := template.New("jsonSchemaOutputGoFilTemplate").Funcs(map[string]any{
		"PathBase": PathBase,
	}).Parse(jsonSchemaOutputGoFilTemplate)
	if err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	for _, message := range f.Messages {
		slot := JsonSchemaOutputGoFilTemplateSlot{
			ServiceClientImportPath:    string(f.GoImportPath),
			DoNotReference:             !easymicroprotogen.MustGetProtocGenConfig().JsonSchemaEnabledReference,
			AllowAdditionalProperties:  !easymicroprotogen.MustGetProtocGenConfig().JsonSchemaDisabledAdditionalProperties,
			RequiredFromJSONSchemaTags: !easymicroprotogen.MustGetProtocGenConfig().JsonSchemaDisabledRequireFromJsonSchemaTags,
			FieldNameTag:               easymicroprotogen.MustGetProtocGenConfig().JsonSchemaFieldNameTag,
			MessageName:                message.GoIdent.GoName,
		}

		if !proto.HasExtension(message.Desc.Options(), pb.E_JsonSchemaOutputOpts) && !easymicroprotogen.MustGetProtocGenConfig().EnabledGenJsonSchemaForAllMessages {
			continue
		} else {
			ext := proto.GetExtension(message.Desc.Options(), pb.E_JsonSchemaOutputOpts).(*pb.JsonSchemaOutputOpts)
			if ext.FieldNameTag != "" {
				slot.FieldNameTag = ext.FieldNameTag
			}
			if ext.DisabledAdditionalProperties {
				slot.AllowAdditionalProperties = false
			}
			if ext.DisabledRequireFromJsonSchemaTags {
				slot.RequiredFromJSONSchemaTags = false
			}
			if ext.EnabledReference {
				slot.DoNotReference = false
			}
		}

		if proto.HasExtension(message.Desc.Options(), mgormpb.E_MgormOpts) {
			slot.IsORM = true
		}

		var b bytes.Buffer
		err = tmpl.Execute(&b, slot)
		if err != nil {
			log.Println(runtimeutil.NewStackErr(err))
			return err
		}

		messageGoNameToSnake := stringhelper.Snake(message.GoIdent.GoName)

		g := plugin.NewGeneratedFile(
			"jsonschemaoutput/"+strings.ReplaceAll(messageGoNameToSnake, "_", "")+"/"+messageGoNameToSnake+"_output.go",
			f.GoImportPath+"/jsonschemaoutput",
		)

		g.P(b.String())
	}

	return nil
}
