if [ $# -eq 0 ]; then
  echo "not service input"
  exit 0
fi

mkdir $1

protoc --go_out=./$1\
  --go-grpc_out=./$1\
  --easymicro-client_out=./$1\
  --mgorm_out=./$1\
  --jsonschema_out=./$1\
  --go_opt=paths=source_relative\
  --go-grpc_opt=paths=source_relative\
  --easymicro-client_opt=paths=source_relative\
  --mgorm_opt=paths=source_relative\
  --jsonschema_opt=paths=source_relative\
  --proto_path=./proto\
  --proto_path=../../proto\
  --proto_path=../../../mgorm/proto\
  $1.proto

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
done