# Void

Void 是一个数据处理和传输工具，支持多种输入源和输出目标，主要用于时序数据的收集、转换和存储。

~~(其主要目的在于实验时, 期望在 Grafana 实时监控数据指标, 并保存成 Parquet 便于后续分析)~~

[![Build](https://github.com/darkhighness/void/actions/workflows/build.yml/badge.svg)](https://github.com/darkhighness/void/actions/workflows/build.yml)

## 功能特点

- 多种输入源支持：命名管道、Unix 套接字
- 灵活的协议适配：CSV、Graphite
- 丰富的输出目标：标准输出、Parquet 文件、Prometheus
- 高效的数据管道处理：时序数据处理和注解
- 高性能设计：使用 Jemalloc 内存分配器和 Tokio 异步运行时

## 安装

### 预编译二进制文件

从 [最新发布](https://github.com/darkhighness/void/releases/latest) 下载适合您平台的预编译二进制文件。

### 从源码构建

要求:
- Rust 1.75+ 和 Cargo

```bash
# 克隆仓库
git clone https://github.com/darkhighness/void.git
cd void

# 构建优化版本
cargo build --release

# 可执行文件位于 target/release/void
```

## 使用方法

### 配置文件

Void 使用 TOML 配置文件来定义其行为。默认配置文件为 `config.toml`。

配置示例:

```toml
[global]
time_tracing=true

[[inbounds]]
tag = "data_np"
type = "named_pipe"
path = "/tmp/data_np"
protocol = "data_csv"

[[inbounds]]
tag = "data"
type = "unix_socket"
path = "/tmp/data.sock"
protocol = "data_graphite"

# 更多配置...
```

### 配置说明

#### 入站配置 (Inbounds)

定义数据输入源:

- `named_pipe`: 从命名管道读取数据
- `unix_socket`: 从 Unix 套接字读取数据

#### 出站配置 (Outbounds)

定义数据输出目标:

- `stdio`: 输出到标准输出
- `parquet`: 输出到 Parquet 文件
- `prometheus`: 通过 Remote Write 写入 Prometheus

#### 管道配置 (Pipes)

定义数据处理逻辑:

- `timeseries`: 处理时序数据
- `timeseries_annotate`: 为时序数据添加注解 (支持动态添加或删除 Labels)

#### 协议配置 (Protocols)

定义数据协议格式:

- `csv`: CSV 格式数据，可定义字段类型
- `graphite`: Graphite 格式数据

### 环境变量

- `RUST_LOG`: 设置日志级别 (默认: info)
- 配置中可以使用 `env:VAR_NAME` 语法引用环境变量
- 部分配置支持占位符, 如 `{{HOME}}`

### 运行

```bash
# 使用默认配置文件
./void

# 指定日志级别
RUST_LOG=debug ./void
```

## 示例

### 收集GPU指标并存储为 Parquet 文件

1. 创建一个配置文件，如上面示例所示
2. 运行 Void: `./void`
3. 将数据发送到指定的命名管道或Unix套接字
4. 数据将被处理并按配置存储

## 构建细节

项目使用GitHub Actions自动构建并发布以下平台的二进制文件:

- Linux (x86_64)
- Linux (ARM64)

二进制文件经过UPX压缩以减小体积。

## 日志

Void使用结构化日志输出信息:

- 日志输出到标准输出及日志文件
- 使用`RUST_LOG`环境变量控制日志级别

## 许可证

MIT

## 贡献

欢迎提交PR和Issue到项目仓库: https://github.com/darkhighness/void
