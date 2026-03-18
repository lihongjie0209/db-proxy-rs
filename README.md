# db-proxy-rs

[![CI](https://github.com/lihongjie0209/db-proxy-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/lihongjie0209/db-proxy-rs/actions/workflows/ci.yml)
[![Release](https://github.com/lihongjie0209/db-proxy-rs/actions/workflows/release.yml/badge.svg)](https://github.com/lihongjie0209/db-proxy-rs/actions/workflows/release.yml)

透明 SQL 代理，支持 MySQL / PostgreSQL，完整记录 SQL 流量。

类似 `tee(1)`：所有字节原封不动地转发，同时将副本送往独立的协议分析器，**不影响转发路径**。

```
Client ──► db-proxy-rs :3307 / :5433 ──► MySQL :3306 / PostgreSQL :5432
                  │
            (tee copy)
                  │
            Analyzer Task
          (logs / JSONL file)
```

---

## 功能特性

- **透明转发**：字节流零修改，客户端无感知
- **多协议解析**：
  - MySQL：握手、认证、`COM_QUERY`、`COM_STMT_PREPARE/EXECUTE`、`COM_INIT_DB`、`COM_QUIT`
  - PostgreSQL：startup/auth、simple query、extended query（`Parse` / `Bind` / `Describe` / `Execute` / `Close` / `Sync`）
- **预处理语句参数还原**：记录执行时的绑定参数值（MySQL `COM_STMT_EXECUTE` / PostgreSQL `Bind`）
- **多输出后端**：结构化日志（控制台）和 JSON Lines 文件，可同时开启
- **高性能**：分析器在独立 task 运行，崩溃或延迟不影响转发

---

## 快速开始

### 下载预编译二进制

从 [Releases](https://github.com/lihongjie0209/db-proxy-rs/releases) 页面下载对应平台的压缩包：

| 平台 | 文件 |
|------|------|
| Linux x86_64 | `db-proxy-rs-*-x86_64-unknown-linux-gnu.tar.gz` |
| Linux ARM64 | `db-proxy-rs-*-aarch64-unknown-linux-gnu.tar.gz` |
| macOS Apple Silicon | `db-proxy-rs-*-aarch64-apple-darwin.tar.gz` |
| macOS Intel | `db-proxy-rs-*-x86_64-apple-darwin.tar.gz` |
| Windows x86_64 | `db-proxy-rs-*-x86_64-pc-windows-msvc.zip` |

### 从源码构建

```bash
cargo build --release
```

---

## 使用方式

```bash
# 最简启动（MySQL 模式，监听 :3307，上游 127.0.0.1:3306）
db-proxy-rs

# 自定义地址
db-proxy-rs --listen 0.0.0.0:3307 --upstream 192.168.1.10:3306

# PostgreSQL 模式
db-proxy-rs --protocol postgres --listen 0.0.0.0:5433 --upstream 127.0.0.1:5432

# 输出到 JSON Lines 文件
db-proxy-rs --output jsonl=/var/log/db-proxy.jsonl

# 同时输出到控制台和文件
db-proxy-rs --output console --output jsonl=/tmp/queries.jsonl
```

也可以通过环境变量配置：

```bash
PROXY_PROTOCOL=mysql PROXY_LISTEN=0.0.0.0:3307 PROXY_UPSTREAM=127.0.0.1:3306 db-proxy-rs
PROXY_PROTOCOL=postgres PROXY_LISTEN=0.0.0.0:5433 PROXY_UPSTREAM=127.0.0.1:5432 db-proxy-rs
```

调整日志级别：

```bash
RUST_LOG=debug db-proxy-rs   # 显示握手、ping、参数绑定等详细信息
RUST_LOG=warn  db-proxy-rs   # 仅显示警告和错误
```

### 完整参数

```
Options:
      --protocol <PROTOCOL>  协议类型（`mysql` / `postgres`） [默认: mysql] [env: PROXY_PROTOCOL]
  -l, --listen <ADDR>      代理监听地址  [默认: 0.0.0.0:3307]  [env: PROXY_LISTEN]
  -u, --upstream <ADDR>    上游数据库    [默认: 127.0.0.1:3306] [env: PROXY_UPSTREAM]
  -o, --output <SINK>      输出后端，可重复指定
                             console        结构化日志（遵循 RUST_LOG）
                             jsonl=<path>   JSON Lines，追加写入
  -h, --help               显示帮助
  -V, --version            显示版本
```

---

## 输出格式

### 控制台（结构化日志）

```
INFO db_proxy_rs::sink: server hello  client="127.0.0.1:54321" server_version="8.0.41" conn_id=42
INFO db_proxy_rs::sink: client handshake  client="127.0.0.1:54321" user="root" database=Some("mydb")
INFO db_proxy_rs::sink: authenticated  client="127.0.0.1:54321"
INFO db_proxy_rs::sink: COM_QUERY  client="127.0.0.1:54321" sql="SELECT * FROM users WHERE id = 1"
INFO db_proxy_rs::sink: COM_STMT_PREPARE  client="127.0.0.1:54321" sql="INSERT INTO t (v) VALUES (?)"
INFO db_proxy_rs::sink: COM_STMT_EXECUTE  client="127.0.0.1:54321" stmt_id=1 sql="INSERT INTO t (v) VALUES (?)" params=["'hello'"]
INFO db_proxy_rs::sink: pg startup  client="127.0.0.1:54432" user="postgres" database="proxy_test"
INFO db_proxy_rs::sink: pg parse  client="127.0.0.1:54432" statement="" sql="INSERT INTO t (v) VALUES ($1)"
INFO db_proxy_rs::sink: pg bind  client="127.0.0.1:54432" portal="" statement="" params=["hello"]
INFO db_proxy_rs::sink: pg execute  client="127.0.0.1:54432" portal="" sql="INSERT INTO t (v) VALUES ($1)"
```

### JSON Lines（`jsonl=<path>`）

每行一个 JSON 对象，带毫秒级时间戳：

```json
{"ts_ms":1710000000000,"type":"query","client":"127.0.0.1:54321","sql":"SELECT 1"}
{"ts_ms":1710000000012,"type":"query_ok","client":"127.0.0.1:54321","affected_rows":0,"last_insert_id":null,"warnings":0}
```

---

## 集成测试

测试框架基于 Python + Docker Compose，覆盖 MySQL 与 PostgreSQL 两套环境。

### MySQL 测试

| 测试 | 内容 |
|------|------|
| 基础连通性 | `SELECT 1` 通过代理返回正确结果 |
| 服务器版本查询 | `VERSION()` 透明转发 |
| DDL / DML 全链路 | CREATE / INSERT / SELECT / UPDATE / DELETE / DROP |
| 预处理语句 | `executemany` 批量插入，验证聚合结果 |
| 大结果集 | 1 000 行 × 200 字节，验证不丢失数据 |
| 并发连接 | 8 线程 × 50 查询，验证多路复用 |
| 错误透传 | 语法错误原样透传给客户端 |
| `USE DATABASE` | `COM_INIT_DB` 命令正常执行 |
| mysqlslap 基准 | 10 并发 × 3 迭代混合读写压测 |

### 一键运行（推荐）

```bash
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit
```

### PostgreSQL 测试

PostgreSQL 测试覆盖：

- 基础连通性
- 服务器版本查询
- DDL / DML 全链路
- 预处理语句 / `executemany`
- 大结果集
- 并发连接
- 错误透传

一键运行：

```bash
docker compose -f docker-compose.test-pg.yml up --build --abort-on-container-exit
```

完成后自动清理：

```bash
docker compose -f docker-compose.test.yml down -v
```

### 本地运行

```bash
pip install -r tests/requirements-test.txt

# 自动启动 MySQL 容器、编译并启动代理、执行测试、清理
python tests/integration_test.py

# 跳过编译（二进制已存在）
python tests/integration_test.py --no-build

# 自定义端口
python tests/integration_test.py --mysql-port 3316 --proxy-port 3317

# PostgreSQL
python tests/integration_test_pg.py
```

---

## CI / CD

| Workflow | 触发条件 | 内容 |
|----------|----------|------|
| [CI](.github/workflows/ci.yml) | push / PR | MySQL + PostgreSQL Docker Compose 集成测试 |
| [Release](.github/workflows/release.yml) | 推送 `v*` tag | MySQL + PostgreSQL 集成测试 → 5 平台编译 → GitHub Release |

发布新版本：

```bash
git tag v0.2.0
git push origin v0.2.0
```

---

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                      db-proxy-rs                        │
│                                                         │
│  TcpListener                                            │
│       │  accept()                                       │
│       ▼                                                 │
│  ┌─────────┐   pipe()   ┌──────────────────────────┐   │
│  │ client  │◄──────────►│ upstream MySQL / Postgres │   │
│  └────┬────┘            └──────────────────────────┘   │
│       │ tee (copy)                                      │
│       ▼                                                 │
│  mpsc::unbounded_channel                                │
│       │                                                 │
│       ▼                                                 │
│  ┌──────────┐  packets  ┌──────────┐  events  ┌──────┐ │
│  │ Analyzer │──────────►│ EventSink│─────────►│ Log  │ │
│  └──────────┘           └──────────┘          └──────┘ │
└─────────────────────────────────────────────────────────┘
```

**关键设计原则**：分析器与转发器完全解耦。分析器 panic 或处理缓慢，不影响客户端连接。

---

## License

MIT
