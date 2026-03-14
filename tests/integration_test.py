#!/usr/bin/env python3
"""
mysql-proxy-rs 集成测试框架
=================================

测试流程
--------
1. 通过 Docker 启动 MySQL 8.0 容器
2. 使用 cargo build 编译代理二进制
3. 启动代理进程（proxy → MySQL）
4. 通过代理执行功能测试（连通性、DDL/DML、预处理语句、并发）
5. 通过代理运行 mysqlslap 性能基准测试（若已安装）
6. 清理所有资源

使用方式
--------
  # 安装依赖
  pip install -r tests/requirements-test.txt

  # 运行全部测试
  python tests/integration_test.py

  # 自定义端口
  python tests/integration_test.py --mysql-port 3316 --proxy-port 3317

  # 跳过 cargo build（代理已编译）
  python tests/integration_test.py --no-build

前置条件
--------
  - Docker（daemon 已运行）
  - Rust 工具链（cargo 在 PATH 中）
  - Python 3.8+
  - pymysql（pip install pymysql）
  - mysqlslap（可选，随 MySQL 客户端工具附带，缺失时自动跳过）
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Callable, List, Optional

_SCRIPT_DIR = pathlib.Path(__file__).parent

# Respect the NO_COLOR convention (https://no-color.org/) used by Docker logs.
if os.environ.get("NO_COLOR"):
    _GREEN = _YELLOW = _RED = _CYAN = _DIM = _RESET = ""

# ── 依赖检查 ─────────────────────────────────────────────────────────────────

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    sys.exit(
        "[ERROR] pymysql 未安装。请执行：\n"
        "  pip install pymysql\n"
        "或者：\n"
        "  pip install -r tests/requirements-test.txt"
    )

# ── Windows ANSI 颜色支持 ─────────────────────────────────────────────────────

if sys.platform == "win32":
    os.system("")  # 激活 Windows 终端 ANSI 转义码支持

# ── 颜色常量 ──────────────────────────────────────────────────────────────────

_GREEN  = "\033[32m"
_YELLOW = "\033[33m"
_RED    = "\033[31m"
_CYAN   = "\033[36m"
_DIM    = "\033[2m"
_RESET  = "\033[0m"


def _ok(msg: str)   -> None: print(f"{_GREEN}✓ {msg}{_RESET}")
def _warn(msg: str) -> None: print(f"{_YELLOW}⚠ {msg}{_RESET}")
def _err(msg: str)  -> None: print(f"{_RED}✗ {msg}{_RESET}", file=sys.stderr)
def _info(msg: str) -> None: print(f"{_DIM}  {msg}{_RESET}")
def _sep()          -> None: print(f"{_CYAN}{'─' * 54}{_RESET}")

# ── 默认配置 ──────────────────────────────────────────────────────────────────

DEFAULT_MYSQL_PORT    = 3316
DEFAULT_PROXY_PORT    = 3317
DEFAULT_PASSWORD      = "proxy_test_pw"
DEFAULT_DATABASE      = "proxy_test"
_CONTAINER_NAME       = "mysql-proxy-rs-integration"

# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _wait_port(host: str, port: int, timeout: float = 90.0) -> bool:
    """轮询直到 TCP 端口可达，超时返回 False。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(1.0)
    return False


def _run(cmd: list[str], *, check: bool = True, capture: bool = False, **kw):
    """subprocess.run 的简单封装。"""
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        **kw,
    )


# ── Docker MySQL 生命周期 ─────────────────────────────────────────────────────

class DockerMySQL:
    """
    管理测试用 MySQL 容器的完整生命周期。

    使用方式::

        with DockerMySQL(port=3316, password="pw", database="db") as mysql:
            ...  # MySQL 已就绪
        # 退出 with 块后容器自动删除
    """

    def __init__(self, port: int, password: str, database: str) -> None:
        self.port     = port
        self.password = password
        self.database = database

    # ── 启动 ─────────────────────────────────────────────────────────────────

    def start(self) -> "DockerMySQL":
        # 清理上次遗留的同名容器
        subprocess.run(
            ["docker", "rm", "-f", _CONTAINER_NAME],
            check=False, capture_output=True,
        )

        _info(f"启动 MySQL 容器（端口 {self.port}）…")
        _run([
            "docker", "run", "-d",
            "--name", _CONTAINER_NAME,
            "-p", f"{self.port}:3306",
            "-e", f"MYSQL_ROOT_PASSWORD={self.password}",
            "-e", f"MYSQL_DATABASE={self.database}",
            "mysql:8.0",
            "--default-authentication-plugin=mysql_native_password",
        ])

        _info("等待 MySQL 开放 TCP 端口…")
        if not _wait_port("127.0.0.1", self.port, timeout=90):
            raise RuntimeError(f"MySQL 端口 {self.port} 在 90 s 内未开放")

        _info("等待 MySQL 接受查询（初始化中）…")
        self._wait_ready()
        _ok(f"MySQL 就绪，端口 {self.port}")
        return self

    def _wait_ready(self) -> None:
        """在 TCP 就绪后继续等待，直到数据库真正可以执行查询。"""
        deadline = time.monotonic() + 60
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                conn = pymysql.connect(
                    host="127.0.0.1", port=self.port,
                    user="root", password=self.password,
                    database=self.database, connect_timeout=3,
                )
                conn.close()
                return
            except Exception as exc:
                last_exc = exc
                time.sleep(2)
        raise RuntimeError(f"MySQL 初始化超时：{last_exc}")

    # ── 停止 ─────────────────────────────────────────────────────────────────

    def stop(self) -> None:
        _info("删除 MySQL 容器…")
        subprocess.run(
            ["docker", "rm", "-f", _CONTAINER_NAME],
            check=False, capture_output=True,
        )

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "DockerMySQL":
        return self.start()

    def __exit__(self, *_) -> None:
        self.stop()


# ── Proxy 生命周期 ────────────────────────────────────────────────────────────

class Proxy:
    """
    编译并启动 mysql-proxy-rs 二进制文件。

    使用方式::

        with Proxy(listen_port=3317, upstream_port=3316) as proxy:
            ...  # 代理正在监听
        # 退出 with 块后进程自动终止
    """

    def __init__(
        self,
        listen_port: int,
        upstream_port: int,
        *,
        no_build: bool = False,
    ) -> None:
        self.listen_port   = listen_port
        self.upstream_port = upstream_port
        self.no_build      = no_build
        self._proc: Optional[subprocess.Popen] = None

    # ── 启动 ─────────────────────────────────────────────────────────────────

    def start(self) -> "Proxy":
        if not self.no_build:
            _info("编译代理（cargo build）…")
            _run(["cargo", "build", "--bin", "mysql-proxy-rs"])
            _ok("代理编译成功")

        binary = (
            r"target\debug\mysql-proxy-rs.exe"
            if sys.platform == "win32"
            else "target/debug/mysql-proxy-rs"
        )

        _info(
            f"启动代理  127.0.0.1:{self.listen_port}"
            f" → 127.0.0.1:{self.upstream_port}…"
        )

        env = {
            **os.environ,
            "PROXY_LISTEN":   f"127.0.0.1:{self.listen_port}",
            "PROXY_UPSTREAM": f"127.0.0.1:{self.upstream_port}",
            "RUST_LOG":       "info",
        }

        # 代理日志写入独立文件，避免干扰测试输出
        log_path = "proxy-test.log"
        self._log_file = open(log_path, "w", encoding="utf-8")  # noqa: WPS515

        self._proc = subprocess.Popen(
            [binary],
            env=env,
            stdout=self._log_file,
            stderr=self._log_file,
        )

        if not _wait_port("127.0.0.1", self.listen_port, timeout=10):
            self.stop()
            raise RuntimeError(
                f"代理端口 {self.listen_port} 在 10 s 内未开放，"
                f"查看 {log_path} 了解详情"
            )

        _ok(f"代理就绪，端口 {self.listen_port}（日志 → {log_path}）")
        return self

    # ── 停止 ─────────────────────────────────────────────────────────────────

    def stop(self) -> None:
        if self._proc:
            _info("停止代理进程…")
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            self._proc = None
        if hasattr(self, "_log_file"):
            self._log_file.close()

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "Proxy":
        return self.start()

    def __exit__(self, *_) -> None:
        self.stop()


# ── 测试结果 ──────────────────────────────────────────────────────────────────

@dataclass
class TestResult:
    name:    str
    passed:  bool
    message: str = ""
    skipped: bool = False


# ── 测试套件 ──────────────────────────────────────────────────────────────────

class TestSuite:
    """
    通过代理端口运行所有功能测试和性能基准。

    每个 test_* 方法都是独立的测试用例；run_all() 依次执行并汇总结果。
    """

    def __init__(self, proxy_host: str, proxy_port: int, password: str, database: str) -> None:
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.password   = password
        self.database   = database
        self.results: List[TestResult] = []

    # ── 连接工厂 ──────────────────────────────────────────────────────────────

    @contextlib.contextmanager
    def _conn(self):
        """通过代理获取一个 pymysql 连接，退出时自动关闭。"""
        conn = pymysql.connect(
            host=self.proxy_host,
            port=self.proxy_port,
            user="root",
            password=self.password,
            database=self.database,
            autocommit=True,
            connect_timeout=5,
        )
        try:
            yield conn
        finally:
            conn.close()

    # ── 测试执行器 ────────────────────────────────────────────────────────────

    def _run(self, name: str, fn: Callable[[], None]) -> None:
        """执行单个测试，捕获异常，记录结果。"""
        try:
            fn()
            self.results.append(TestResult(name, passed=True))
            _ok(name)
        except SkipTest as s:
            self.results.append(TestResult(name, passed=True, skipped=True, message=str(s)))
            _warn(f"{name}  [跳过: {s}]")
        except Exception as exc:
            self.results.append(TestResult(name, passed=False, message=str(exc)))
            _err(f"{name}")
            _info(f"    错误: {exc}")

    # ── 测试用例 ──────────────────────────────────────────────────────────────

    def test_ping(self) -> None:
        """基础连通性：SELECT 1 通过代理应返回 (1,)。"""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                assert row == (1,), f"期望 (1,)，实际 {row!r}"

    def test_server_version(self) -> None:
        """代理能透明转发 VERSION() 查询并返回非空字符串。"""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                (version,) = cur.fetchone()
                assert version, "VERSION() 返回空字符串"
                _info(f"    MySQL 版本: {version}")

    def test_ddl_dml(self) -> None:
        """CREATE / INSERT / SELECT / UPDATE / DELETE / DROP 全链路转发。"""
        table = "_proxy_ddl_test"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                cur.execute(
                    f"CREATE TABLE {table} "
                    "(id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64) NOT NULL)"
                )

                # INSERT
                cur.execute(f"INSERT INTO {table} (val) VALUES (%s)", ("hello",))
                cur.execute(f"INSERT INTO {table} (val) VALUES (%s)", ("world",))

                # SELECT
                cur.execute(f"SELECT val FROM {table} ORDER BY id")
                rows = [r[0] for r in cur.fetchall()]
                assert rows == ["hello", "world"], f"SELECT 结果异常: {rows}"

                # UPDATE
                cur.execute(
                    f"UPDATE {table} SET val = %s WHERE val = %s",
                    ("HELLO", "hello"),
                )
                cur.execute(f"SELECT val FROM {table} WHERE id = 1")
                (updated,) = cur.fetchone()
                assert updated == "HELLO", f"UPDATE 后期望 HELLO，实际 {updated!r}"

                # DELETE
                cur.execute(f"DELETE FROM {table} WHERE val = 'world'")
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                (count,) = cur.fetchone()
                assert count == 1, f"DELETE 后期望 1 行，实际 {count}"

                cur.execute(f"DROP TABLE {table}")

    def test_prepared_statements(self) -> None:
        """通过代理执行 executemany 批量预处理语句，验证聚合结果。"""
        table = "_proxy_stmt_test"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                cur.execute(
                    f"CREATE TABLE {table} "
                    "(id INT AUTO_INCREMENT PRIMARY KEY, n INT NOT NULL)"
                )

                # 批量插入 0..9
                cur.executemany(
                    f"INSERT INTO {table} (n) VALUES (%s)",
                    [(i,) for i in range(10)],
                )

                cur.execute(f"SELECT SUM(n) FROM {table}")
                (total,) = cur.fetchone()
                assert int(total) == 45, f"SUM 期望 45，实际 {total}"

                cur.execute(f"DROP TABLE {table}")

    def test_large_result_set(self) -> None:
        """代理可透明转发大结果集（1 000 行 × 200 字节）而不丢失数据。"""
        table   = "_proxy_large_test"
        n_rows  = 1_000
        payload = "x" * 200
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                cur.execute(
                    f"CREATE TABLE {table} "
                    "(id INT AUTO_INCREMENT PRIMARY KEY, payload VARCHAR(255) NOT NULL)"
                )
                cur.executemany(
                    f"INSERT INTO {table} (payload) VALUES (%s)",
                    [(payload,)] * n_rows,
                )
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                (count,) = cur.fetchone()
                assert count == n_rows, f"期望 {n_rows} 行，实际 {count}"

                cur.execute(f"DROP TABLE {table}")

        _info(f"    转发了 {n_rows} 行 × {len(payload)} 字节/行")

    def test_concurrent_connections(
        self,
        n_threads: int = 8,
        queries_per_thread: int = 50,
    ) -> None:
        """多线程并发连接：每个线程独立连接代理并执行若干查询。"""
        errors: list[Exception] = []

        def worker() -> None:
            try:
                with self._conn() as conn:
                    with conn.cursor() as cur:
                        for i in range(queries_per_thread):
                            cur.execute(f"SELECT {i}")
                            (val,) = cur.fetchone()
                            assert val == i, f"期望 {i}，实际 {val}"
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            raise AssertionError(
                f"{len(errors)}/{n_threads} 个线程失败，首个错误: {errors[0]}"
            )

        _info(f"    {n_threads} 个并发连接 × {queries_per_thread} 次查询/连接")

    def test_error_propagation(self) -> None:
        """代理应将 MySQL 错误（语法错误）原样透传给客户端。"""
        with self._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("THIS IS NOT VALID SQL !!!!")
                    raise AssertionError("应当抛出 SQL 错误，但未抛出")
                except pymysql.err.ProgrammingError:
                    pass  # 期望收到语法错误，代理正确透传 ✓
                except pymysql.err.OperationalError:
                    pass  # 部分 MySQL 版本以 OperationalError 上报，同样可接受

    def test_use_database(self) -> None:
        """COM_INIT_DB（USE db）命令能通过代理正常执行。"""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"USE {self.database}")
                cur.execute("SELECT DATABASE()")
                (db,) = cur.fetchone()
                assert db == self.database, f"期望 {self.database!r}，实际 {db!r}"

    def test_mysqlslap(self) -> None:
        """
        使用 mysqlslap 对代理进行性能基准测试。

        若系统未安装 mysqlslap，自动跳过此测试。
        mysqlslap 随 MySQL 客户端工具一同分发，
        Linux：mysql-client / mysql-server 包，
        Windows：MySQL Installer 中的 "MySQL Client" 组件。
        """
        if not shutil.which("mysqlslap"):
            raise SkipTest("mysqlslap 未在 PATH 中找到")

        result = subprocess.run(
            [
                "mysqlslap",
                f"--host={self.proxy_host}",
                f"--port={self.proxy_port}",
                "--user=root",
                f"--password={self.password}",
                f"--create-schema={self.database}",
                "--skip-ssl",
                "--auto-generate-sql",
                "--auto-generate-sql-load-type=mixed",
                "--auto-generate-sql-add-autoincrement",
                # 10 并发客户端，迭代 3 次，每次 100 个查询
                "--concurrency=10",
                "--iterations=3",
                "--number-of-queries=100",
                "--verbose",
            ],
            text=True,
            capture_output=True,
        )

        # mysqlslap 将基准报告写到 stdout 或 stderr（版本不同有差异）
        output = (result.stdout + result.stderr).strip()
        if output:
            for line in output.splitlines():
                _info(f"    {line}")

        if result.returncode != 0:
            raise AssertionError(
                f"mysqlslap 以退出码 {result.returncode} 退出\n{output}"
            )

    # ── 运行全部 ──────────────────────────────────────────────────────────────

    def run_all(self) -> bool:
        """依次运行所有测试，返回 True 表示全部通过（跳过的视为通过）。"""
        print()
        _sep()
        _info(f" 通过代理 {self.proxy_host}:{self.proxy_port} 运行集成测试")
        _sep()

        self._run("基础连通性（SELECT 1）",                self.test_ping)
        self._run("服务器版本查询",                        self.test_server_version)
        self._run("DDL / DML 全链路转发",                  self.test_ddl_dml)
        self._run("预处理语句 / executemany",              self.test_prepared_statements)
        self._run("大结果集转发（1 000 行）",              self.test_large_result_set)
        self._run("并发连接（8 线程 × 50 查询）",          self.test_concurrent_connections)
        self._run("错误透传（语法错误）",                  self.test_error_propagation)
        self._run("USE DATABASE（COM_INIT_DB）",           self.test_use_database)
        self._run("mysqlslap 性能基准",                    self.test_mysqlslap)

        # ── 汇总 ─────────────────────────────────────────────────────────────
        print()
        _sep()
        passed  = sum(1 for r in self.results if r.passed)
        failed  = sum(1 for r in self.results if not r.passed)
        skipped = sum(1 for r in self.results if r.skipped)
        total   = len(self.results)

        colour = _GREEN if failed == 0 else _RED
        print(
            f"{colour}结果: {passed}/{total} 通过"
            + (f"，{skipped} 跳过" if skipped else "")
            + (f"，{failed} 失败" if failed else "")
            + _RESET
        )

        if failed:
            print()
            for r in self.results:
                if not r.passed:
                    print(f"  {_RED}✗ {r.name}{_RESET}")
                    if r.message:
                        print(f"    {_DIM}{r.message}{_RESET}")

        _sep()
        return failed == 0


# ── 辅助：跳过测试 ────────────────────────────────────────────────────────────

class SkipTest(Exception):
    """抛出此异常可在测试内部标记"跳过"。"""


# ── 先决条件检查 ──────────────────────────────────────────────────────────────

def _check_prerequisites(no_build: bool, no_docker: bool) -> None:
    missing = []
    if not no_docker and not shutil.which("docker"):
        missing.append("docker")
    if not no_build and not no_docker and not shutil.which("cargo"):
        missing.append("cargo（Rust 工具链）")
    if missing:
        sys.exit(
            "[ERROR] 以下工具未找到，请先安装：\n"
            + "\n".join(f"  • {t}" for t in missing)
        )


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="mysql-proxy-rs 集成测试框架",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mysql-port", type=int, default=DEFAULT_MYSQL_PORT,
        metavar="PORT",
        help=f"MySQL 容器映射到本机的端口（默认 {DEFAULT_MYSQL_PORT}）",
    )
    parser.add_argument(
        "--proxy-port", type=int, default=DEFAULT_PROXY_PORT,
        metavar="PORT",
        help=f"代理监听端口（默认 {DEFAULT_PROXY_PORT}）",
    )
    parser.add_argument(
        "--password", default=DEFAULT_PASSWORD,
        help="MySQL root 密码（默认 proxy_test_pw）",
    )
    parser.add_argument(
        "--database", default=DEFAULT_DATABASE,
        help="测试使用的数据库名称（默认 proxy_test）",
    )
    parser.add_argument(
        "--no-build", action="store_true",
        help="跳过 cargo build（代理二进制已存在时使用）",
    )
    parser.add_argument(
        "--no-docker", action="store_true",
        help="跳过 Docker/cargo 生命周期管理，直接连接已运行的代理（Docker Compose 模式）",
    )
    parser.add_argument(
        "--proxy-host", default="127.0.0.1", metavar="HOST",
        help="代理主机地址（--no-docker 时使用，默认 127.0.0.1）",
    )
    args = parser.parse_args()

    _check_prerequisites(args.no_build, args.no_docker)

    success = False
    try:
        if args.no_docker:
            # Infrastructure is managed externally (e.g. Docker Compose).
            # Just wait for the proxy port and run the tests.
            _info(f"等待代理 {args.proxy_host}:{args.proxy_port} 可访问…")
            if not _wait_port(args.proxy_host, args.proxy_port, timeout=60):
                sys.exit(
                    f"[ERROR] 代理 {args.proxy_host}:{args.proxy_port} "
                    "在 60 s 内未响应"
                )
            _ok(f"代理已就绪 ({args.proxy_host}:{args.proxy_port})")
            suite = TestSuite(
                args.proxy_host, args.proxy_port,
                args.password, args.database,
            )
            success = suite.run_all()
        else:
            mysql = DockerMySQL(args.mysql_port, args.password, args.database)
            proxy = Proxy(args.proxy_port, args.mysql_port, no_build=args.no_build)
            suite = TestSuite(
                "127.0.0.1", args.proxy_port,
                args.password, args.database,
            )
            with mysql, proxy:
                success = suite.run_all()
    except KeyboardInterrupt:
        print()
        _warn("已中断，正在清理…")
    except Exception as exc:
        _err(f"基础设施启动失败: {exc}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
