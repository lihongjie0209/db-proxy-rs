#!/usr/bin/env python3
"""
db-proxy-rs PostgreSQL 集成测试框架
=====================================

测试流程
--------
1. 通过 Docker 启动 PostgreSQL 容器
2. 使用 cargo build 编译代理二进制
3. 启动代理进程（proxy → PostgreSQL）
4. 通过代理执行 PostgreSQL 功能测试
5. 清理所有资源
"""

from __future__ import annotations

import argparse
import contextlib
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, List, Optional

_SCRIPT_DIR = pathlib.Path(__file__).parent

if os.environ.get("NO_COLOR"):
    _GREEN = _YELLOW = _RED = _CYAN = _DIM = _RESET = ""

try:
    import psycopg
except ImportError:
    sys.exit(
        "[ERROR] psycopg 未安装。请执行：\n"
        "  pip install -r tests/requirements-test.txt"
    )

if sys.platform == "win32":
    os.system("")

_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_CYAN = "\033[36m"
_DIM = "\033[2m"
_RESET = "\033[0m"


def _ok(msg: str) -> None: print(f"{_GREEN}✓ {msg}{_RESET}")
def _warn(msg: str) -> None: print(f"{_YELLOW}⚠ {msg}{_RESET}")
def _err(msg: str) -> None: print(f"{_RED}✗ {msg}{_RESET}", file=sys.stderr)
def _info(msg: str) -> None: print(f"{_DIM}  {msg}{_RESET}")
def _sep() -> None: print(f"{_CYAN}{'─' * 54}{_RESET}")


DEFAULT_PG_PORT = 55432
DEFAULT_PROXY_PORT = 55433
DEFAULT_PASSWORD = "proxy_test_pw"
DEFAULT_DATABASE = "proxy_test"
DEFAULT_USER = "postgres"
_CONTAINER_NAME = "db-proxy-rs-pg-integration"


def _wait_port(host: str, port: int, timeout: float = 90.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(1.0)
    return False


def _run(cmd: list[str], *, check: bool = True, capture: bool = False, **kw):
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        **kw,
    )


class DockerPostgres:
    def __init__(self, port: int, password: str, database: str, user: str) -> None:
        self.port = port
        self.password = password
        self.database = database
        self.user = user

    def start(self) -> "DockerPostgres":
        subprocess.run(["docker", "rm", "-f", _CONTAINER_NAME], check=False, capture_output=True)

        _info(f"启动 PostgreSQL 容器（端口 {self.port}）…")
        _run([
            "docker", "run", "-d",
            "--name", _CONTAINER_NAME,
            "-p", f"{self.port}:5432",
            "-e", f"POSTGRES_PASSWORD={self.password}",
            "-e", f"POSTGRES_DB={self.database}",
            "-e", f"POSTGRES_USER={self.user}",
            "postgres:16",
        ])

        _info("等待 PostgreSQL 开放 TCP 端口…")
        if not _wait_port("127.0.0.1", self.port, timeout=90):
            raise RuntimeError(f"PostgreSQL 端口 {self.port} 在 90 s 内未开放")

        _info("等待 PostgreSQL 接受查询（初始化中）…")
        self._wait_ready()
        _ok(f"PostgreSQL 就绪，端口 {self.port}")
        return self

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + 60
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(
                    host="127.0.0.1",
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname=self.database,
                    connect_timeout=3,
                    autocommit=True,
                ):
                    return
            except Exception as exc:
                last_exc = exc
                time.sleep(2)
        raise RuntimeError(f"PostgreSQL 初始化超时：{last_exc}")

    def stop(self) -> None:
        _info("删除 PostgreSQL 容器…")
        subprocess.run(["docker", "rm", "-f", _CONTAINER_NAME], check=False, capture_output=True)

    def __enter__(self) -> "DockerPostgres":
        return self.start()

    def __exit__(self, *_) -> None:
        self.stop()


class Proxy:
    def __init__(self, listen_port: int, upstream_port: int, *, no_build: bool = False) -> None:
        self.listen_port = listen_port
        self.upstream_port = upstream_port
        self.no_build = no_build
        self._proc: Optional[subprocess.Popen] = None
        self.jsonl_path = "proxy-test-pg.jsonl"

    def start(self) -> "Proxy":
        if not self.no_build:
            _info("编译代理（cargo build）…")
            _run(["cargo", "build", "--bin", "db-proxy-rs"])
            _ok("代理编译成功")

        binary = (
            r"target\debug\db-proxy-rs.exe"
            if sys.platform == "win32"
            else "target/debug/db-proxy-rs"
        )

        _info(
            f"启动 PostgreSQL 代理 127.0.0.1:{self.listen_port}"
            f" → 127.0.0.1:{self.upstream_port}…"
        )

        env = {
            **os.environ,
            "PROXY_PROTOCOL": "postgres",
            "PROXY_LISTEN": f"127.0.0.1:{self.listen_port}",
            "PROXY_UPSTREAM": f"127.0.0.1:{self.upstream_port}",
            "RUST_LOG": "info",
        }

        log_path = "proxy-test-pg.log"
        pathlib.Path(self.jsonl_path).unlink(missing_ok=True)
        self._log_file = open(log_path, "w", encoding="utf-8")
        self._proc = subprocess.Popen(
            [
                binary,
                "--output",
                "console",
                "--output",
                f"jsonl={self.jsonl_path}",
            ],
            env=env,
            stdout=self._log_file,
            stderr=self._log_file,
        )

        if not _wait_port("127.0.0.1", self.listen_port, timeout=10):
            self.stop()
            raise RuntimeError(
                f"代理端口 {self.listen_port} 在 10 s 内未开放，查看 {log_path} 了解详情"
            )

        _ok(
            f"PostgreSQL 代理就绪，端口 {self.listen_port}"
            f"（日志 → {log_path}，JSONL → {self.jsonl_path}）"
        )
        return self

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

    def __enter__(self) -> "Proxy":
        return self.start()

    def __exit__(self, *_) -> None:
        self.stop()


@dataclass
class TestResult:
    name: str
    passed: bool
    message: str = ""


class TestSuite:
    def __init__(self, proxy_host: str, proxy_port: int, password: str, database: str, user: str) -> None:
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.password = password
        self.database = database
        self.user = user
        self.results: List[TestResult] = []

    @contextlib.contextmanager
    def _conn(self):
        conn = psycopg.connect(
            host=self.proxy_host,
            port=self.proxy_port,
            user=self.user,
            password=self.password,
            dbname=self.database,
            autocommit=True,
            connect_timeout=5,
        )
        try:
            yield conn
        finally:
            conn.close()

    def _run(self, name: str, fn: Callable[[], None]) -> None:
        try:
            fn()
            self.results.append(TestResult(name, passed=True))
            _ok(name)
        except Exception as exc:
            self.results.append(TestResult(name, passed=False, message=str(exc)))
            _err(name)
            _info(f"    错误: {exc}")

    def test_ping(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                assert row == (1,), f"期望 (1,)，实际 {row!r}"

    def test_server_version(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW server_version")
                (version,) = cur.fetchone()
                assert version, "server_version 返回空字符串"
                _info(f"    PostgreSQL 版本: {version}")

    def test_ddl_dml(self) -> None:
        table = "_proxy_pg_ddl_test"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
                cur.execute(f'CREATE TABLE "{table}" (id SERIAL PRIMARY KEY, val TEXT NOT NULL)')
                cur.execute(f'INSERT INTO "{table}" (val) VALUES (%s)', ("hello",))
                cur.execute(f'INSERT INTO "{table}" (val) VALUES (%s)', ("world",))
                cur.execute(f'SELECT val FROM "{table}" ORDER BY id')
                rows = [r[0] for r in cur.fetchall()]
                assert rows == ["hello", "world"], f"SELECT 结果异常: {rows}"
                cur.execute(f'UPDATE "{table}" SET val = %s WHERE val = %s', ("HELLO", "hello"))
                cur.execute(f'SELECT val FROM "{table}" WHERE id = 1')
                (updated,) = cur.fetchone()
                assert updated == "HELLO", f"UPDATE 后期望 HELLO，实际 {updated!r}"
                cur.execute(f"DELETE FROM \"{table}\" WHERE val = 'world'")
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                (count,) = cur.fetchone()
                assert count == 1, f"DELETE 后期望 1 行，实际 {count}"
                cur.execute(f'DROP TABLE "{table}"')

    def test_prepared_statements(self) -> None:
        table = "_proxy_pg_stmt_test"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
                cur.execute(f'CREATE TABLE "{table}" (id SERIAL PRIMARY KEY, n INT NOT NULL)')
                cur.executemany(
                    f'INSERT INTO "{table}" (n) VALUES (%s)',
                    [(i,) for i in range(10)],
                )
                cur.execute(f'SELECT SUM(n) FROM "{table}"')
                (total,) = cur.fetchone()
                assert int(total) == 45, f"SUM 期望 45，实际 {total}"
                cur.execute(f'DROP TABLE "{table}"')

    def test_large_result_set(self) -> None:
        table = "_proxy_pg_large_test"
        n_rows = 1000
        payload = "x" * 200
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
                cur.execute(f'CREATE TABLE "{table}" (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)')
                cur.executemany(
                    f'INSERT INTO "{table}" (payload) VALUES (%s)',
                    [(payload,)] * n_rows,
                )
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                (count,) = cur.fetchone()
                assert count == n_rows, f"期望 {n_rows} 行，实际 {count}"
                cur.execute(f'DROP TABLE "{table}"')
        _info(f"    转发了 {n_rows} 行 × {len(payload)} 字节/行")

    def test_concurrent_connections(self, n_threads: int = 8, queries_per_thread: int = 50) -> None:
        errors: list[Exception] = []

        def worker() -> None:
            try:
                with self._conn() as conn:
                    with conn.cursor() as cur:
                        for i in range(queries_per_thread):
                            cur.execute("SELECT %s", (i,))
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
            raise AssertionError(f"{len(errors)}/{n_threads} 个线程失败，首个错误: {errors[0]}")

        _info(f"    {n_threads} 个并发连接 × {queries_per_thread} 次查询/连接")

    def test_error_propagation(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("THIS IS NOT VALID SQL !!!!")
                    raise AssertionError("应当抛出 SQL 错误，但未抛出")
                except psycopg.Error:
                    pass

    def run_all(self) -> bool:
        print()
        _sep()
        _info(f" 通过 PostgreSQL 代理 {self.proxy_host}:{self.proxy_port} 运行集成测试")
        _sep()

        self._run("基础连通性（SELECT 1）", self.test_ping)
        self._run("服务器版本查询", self.test_server_version)
        self._run("DDL / DML 全链路转发", self.test_ddl_dml)
        self._run("预处理语句 / executemany", self.test_prepared_statements)
        self._run("大结果集转发（1 000 行）", self.test_large_result_set)
        self._run("并发连接（8 线程 × 50 查询）", self.test_concurrent_connections)
        self._run("错误透传（语法错误）", self.test_error_propagation)

        print()
        _sep()
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        total = len(self.results)
        colour = _GREEN if failed == 0 else _RED
        print(f"{colour}结果: {passed}/{total} 通过" + (f"，{failed} 失败" if failed else "") + _RESET)

        if failed:
            print()
            for r in self.results:
                if not r.passed:
                    print(f"  {_RED}✗ {r.name}{_RESET}")
                    if r.message:
                        print(f"    {_DIM}{r.message}{_RESET}")

        _sep()
        return failed == 0


def _check_prerequisites(no_build: bool, no_docker: bool) -> None:
    missing = []
    if not no_docker and not shutil.which("docker"):
        missing.append("docker")
    if not no_build and not no_docker and not shutil.which("cargo"):
        missing.append("cargo（Rust 工具链）")
    if missing:
        sys.exit("[ERROR] 以下工具未找到，请先安装：\n" + "\n".join(f"  • {t}" for t in missing))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="db-proxy-rs PostgreSQL 集成测试框架",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--pg-port", type=int, default=DEFAULT_PG_PORT, metavar="PORT")
    parser.add_argument("--proxy-port", type=int, default=DEFAULT_PROXY_PORT, metavar="PORT")
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--no-docker", action="store_true")
    parser.add_argument("--proxy-host", default="127.0.0.1", metavar="HOST")
    args = parser.parse_args()

    _check_prerequisites(args.no_build, args.no_docker)

    success = False
    try:
        if args.no_docker:
            _info(f"等待代理 {args.proxy_host}:{args.proxy_port} 可访问…")
            if not _wait_port(args.proxy_host, args.proxy_port, timeout=60):
                sys.exit(f"[ERROR] 代理 {args.proxy_host}:{args.proxy_port} 在 60 s 内未响应")
            _ok(f"代理已就绪 ({args.proxy_host}:{args.proxy_port})")
            suite = TestSuite(args.proxy_host, args.proxy_port, args.password, args.database, args.user)
            success = suite.run_all()
        else:
            postgres = DockerPostgres(args.pg_port, args.password, args.database, args.user)
            proxy = Proxy(args.proxy_port, args.pg_port, no_build=args.no_build)
            suite = TestSuite("127.0.0.1", args.proxy_port, args.password, args.database, args.user)
            with postgres, proxy:
                success = suite.run_all()
    except KeyboardInterrupt:
        print()
        _warn("已中断，正在清理…")
    except Exception as exc:
        _err(f"基础设施启动失败: {exc}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
