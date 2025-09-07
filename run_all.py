# 作用：一键启动五个 MCP Server + ADK Web

import os
import sys
import time
import socket
import subprocess
import threading
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent

# ========= 内置服务列表 =========
SERVICES = [
    {
        "name": "Normal_MCP",
        "cmd": [sys.executable, "MCP_SERVER/Normal_MCP.py"],
        "cwd": str(PROJECT_ROOT),
        "port": 8001,
    },
    {
        "name": "SQLSynthesizer_MCP",
        "cmd": [sys.executable, "MCP_SERVER/SQLSynthesizer_MCP.py"],
        "cwd": str(PROJECT_ROOT),
        "port": 8002,
    },
    {
        "name": "SQLCritic_MCP",
        "cmd": [sys.executable, "MCP_SERVER/SQLCritic_MCP.py"],
        "cwd": str(PROJECT_ROOT),
        "port": 8003,
    },
    {
        "name": "Analyst_MCP",
        "cmd": [sys.executable, "MCP_SERVER/Analyst_MCP.py"],
        "cwd": str(PROJECT_ROOT),
        "port": 8004,
    },
    {
        "name": "Reporter_MCP",
        "cmd": [sys.executable, "MCP_SERVER/Reporter_MCP.py"],
        "cwd": str(PROJECT_ROOT),
        "port": 8005,
    },
    {
        "name": "ADK_Web",
        "cmd": [sys.executable, "-m", "google.adk.cli", "web", "--host", "127.0.0.1", "--port", "8000"],
        "cwd": str(PROJECT_ROOT / "Agents"),
        "port": 8000,
    },
]

# ========= 工具函数 =========
def load_env():
    load_dotenv(PROJECT_ROOT / ".env")

def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) == 0

def wait_for_port(port: int, host: str, timeout: float = 15.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if is_port_in_use(port, host):
            return True
        time.sleep(0.25)
    return False

def start_service(svc):
    print(f"[启动] {svc['name']}: {' '.join(map(str, svc['cmd']))}")
    p = subprocess.Popen(
        svc["cmd"],
        cwd=svc["cwd"],
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return p

def stream_output(name, proc):
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            print(f"[{name}] {line}", end="")
    except Exception:
        pass

def main():
    load_env()
    procs = []

    try:
        for svc in SERVICES:
            if "port" in svc and is_port_in_use(svc["port"]):
                print(f"[警告] 端口 {svc['port']} 已被占用，跳过 {svc['name']}")
                continue

            p = start_service(svc)
            t = threading.Thread(target=stream_output, args=(svc["name"], p), daemon=True)
            t.start()
            procs.append((svc, p))

            if "port" in svc:
                ok = wait_for_port(svc["port"], "127.0.0.1")
                if ok:
                    print(f"[就绪] {svc['name']} 已监听端口 {svc['port']}")
                else:
                    print(f"[超时] {svc['name']} 未在 15s 内监听端口 {svc['port']}")

            time.sleep(0.2)

        while True:
            any_alive = False
            for svc, p in procs:
                if p.poll() is None:
                    any_alive = True
            if not any_alive:
                break
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[停止] 正在关闭所有服务…")
    finally:
        for svc, p in procs:
            if p.poll() is None:
                print(f"[终止] {svc['name']} …")
                try:
                    p.terminate()
                except Exception:
                    pass
        for svc, p in procs:
            if p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass
        print("[完成] 所有服务已关闭。")

if __name__ == "__main__":
    main()
