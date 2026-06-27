from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "ha_state"


def spawn(module: str, env_updates: dict[str, str]) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env.update(env_updates)
    return subprocess.Popen([sys.executable, "-m", module], cwd=str(ROOT), env=env)


def main() -> int:
    lb1_url = "http://127.0.0.1:8100"
    lb2_url = "http://127.0.0.1:8101"
    gateway_url = "http://127.0.0.1:8099"

    processes = [
        spawn(
            "balancer.app",
            {
                "BALANCER_INSTANCE_ID": "lb1",
                "BALANCER_PORT": "8100",
                "BALANCER_PRIORITY": "1",
                "BALANCER_PEER_URLS": lb2_url,
                "BALANCER_SHARED_STATE_DIR": str(STATE_DIR),
            },
        ),
        spawn(
            "balancer.app",
            {
                "BALANCER_INSTANCE_ID": "lb2",
                "BALANCER_PORT": "8101",
                "BALANCER_PRIORITY": "2",
                "BALANCER_PEER_URLS": lb1_url,
                "BALANCER_SHARED_STATE_DIR": str(STATE_DIR),
            },
        ),
        spawn(
            "balancer.ha_gateway",
            {
                "HA_GATEWAY_PORT": "8099",
                "HA_BALANCER_URLS": f"{lb1_url},{lb2_url}",
                "BALANCER_SHARED_STATE_DIR": str(STATE_DIR),
            },
        ),
    ]

    print("Started HA demo:")
    print(f"  LB1: {lb1_url}")
    print(f"  LB2: {lb2_url}")
    print(f"  Gateway: {gateway_url}")
    print("Press Ctrl+C to stop all processes.")

    try:
        while True:
            alive = [process.poll() is None for process in processes]
            if not any(alive):
                return 0
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                process.kill()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())