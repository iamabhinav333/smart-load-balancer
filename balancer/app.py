from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from balancer.service import app

import uvicorn


if __name__ == "__main__":
    host = os.getenv("BALANCER_HOST", "127.0.0.1")
    port = int(os.getenv("BALANCER_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
