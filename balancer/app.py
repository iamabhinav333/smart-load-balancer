from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from balancer.service import app

import uvicorn


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
